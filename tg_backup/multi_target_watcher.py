import asyncio
import logging
from typing import TYPE_CHECKING, Optional, Union

import telethon.tl.types
from telethon import TelegramClient, events
from telethon.tl.custom.chatgetter import ChatGetter

from tg_backup.archive_target import ArchiveTarget
from tg_backup.chat_settings_store import ChatSettingsStore
from tg_backup.config import BehaviourConfig
from tg_backup.models.dialog import Dialog

if TYPE_CHECKING:
    from tg_backup.archiver import Archiver

logger = logging.getLogger(__name__)

Peer = Union[telethon.tl.types.User, telethon.tl.types.Chat, telethon.tl.types.Channel]


class MultiTargetWatcher:
    """
    This class provides callbacks for watching multiple archive targets at once, without overloading Telethon with different callback handlers.
    """

    def __init__(
            self,
            client: TelegramClient,
            archiver: "Archiver",
            chat_settings: ChatSettingsStore,
            targets: list[ArchiveTarget],
            not_watching_chat_ids: set[int],
    ) -> None:
        self.client = client
        self.archiver = archiver
        self.chat_settings = chat_settings
        # Construct the list of targets to watch and not watch
        self.follow_targets: dict[int, ArchiveTarget] = {t.chat_id: t for t in targets if t.behaviour.follow_live}
        self.not_watching_chat_ids = not_watching_chat_ids # We need to know which chats are not watched, so we know which are new
        # Internal attributes
        self._small_group_targets: Optional[list[ArchiveTarget]] = None
        self.running = False
        self._shutdown_event = asyncio.Event()

    async def list_small_group_targets(self) -> list[ArchiveTarget]:
        if self._small_group_targets is None:
            small_group_targets = []
            for target in self.follow_targets.values():
                if await target.is_small_chat():
                    small_group_targets.append(target)
            self._small_group_targets = small_group_targets
        return self._small_group_targets

    def count_watched_targets(self) -> int:
        return len(self.follow_targets)

    def watching_nothing(self) -> bool:
        return len(self.follow_targets) == 0

    def chat_is_known(self, chat_id: int) -> bool:
        return chat_id in self.follow_targets or chat_id in self.not_watching_chat_ids

    @classmethod
    def from_dialogs(
            cls,
            client: TelegramClient,
            archiver: "Archiver",
            chat_settings: ChatSettingsStore,
            dialogs: list[Dialog],
    ) -> "MultiTargetWatcher":
        # Figure out which chats are small group chats
        follow_targets = []
        not_watching_chat_ids = set()
        for dialog in dialogs:
            if not chat_settings.should_archive_dialog(dialog):
                not_watching_chat_ids.add(dialog.resource_id)
                continue
            behaviour = chat_settings.behaviour_for_dialog(dialog, archiver.config.default_behaviour)
            if not behaviour.follow_live:
                not_watching_chat_ids.add(dialog.resource_id)
                continue
            target = ArchiveTarget(dialog, behaviour, archiver)
            follow_targets.append(target)
        return cls(client, archiver, chat_settings, follow_targets, not_watching_chat_ids)

    async def watch(self) -> None:
        await self._start_watch()
        # Watch the client until disconnect
        try:
            await self._shutdown_event.wait()
        finally:
            await self._stop_watch()

    async def _start_watch(self) -> None:
        # Mark all archive targets as starting watch, and connect to their databases
        for target in self.follow_targets.values():
            target.run_record.follow_live_timer.start()
            target.chat_db.start()
        # Register event handlers
        self.client.add_event_handler(self._watch_new_message, events.NewMessage())
        self.client.add_event_handler(self._watch_edit_message, events.MessageEdited())
        self.client.add_event_handler(self._watch_delete_message, events.MessageDeleted())
        self.running = True

    async def _stop_watch(self) -> None:
        self._shutdown_event.set()
        self._shutdown_event.clear()
        self.running = False
        # Unregister event handlers
        self.client.remove_event_handler(self._watch_new_message)
        self.client.remove_event_handler(self._watch_edit_message)
        self.client.remove_event_handler(self._watch_delete_message)
        # Mark all targets as stopped and disconnect from databases
        for target in self.follow_targets.values():
            target.run_record.follow_live_timer.end()
            target.chat_db.stop()

    def shutdown(self) -> None:
        self._shutdown_event.set()
        self._shutdown_event.clear()

    async def _target_if_watching(self, evt: ChatGetter) -> Optional[ArchiveTarget]:
        chat_id = evt.chat_id
        if not self.chat_is_known(chat_id):
            logger.info(f"New message from unknown chat ID %s", chat_id)
            await self._handle_new_chat(await evt.get_chat())
        if chat_id in self.not_watching_chat_ids:
            logger.debug("Ignoring message in unwatched chat ID %s", chat_id)
            return None
        target = self.follow_targets.get(chat_id)
        if target is None:
            logger.warning("Could not find whether to follow chat ID %s, after checking", chat_id)
        return target

    async def _watch_new_message(self, event: events.NewMessage.Event) -> None:
        target = await self._target_if_watching(event)
        if target is None:
            logger.debug("Ignoring new message in unwatched chat ID %s", event.chat_id)
            return
        await target.on_live_new_message(event)

    async def _watch_edit_message(self, event: events.MessageEdited.Event) -> None:
        target = await self._target_if_watching(event)
        if target is None:
            logger.warning("Ignoring edited message in unwatched chat ID %s", event.chat_id)
            return
        await target.on_live_edit_message(event)

    async def _watch_delete_message(self, event: events.MessageDeleted.Event) -> None:
        # Telegram does not send information about where a message was deleted if it occurs in private conversations
        # with other users or in small group chats, because message IDs are unique and you can identify the chat with
        # the message ID alone if you saved it previously.
        if event.chat_id is not None:
            target = await self._target_if_watching(event)
            if target is None:
                logger.debug("Ignoring deleted message in unwatched chat ID %s", event.chat_id)
                return
            await target.on_live_delete_message(event)
            return
        logger.info("Sending deleted message (without chat ID) to all monitored small chats")
        small_group_targets = await self.list_small_group_targets()
        for target in small_group_targets:
            await target.on_live_delete_message(event)

    async def _handle_new_chat(self, chat: Peer) -> None:
        dialogs = await self.client.get_dialogs(offset_peer=chat, limit=1)
        if len(dialogs) == 0:
            logger.error("Did not find dialog matching new chat ID %s", chat.id)
            return # TODO: exception, or what??? Test this somehow
        raw_dialog = dialogs[0]
        dialog_obj = Dialog.from_dialog(raw_dialog)
        if not self.chat_settings.should_archive_dialog(dialog_obj):
            logger.info("New chat ID %s does not match archive settings, noting not to archive it.", chat.id)
            self.not_watching_chat_ids.add(chat.id)
            return
        behaviour = self.chat_settings.behaviour_for_dialog(dialog_obj, self.archiver.config.default_behaviour)
        if not behaviour.follow_live:
            self.not_watching_chat_ids.add(dialog_obj.resource_id)
            if behaviour.needs_archive_run():
                logger.info("New chat ID %s does not match follow live settings, but does need history archival. Sending to archiver for history archival.", chat.id)
                history_target = ArchiveTarget(dialog_obj, behaviour, self.archiver)
                self.archiver.add_archive_history_target_while_running(history_target)
            else:
                logger.info("New chat ID %s does not match follow live settings, noting not to archive it.", chat.id)
            return
        follow_target = ArchiveTarget(dialog_obj, behaviour, self.archiver)
        self.follow_targets[chat.id] = follow_target
        if behaviour.needs_archive_run():
            logger.info("New chat ID %s has been followed, and needs history archival. Sending to archiver for history archival.", chat.id)
            history_behaviour = BehaviourConfig.merge(behaviour, BehaviourConfig(follow_live=False))
            history_target = ArchiveTarget(dialog_obj, history_behaviour, self.archiver)
            self.archiver.add_archive_history_target_while_running(history_target)
        else:
            logger.info("New chat ID %s has been followed", chat.id)
        return
