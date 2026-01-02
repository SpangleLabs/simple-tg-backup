import asyncio
import dataclasses
import datetime
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


@dataclasses.dataclass
class TargetConnectionState:
    target: ArchiveTarget
    last_activity: datetime.datetime
    is_connected: bool = False
    is_disconnecting: bool = False
    disconnection_task: Optional[asyncio.Task] = None
    is_connecting: bool = False
    connection_task: Optional[asyncio.Task] = None

    def update_activity(self) -> None:
        self.last_activity = datetime.datetime.now(datetime.timezone.utc)

    def older_than(self, time_period: datetime.timedelta) -> bool:
        now = datetime.datetime.now(datetime.timezone.utc)
        return (now - self.last_activity) > time_period

    def is_ready(self) -> bool:
        return self.is_connected and not self.is_disconnecting

    async def _disconnect(self) -> None:
        self.is_disconnecting = True
        logger.info("Disconnecting from Dialog ID %s", self.target.chat_id)
        await self.target.disconnect_db()
        self.is_connected = False
        self.is_disconnecting = False

    def run_disconnect(self) -> asyncio.Task:
        if self.is_disconnecting:
            return self.disconnection_task
        self.is_disconnecting = True
        self.disconnection_task = asyncio.create_task(self._disconnect())
        return self.disconnection_task

    async def _connect(self) -> None:
        self.is_connecting = True
        logger.info("Watcher is connecting to Dialog ID %s", self.target.chat_id)
        await self.target.connect_db()
        self.is_connected = True
        self.is_connecting = False

    async def run_connect(self) -> None:
        if self.is_connected:
            return
        if self.is_disconnecting:
            logger.info("Watcher is waiting for Dialog ID %s to disconnect before reconnecting", self.target.chat_id)
            await self.run_disconnect()
        if self.is_connecting:
            logger.info("Dialog ID %s is already connecting, waiting for connection", self.target.chat_id)
            await self.connection_task
        self.is_connecting = True
        self.connection_task = asyncio.create_task(self._connect())
        await self.connection_task
        self.is_connected = True


class MultiTargetWatcher:
    """
    This class provides callbacks for watching multiple archive targets at once, without overloading Telethon with different callback handlers.
    """
    AUTO_CONNECT_TIME_PERIOD = datetime.timedelta(days=1)

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
        self._target_connections: dict[int, TargetConnectionState] = {}

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
        # Mark all archive targets as starting watch, and connect to their databases if necessary
        for target in self.follow_targets.values():
            target.run_record.run_timer.start()
            # Check whether to auto-connect to chat databases
            target_dialog_msg_age = target.dialog.last_seen_msg_age()
            if target_dialog_msg_age is None:
                # If we don't know how old the last message is, pre-connect just in case
                logger.info("Unsure how old last message in dialog ID %s was, pre-emptively connecting to database", target.chat_id)
                await self._connect_target(target)
            elif target_dialog_msg_age < self.AUTO_CONNECT_TIME_PERIOD:
                # If the target dialog has been active recently, pre-connect to the database
                logger.info("Last message in dialog ID %s is new-enough, pre-emptively connecting to database", target.chat_id)
                await self._connect_target(target)
            elif await target.is_small_chat():
                # Otherwise, if it's a small chat, connect and disconnect to ensure known msg IDs is populated
                logger.info("Populating known message IDs for dialog ID %s", target.chat_id)
                conn_state = await self._connect_target(target)
                await conn_state.run_disconnect()
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
        # Mark all targets as stopped
        for target in self.follow_targets.values():
            target.run_record.run_timer.end()
        # Disconnect from all connected targets
        await asyncio.gather(
            *[conn.run_disconnect() for conn in self._target_connections.values() if conn.is_connected]
        )

    def target_db_connection_state(self, target: ArchiveTarget) -> str:
        conn_state = self._target_connections.get(target.dialog.resource_id)
        if conn_state is None:
            return "disconnected"
        if conn_state.is_disconnecting:
            return "disconnecting"
        if conn_state.is_connecting:
            return "connecting"
        if conn_state.is_connected:
            return "connected"
        return "disconnected"

    async def _connect_target(self, target: ArchiveTarget) -> TargetConnectionState:
        conn_state = self._target_connections.get(target.dialog.resource_id)
        if conn_state is not None:
            if conn_state.is_ready():
                conn_state.update_activity()
                self._cleanup_connections()
                return conn_state
            await conn_state.run_connect()
            conn_state.update_activity()
            return conn_state
        target_dialog_msg_age = target.dialog.last_seen_msg_age()
        now = datetime.datetime.now(datetime.timezone.utc)
        last_activity_datetime = now - (target_dialog_msg_age or datetime.timedelta(0))
        new_conn = TargetConnectionState(target, last_activity_datetime)
        self._target_connections[target.dialog.resource_id] = new_conn
        await new_conn.run_connect()
        # Populate list of known message IDs in the chat
        known_msg_ids = target.known_msg_ids()
        logger.info("Connected to dialog ID %s database, it has %s known messages", target.chat_id, len(known_msg_ids))
        return new_conn

    async def _disconnect_target(self, target: ArchiveTarget) -> None:
        conn_state = self._target_connections.get(target.dialog.resource_id)
        if conn_state is not None:
            await conn_state.run_disconnect()

    def _cleanup_connections(self) -> None:
        for conn_state in self._target_connections.values():
            if conn_state.is_ready():
                if conn_state.older_than(self.AUTO_CONNECT_TIME_PERIOD):
                    conn_state.run_disconnect()

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
        await self._connect_target(target)
        await target.on_live_new_message(event)

    async def _watch_edit_message(self, event: events.MessageEdited.Event) -> None:
        target = await self._target_if_watching(event)
        if target is None:
            logger.warning("Ignoring edited message in unwatched chat ID %s", event.chat_id)
            return
        await self._connect_target(target)
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
            await self._connect_target(target)
            await target.on_live_delete_message(event)
            return
        logger.info("Sending deleted message (without chat ID) to relevant small chats")
        small_group_targets = await self.list_small_group_targets()
        for target in small_group_targets:
            if target.any_msg_id_is_known(event.deleted_ids):
                await self._connect_target(target)
                await target.on_live_delete_message(event)

    async def _handle_new_chat(self, chat: Peer) -> None:
        # Fetch the appropriate dialog object
        dialog_obj = await self.archiver.dialog_fetcher.get_dialog(chat.id)
        if dialog_obj is None:
            # Sometimes messages are delivered for chats which you are not in. For example, you may receive events for
            # messages in the linked discussion group, when you are subscribed to a channel.
            logger.info("Could not find dialog matching new chat ID %s, skipping event", chat.id)
            return
        # Figure out whether to archive the dialog
        if not self.chat_settings.should_archive_dialog(dialog_obj):
            logger.info("New chat ID %s does not match archive settings, noting not to archive it.", chat.id)
            self.not_watching_chat_ids.add(chat.id)
            return
        # Figure out the archive behaviour for the dialog
        behaviour = self.chat_settings.behaviour_for_dialog(dialog_obj, self.archiver.config.default_behaviour)
        # If we're not following the dialog, tell the archiver or not
        if not behaviour.follow_live:
            self.not_watching_chat_ids.add(dialog_obj.resource_id)
            if behaviour.needs_archive_run():
                logger.info("New chat ID %s does not match follow live settings, but does need history archival. Sending to archiver for history archival.", chat.id)
                history_target = ArchiveTarget(dialog_obj, behaviour, self.archiver)
                self.archiver.add_archive_history_target_while_running(history_target)
            else:
                logger.info("New chat ID %s does not match follow live settings, noting not to archive it.", chat.id)
            return
        # We are meant to follow this chat, so add an archive target
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
