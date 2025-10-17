import logging

from telethon import TelegramClient, events

from tg_backup.archive_target import ArchiveTarget


logger = logging.getLogger(__name__)


class MultiTargetWatcher:
    """
    This class provides callbacks for watching multiple archive targets at once, without overloading Telethon with different callback handlers.
    """

    def __init__(self, client: TelegramClient, targets: list[ArchiveTarget], small_group_targets: list[ArchiveTarget]) -> None:
        self.client = client
        self.targets: dict[int, ArchiveTarget] = {t.chat_id: t for t in targets if t.behaviour.follow_live}
        self.small_group_targets = small_group_targets # Need passing in, because cannot async in init

    @classmethod
    async def from_targets(cls, client: TelegramClient, targets: list[ArchiveTarget]) -> "MultiTargetWatcher":
        small_group_targets = []
        # Figure out which chats are small group chats
        for target in targets:
            if await target.is_small_chat():
                small_group_targets.append(target)
        return cls(client, targets, small_group_targets)

    async def watch(self) -> None:
        # Mark all archive targets as starting watch, and connect to their databases
        for target in self.targets.values():
            target.run_record.follow_live_timer.started()
            target.chat_db.start()
        # Register event handlers
        chat_ids = list(self.targets.keys())
        self.client.add_event_handler(self._watch_new_message, events.NewMessage(chats=chat_ids))
        self.client.add_event_handler(self._watch_edit_message, events.MessageEdited(chats=chat_ids))
        self.client.add_event_handler(self._watch_delete_message, events.MessageDeleted())
        # Watch the client until disconnect
        try:
            await self.client.run_until_disconnected()
        finally:
            # Mark all targets as stopped and disconnect from databases
            for target in self.targets.values():
                target.run_record.follow_live_timer.ended()
                target.chat_db.stop()

    async def _watch_new_message(self, event: events.NewMessage.Event) -> None:
        target = self.targets.get(event.chat_id)
        if target is None:
            logger.warning("Received new message from unknown chat")
            return
        await target.on_live_new_message(event)

    async def _watch_edit_message(self, event: events.MessageEdited.Event) -> None:
        target = self.targets.get(event.chat_id)
        if target is None:
            logger.warning("Received edited message from unknown chat")
            return
        await target.on_live_edit_message(event)

    async def _watch_delete_message(self, event: events.MessageDeleted.Event) -> None:
        # Telegram does not send information about where a message was deleted if it occurs in private conversations
        # with other users or in small group chats, because message IDs are unique and you can identify the chat with
        # the message ID alone if you saved it previously.
        if event.chat_id is not None:
            target = self.targets.get(event.chat_id)
            if target is None:
                logger.warning("Received deleted message from unknown chat")
                return
            await target.on_live_delete_message(event)
            return
        logger.info("Sending deleted message (without chat ID) to all monitored small chats")
        for target in self.small_group_targets:
            await target.on_live_delete_message(event)
