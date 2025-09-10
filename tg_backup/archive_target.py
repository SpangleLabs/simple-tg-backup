import logging
from typing import TYPE_CHECKING, Optional

from prometheus_client import Counter
from telethon import hints
from telethon.tl.types import ChannelAdminLogEventActionDeleteMessage, ChannelAdminLogEventActionEditMessage

from tg_backup.config import BehaviourConfig
from tg_backup.database.chat_database import ChatDatabase
from tg_backup.models.admin_event import AdminEvent
from tg_backup.models.chat import Chat
from tg_backup.models.message import Message

if TYPE_CHECKING:
    from tg_backup.archiver import Archiver


logger = logging.getLogger(__name__)


messages_processed_count = Counter(
    "tgbackup_messages_processed_count",
    "Total number of messages which have been processed",
)
admin_log_events_processed = Counter(
    "tgbackup_admin_log_events_processed_count",
    "Total number of admin log events which have been processed",
)


class ArchiveTarget:
    def __init__(self, chat_id: int, behaviour: BehaviourConfig, archiver: "Archiver") -> None:
        self.chat_id = chat_id
        self._chat_entity: Optional[hints.Entity] = None
        self.behaviour = behaviour
        self.archiver = archiver
        self.client = archiver.client
        self.chat_db = ChatDatabase(chat_id)
        self.seen_user_ids: set[int] = set()

    async def chat_entity(self) -> hints.Entity:
        if self._chat_entity is None:
            self._chat_entity = await self.client.get_entity(self.chat_id)
        return self._chat_entity

    async def _archive_chat_data(self) -> None:
        chat_entity = await self.chat_entity()
        logger.info("Got chat data: %s", chat_entity)
        chat_obj = Chat.from_chat_entity(chat_entity)
        self.archiver.core_db.save_chat(chat_obj)
        self.chat_db.save_chat(chat_obj)

    async def _archive_admin_log(self) -> None:
        chat_entity = await self.chat_entity()
        async for evt in self.client.iter_admin_log(chat_entity):
            logger.info("Processing admin event ID: %s", evt.id)
            admin_log_events_processed.inc()
            evt_obj = AdminEvent.from_event(evt)
            self.chat_db.save_admin_event(evt_obj)
            if isinstance(evt.action, ChannelAdminLogEventActionDeleteMessage):
                msg = evt.action.message
                msg_obj = Message.from_msg(msg, deleted=True)
                self.chat_db.save_message(msg_obj)
            if isinstance(evt.action, ChannelAdminLogEventActionEditMessage):
                prev_msg = evt.action.prev_message
                new_msg = evt.action.new_message
                prev_msg_obj = Message.from_msg(prev_msg)
                new_msg_obj = Message.from_msg(new_msg)
                self.chat_db.save_message(prev_msg_obj)
                self.chat_db.save_message(new_msg_obj)

    async def _archive_history(self) -> None:
        chat_entity = await self.chat_entity()
        async for msg in self.client.iter_messages(chat_entity):
            logger.info("Processing message ID: %s", msg.id)
            messages_processed_count.inc()
            msg_obj = Message.from_msg(msg)
            self.chat_db.save_message(msg_obj)
            if hasattr(msg, "from_id") and msg.from_id is not None:
                if hasattr(msg.from_id, "user_id"):
                    if msg.from_id.user_id not in self.seen_user_ids:
                        await self.archiver.user_fetcher.queue_user(self.chat_id, self.chat_db, msg.from_id)
                        self.seen_user_ids.add(msg.from_id.user_id)
                else:
                    await self.archiver.user_fetcher.queue_user(self.chat_id, self.chat_db, msg.from_id)
            if hasattr(msg, "sticker") and msg.sticker is not None:
                await self.archiver.sticker_downloader.queue_sticker(msg.sticker)
                continue
            if hasattr(msg, "media") and msg.media is not None:
                if self.behaviour.download_media:
                    await self.archiver.media_dl.queue_media(self.chat_id, msg)

    async def archive_chat(self) -> None:
        # Connect to chat database
        self.chat_db.start()
        # Get chat data
        await self._archive_chat_data(),
        # Gather data from admin log
        if self.behaviour.check_admin_log:
            await self._archive_admin_log()
        # Gather messages from chat
        if self.behaviour.archive_history:
            await self._archive_history()
        # Disconnect from chat DB
        self.chat_db.stop()
