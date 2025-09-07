import logging
from typing import TYPE_CHECKING, Optional

from telethon import hints
from telethon.tl.types import ChannelAdminLogEventActionDeleteMessage, ChannelAdminLogEventActionEditMessage

from tg_backup.config import BehaviourConfig
from tg_backup.database import ChatDatabase
from tg_backup.models.admin_event import AdminEvent
from tg_backup.models import Chat
from tg_backup.models import Message

if TYPE_CHECKING:
    from tg_backup.archiver import Archiver


logger = logging.getLogger(__name__)


class ArchiveTarget:
    def __init__(self, chat_id: int, behaviour: BehaviourConfig, archiver: "Archiver") -> None:
        self.chat_id = chat_id
        self._chat_entity: Optional[hints.Entity] = None
        self.behaviour = behaviour
        self.archiver = archiver
        self.client = archiver.client
        self.chat_db = ChatDatabase(chat_id)

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
            msg_obj = Message.from_msg(msg)
            self.chat_db.save_message(msg_obj)
            if hasattr(msg, "media") and msg.media is not None:
                # TODO: tell the media downloader which chat this is
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
