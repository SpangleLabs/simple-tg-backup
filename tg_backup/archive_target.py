import asyncio
import logging
from typing import TYPE_CHECKING, Optional

from prometheus_client import Counter
from telethon import hints, events
from telethon.tl.types import ChannelAdminLogEventActionDeleteMessage, ChannelAdminLogEventActionEditMessage
import telethon.tl.types

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
        self._known_msg_ids: Optional[set[int]] = None

    async def chat_entity(self) -> hints.Entity:
        if self._chat_entity is None:
            self._chat_entity = await self.client.get_entity(self.chat_id)
        return self._chat_entity

    def known_msg_ids(self) -> set[int]:
        if self._known_msg_ids is None:
            self._known_msg_ids = self.chat_db.list_message_ids()
        return self._known_msg_ids

    def add_known_msg_id(self, msg_id: int) -> None:
        known_msg_ids = self.known_msg_ids()
        known_msg_ids.add(msg_id)
        self._known_msg_ids = known_msg_ids

    async def is_small_chat(self) -> bool:
        """Telegram handles small chats differently to large ones. Small means a user chat or a small group chat"""
        return isinstance(await self.chat_entity(), telethon.tl.types.Channel)

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

    async def _process_message(self, msg: telethon.tl.types.Message) -> None:
        logger.debug("Checking message ID: %s in chat ID: %s", msg.id, self.chat_id)
        messages_processed_count.inc()
        msg_obj = Message.from_msg(msg)
        # Check if the message has already been identically archived
        if msg.id in self.known_msg_ids():
            old_msg_objs = self.chat_db.get_messages(msg.id)
            if self.behaviour.cleanup_duplicates and len(old_msg_objs) >= 2:
                cleaned_msg_objs = Message.remove_redundant_copies(old_msg_objs)
                if len(cleaned_msg_objs) != len(old_msg_objs):
                    logger.info(
                        "Cleaning up redundant %s message copies for msg ID: %s",
                        len(old_msg_objs) - len(cleaned_msg_objs),
                        msg.id
                    )
                    self.chat_db.delete_messages(msg.id)
                    for msg_obj in cleaned_msg_objs:
                        self.chat_db.save_message(msg_obj)
            latest_msg_obj = Message.latest_copy_of_message(old_msg_objs)
            if msg_obj.no_useful_difference(latest_msg_obj):
                logger.debug("Already have message ID %s archived sufficiently", msg.id)
                return
            else:
                logger.info("Message ID %s is sufficiently different to archived copies as to deserve re-saving", msg.id)
        else:
            logger.debug("Processing new message ID: %s in chat ID: %s", msg.id, self.chat_id)
        self.chat_db.save_message(msg_obj)
        self.add_known_msg_id(msg.id)
        if hasattr(msg, "from_id") and msg.from_id is not None:
            if hasattr(msg.from_id, "user_id"):
                if msg.from_id.user_id not in self.seen_user_ids:
                    await self.archiver.user_fetcher.queue_user(self.chat_id, self.chat_db, msg.from_id)
                    self.seen_user_ids.add(msg.from_id.user_id)
            else:
                await self.archiver.user_fetcher.queue_user(self.chat_id, self.chat_db, msg.from_id)
        if hasattr(msg, "sticker") and msg.sticker is not None:
            await self.archiver.sticker_downloader.queue_sticker(msg.sticker)
            return
        if hasattr(msg, "media") and msg.media is not None:
                if self.behaviour.download_media:
                    await self.archiver.media_dl.queue_media(self.chat_id, msg)

    async def _archive_history(self) -> None:
        chat_entity = await self.chat_entity()
        async for msg in self.client.iter_messages(chat_entity):
            await self._process_message(msg)

    async def archive_chat(self) -> None:
        # Connect to chat database
        self.chat_db.start()
        # Get chat data
        await self._archive_chat_data(),
        # Start the chat watcher
        watch_task: Optional[asyncio.Task] = None
        if self.behaviour.follow_live:
            logger.info("Following live chat")
            watch_task = asyncio.create_task(self.watch_chat())
        # Gather data from admin log
        if self.behaviour.check_admin_log:
            await self._archive_admin_log()
        # Gather messages from chat
        if self.behaviour.archive_history:
            await self._archive_history()
        # Continue watching if relevant
        if self.behaviour.follow_live:
            logger.info("Chat history archive complete, watching live updates")
            await watch_task
        # Disconnect from chat DB
        self.chat_db.stop()

    async def watch_chat(self) -> None:
        self.client.add_event_handler(self._watch_new_message, events.NewMessage(chats=self.chat_id))
        self.client.add_event_handler(self._watch_edit_message, events.MessageEdited(chats=self.chat_id))
        self.client.add_event_handler(self._watch_delete_message, events.MessageDeleted())
        await self.client.run_until_disconnected()

    async def _watch_new_message(self, event: events.NewMessage.Event) -> None:
        logger.info("New message received")
        await self._process_message(event.message)

    async def _watch_edit_message(self, event: events.MessageEdited.Event) -> None:
        logger.info("Edited message received")
        await self._process_message(event.message)

    async def _watch_delete_message(self, event: events.MessageDeleted.Event) -> None:
        # Telegram does not send information about where a message was deleted if it occurs in private conversations
        # with other users or in small group chats, because message IDs are unique and you can identify the chat with
        # the message ID alone if you saved it previously.
        logger.info("Message deletion event received with %s message IDs", len(event.deleted_ids))
        if event.chat_id == self.chat_id or (event.chat_id is None and self.is_small_chat()):
            for msg_id in event.deleted_ids:
                msg_objs = self.chat_db.get_messages(msg_id)
                if not msg_objs:
                    continue
                logger.debug("Found %s records in chat ID matching deleted message ID %s", len(msg_objs), msg_id)
                latest_msg_obj = Message.latest_copy_of_message(msg_objs)
                deleted_msg = latest_msg_obj.mark_deleted()
                self.chat_db.save_message(deleted_msg)
