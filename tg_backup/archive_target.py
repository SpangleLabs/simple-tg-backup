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
from tg_backup.models.archive_run_record import ArchiveRunRecord
from tg_backup.models.dialog import Dialog
from tg_backup.utils.dialog_type import DialogType
from tg_backup.models.message import Message

if TYPE_CHECKING:
    from tg_backup.archiver import Archiver


logger = logging.getLogger(__name__)


messages_processed_count = Counter(
    "tgbackup_archivetarget_messages_processed_count",
    "Total number of messages which have been processed by archive targets",
)
admin_log_events_processed = Counter(
    "tgbackup_archivetarget_admin_log_events_processed_count",
    "Total number of admin log events which have been processed by archive targets",
)


class ArchiveTarget:
    def __init__(self, dialog: Dialog, behaviour: BehaviourConfig, archiver: "Archiver") -> None:
        self.dialog = dialog
        self.chat_id = dialog.resource_id
        self._chat_entity: Optional[hints.Entity] = None
        self.behaviour = behaviour
        self.archiver = archiver
        self.client = archiver.client
        self.chat_db = ChatDatabase(self.chat_id)
        self._known_msg_ids: Optional[set[int]] = None
        self.run_record = ArchiveRunRecord(dialog.chat_type, self.chat_id, behaviour_config=behaviour, core_db=archiver.core_db)

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
        return not isinstance(await self.chat_entity(), telethon.tl.types.Channel)

    async def is_user(self) -> bool:
        return isinstance(await self.chat_entity(), telethon.tl.types.User)

    async def _archive_chat_data(self) -> None:
        chat_entity = await self.chat_entity()
        logger.info("Got chat entity data: %s", chat_entity)
        peer = telethon.utils.get_peer(chat_entity)
        await self.archiver.peer_fetcher.queue_peer(self.chat_id, self.chat_db, peer)

    async def _archive_admin_log(self) -> None:
        self.run_record.archive_history_timer.started()
        chat_entity = await self.chat_entity()
        if await self.is_small_chat():
            logger.info("No admin log in small chats")
            return
        async for evt in self.client.iter_admin_log(chat_entity):
            logger.info("Processing admin event ID: %s", evt.id)
            admin_log_events_processed.inc()
            evt_obj = AdminEvent.from_event(evt)
            self.chat_db.save_admin_event(evt_obj)
            self.run_record.archive_history_timer.latest_msg()
            self.run_record.archive_stats.inc_admin_events_seen()
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
        self.run_record.archive_history_timer.ended()

    async def _process_message(self, msg: telethon.tl.types.Message) -> None:
        logger.info("Checking message ID: %s in chat ID: %s", msg.id, self.chat_id)
        messages_processed_count.inc()
        self.run_record.archive_stats.inc_messages_seen()
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
        self.run_record.archive_stats.inc_messages_saved()
        self.add_known_msg_id(msg.id)
        if hasattr(msg, "from_id") and msg.from_id is not None:
            await self.archiver.peer_fetcher.queue_peer(self.chat_id, self.chat_db, msg.from_id)
        if hasattr(msg, "sticker") and msg.sticker is not None:
            await self.archiver.sticker_downloader.queue_sticker(msg.sticker)
            return
        if hasattr(msg, "media") and msg.media is not None:
            if self.behaviour.download_media:
                await self.archiver.media_dl.queue_media(self.chat_id, msg)
                self.run_record.archive_stats.inc_media_seen()

    async def _archive_history(self) -> None:
        self.run_record.archive_history_timer.started()
        chat_entity = await self.chat_entity()
        async for msg in self.client.iter_messages(chat_entity):
            self.run_record.archive_history_timer.latest_msg()
            await self._process_message(msg)
        self.run_record.archive_history_timer.ended()

    async def archive_chat(self) -> None:
        logger.info("Starting archive of chat %s", self.chat_id)
        self.run_record.mark_queued()
        self.run_record.target_type = DialogType.USER if await self.is_user() else DialogType.GROUP
        # Connect to chat database
        self.chat_db.start()
        # Get chat data
        await self._archive_chat_data()
        # Start the chat watcher
        watch_task: Optional[asyncio.Task] = None
        if self.behaviour.follow_live:
            logger.info("Following live chat")
            watch_task = asyncio.create_task(self.watch_chat())
        # Gather data from admin log
        if self.behaviour.check_admin_log:
            try:
                await self._archive_admin_log()
            except telethon.errors.rpcerrorlist.ChatAdminRequiredError as e:
                logger.warning("Do not have sufficient permissions to archive admin log of chat.", exc_info=e)
        # Gather messages from chat
        if self.behaviour.archive_history:
            await self._archive_history()
        # Continue watching if relevant
        if self.behaviour.follow_live:
            logger.info("Chat history archive complete, watching live updates")
            await watch_task
        # Wait for user fetcher to be done before disconnecting database
        logger.info("Waiting for peer fetcher to complete for chat")
        await self.archiver.peer_fetcher.wait_until_chat_empty(self.chat_id)
        # Disconnect from chat DB
        logger.info("Disconnecting from chat database")
        self.chat_db.stop()
        self.run_record.mark_complete()
        logger.info("Chat archive complete %s", self.chat_id)

    async def watch_chat(self) -> None:
        self.run_record.follow_live_timer.started()
        self.client.add_event_handler(self.on_live_new_message, events.NewMessage(chats=self.chat_id))
        self.client.add_event_handler(self.on_live_edit_message, events.MessageEdited(chats=self.chat_id))
        self.client.add_event_handler(self.on_live_delete_message, events.MessageDeleted())
        try:
            await self.client.run_until_disconnected()
        finally:
            self.run_record.follow_live_timer.ended()

    async def on_live_new_message(self, event: events.NewMessage.Event) -> None:
        logger.info("New message received")
        self.run_record.follow_live_timer.latest_msg()
        await self._process_message(event.message)

    async def on_live_edit_message(self, event: events.MessageEdited.Event) -> None:
        logger.info("Edited message received")
        self.run_record.follow_live_timer.latest_msg()
        await self._process_message(event.message)

    async def on_live_delete_message(self, event: events.MessageDeleted.Event) -> None:
        # Telegram does not send information about where a message was deleted if it occurs in private conversations
        # with other users or in small group chats, because message IDs are unique and you can identify the chat with
        # the message ID alone if you saved it previously.
        logger.info("Message deletion event received with %s message IDs", len(event.deleted_ids))
        if event.chat_id == self.chat_id or (event.chat_id is None and await self.is_small_chat()):
            for msg_id in event.deleted_ids:
                self.run_record.follow_live_timer.latest_msg()
                msg_objs = self.chat_db.get_messages(msg_id)
                if not msg_objs:
                    continue
                logger.debug("Found %s records in chat ID matching deleted message ID %s", len(msg_objs), msg_id)
                latest_msg_obj = Message.latest_copy_of_message(msg_objs)
                deleted_msg = latest_msg_obj.mark_deleted()
                self.chat_db.save_message(deleted_msg)
