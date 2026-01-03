import asyncio
import datetime
import logging
from typing import TYPE_CHECKING, Optional, Iterable

from prometheus_client import Counter
from telethon import hints, events
from telethon.tl.types import ChannelAdminLogEventActionDeleteMessage, ChannelAdminLogEventActionEditMessage
import telethon.tl.types

from tg_backup.config import BehaviourConfig
from tg_backup.database.chat_database import ChatDatabase
from tg_backup.models.abstract_resource import cleanup_existing_duplicates
from tg_backup.models.admin_event import AdminEvent
from tg_backup.models.archive_run_record import ArchiveRunRecord
from tg_backup.models.dialog import Dialog
from tg_backup.models.message import Message
from tg_backup.utils.dialog_type import DialogType

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


class HighWaterMark:
    def __init__(self, archive_target: "ArchiveTarget") -> None:
        self.archive_target = archive_target
        self.initialised = False
        self._archive_target_started_empty = False
        self._high_water_mark: Optional[datetime.datetime] = None

    def initialise(self) -> None:
        # Make sure to set a cutoff for the latest timestamp, otherwise live messages for this chat might have been
        # picked up before this archive target got to run.
        latest_cutoff = self.archive_target.run_record.time_queued - datetime.timedelta(minutes=5)
        newest_msg = self.archive_target.chat_db.get_newest_message(latest_cutoff=latest_cutoff)
        if newest_msg is None:
            self._archive_target_started_empty = True
            return None
        self._high_water_mark = newest_msg.datetime
        self.initialised = True

    def high_water_mark(self) -> datetime.datetime:
        if not self.initialised:
            self.initialise()
        return self._high_water_mark

    def bump_high_water_mark(self, new_hwm: datetime.datetime) -> None:
        """
        This is called every time a message change is detected, in order to keep going through history beyond it for
        another `overlap_days` days.
        """
        if not self.initialised:
            self.initialise()
        if self._high_water_mark is None:
            self._high_water_mark = new_hwm
        hwm = min(self._high_water_mark, new_hwm)
        if new_hwm == hwm and not self._archive_target_started_empty:
            logger.info("Updating high water mark in chat history")
        self._high_water_mark = hwm

    def cutoff_date(self) -> Optional[datetime.datetime]:
        if not self.initialised:
            self.initialise()
        overlap_days = self.archive_target.behaviour.msg_history_overlap_days
        if overlap_days == 0:
            return None
        if self._archive_target_started_empty:
            return None
        # Only now check the high water mark
        hwm = self.high_water_mark()
        if hwm is None:
            return None
        return hwm - datetime.timedelta(days=overlap_days)

    def cutoff_date_met(self, current_msg_date: datetime.datetime) -> bool:
        cutoff_date = self.cutoff_date()
        if cutoff_date is None:
            return False
        # If the current message is before the cutoff, we've gone back far enough
        return current_msg_date < cutoff_date


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

    async def connect_db(self) -> None:
        self.chat_db.start()

    async def disconnect_db(self) -> None:
        # Wait for media downloader to be done before disconnecting database.
        # You must wait for media downloader first, because it might queue more peers when refreshing messages.
        logger.info("Waiting for media downloader to complete for archive target")
        await self.archiver.media_dl.wait_until_queue_empty(self.run_record.archive_run_id)
        # Wait for sticker downloader to be done, which might also queue peers during refresh
        logger.info("Waiting for sticker downloader to complete for archive target")
        await self.archiver.sticker_downloader.wait_until_queue_empty(self.run_record.archive_run_id)
        # Wait for user fetcher to be done before disconnecting database
        logger.info("Waiting for peer fetcher to complete for archive target")
        await self.archiver.peer_fetcher.wait_until_queue_empty(self.run_record.archive_run_id)
        # Disconnect from chat DB
        logger.info("Disconnecting from chat database")
        self.chat_db.stop()

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

    def msg_id_is_known(self, msg_id: int) -> bool:
        return msg_id in self.known_msg_ids()

    def any_msg_id_is_known(self, msg_ids: Iterable[int]):
        return not self.known_msg_ids().isdisjoint(set(msg_ids))

    async def is_small_chat(self) -> bool:
        """Telegram handles small chats differently to large ones. Small means a user chat or a small group chat"""
        # Check dialog type first, and fall back to checking entity
        if self.dialog.chat_type in [DialogType.USER, DialogType.SMALL_GROUP]:
            return True
        if self.dialog.chat_type in [DialogType.LARGE_GROUP, DialogType.CHANNEL]:
            return False
        # Otherwise, fall back to checking the chat entity to find out
        return not isinstance(await self.chat_entity(), telethon.tl.types.Channel)

    async def is_user(self) -> bool:
        # Check dialog type first, then fall back to checking entity
        if self.dialog.chat_type == DialogType.USER:
            return True
        if self.dialog.chat_type == DialogType.UNKNOWN:
            return isinstance(await self.chat_entity(), telethon.tl.types.User)
        return False

    async def _archive_chat_data(self) -> None:
        chat_entity = await self.chat_entity()
        logger.info("Got chat entity data: %s", chat_entity)
        peer = telethon.utils.get_peer(chat_entity)
        queue_key = self.run_record.archive_run_id
        await self.archiver.peer_fetcher.queue_peer(queue_key, self.chat_id, self.chat_db, peer)

    async def _archive_admin_log(self) -> None:
        chat_entity = await self.chat_entity()
        if await self.is_small_chat():
            logger.info("No admin log in small chats")
            return
        async for evt in self.client.iter_admin_log(chat_entity):
            logger.info("Processing admin event ID: %s", evt.id)
            admin_log_events_processed.inc()
            evt_obj = AdminEvent.from_event(evt)
            self.chat_db.save_admin_event(evt_obj)
            self.run_record.run_timer.latest_msg()
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

    async def process_message(self, msg: telethon.tl.types.Message) -> Optional[Message]:
        """
        Processes a new message, returning the model Message object, if it was saved
        """
        logger.info("Checking message ID: %s in chat ID: %s", msg.id, self.chat_id)
        messages_processed_count.inc()
        self.run_record.archive_stats.inc_messages_seen()
        # Convert raw telegram message into storage object
        msg_obj = Message.from_msg(msg)
        # Check if the message has already been identically archived
        if self.msg_id_is_known(msg.id):
            old_msg_objs = self.chat_db.get_messages(msg.id)
            # Cleanup duplicate stored messages if applicable
            if self.behaviour.cleanup_duplicates and len(old_msg_objs) >= 2:
                cleanup_existing_duplicates(old_msg_objs, self.chat_db.delete_messages, self.chat_db.save_message)
            # Get the latest copy of the message and see if it needs re-saving
            latest_msg_obj = Message.latest_copy_of_resource(old_msg_objs)
            if msg_obj.no_useful_difference(latest_msg_obj):
                logger.debug("Already have message ID %s archived sufficiently", msg.id)
                if self.behaviour.recheck_media:
                    await self._msg_to_subsystems(msg)
                return None
            else:
                logger.info("Message ID %s is sufficiently different to archived copies as to deserve re-saving", msg.id)
            # If the previous version was deleted, and this version isn't, delete the old record claiming it was deleted
            if latest_msg_obj.deleted and not msg_obj.deleted:
                logger.info("Message ID %s was previously thought to be deleted. Removing records of it being deleted")
                self.chat_db.delete_deleted_messages(msg_obj.resource_id)
        else:
            logger.debug("Processing new message ID: %s in chat ID: %s", msg.id, self.chat_id)
        # Save the message
        self.chat_db.save_message(msg_obj)
        self.run_record.archive_stats.inc_messages_saved()
        self.add_known_msg_id(msg.id)
        # Send peers, media, and sticker to relevant subsystems
        await self._msg_to_subsystems(msg)
        # Return the saved message object
        return msg_obj

    async def _msg_to_subsystems(self, msg: telethon.tl.types.Message) -> None:
        queue_key = self.run_record.archive_run_id
        if hasattr(msg, "from_id") and msg.from_id is not None:
            await self.archiver.peer_fetcher.queue_peer(queue_key, self.chat_id, self.chat_db, msg.from_id)
        if hasattr(msg, "sticker") and msg.sticker is not None:
            await self.archiver.sticker_downloader.queue_sticker(queue_key, msg, msg.sticker, self)
        else:
            if hasattr(msg, "media") and msg.media is not None:
                if self.behaviour.download_media:
                    await self.archiver.media_dl.queue_media(queue_key, self.chat_id, self.chat_db, msg, self)
                    self.run_record.archive_stats.inc_media_seen()

    async def _archive_message_history(self) -> None:
        chat_entity = await self.chat_entity()
        high_water_mark = HighWaterMark(self)
        initial_cutoff = high_water_mark.cutoff_date()
        if initial_cutoff is not None:
            logger.info("Archiving message history for chat ID %s until cutoff date %s", chat_entity.id, initial_cutoff)
        prev_msg_id: Optional[int] = None
        initial_known_msg_ids = self.known_msg_ids()
        logger.info("Chat ID %s currently contains %s messages", chat_entity.id, len(initial_known_msg_ids))
        async for msg in self.client.iter_messages(chat_entity):
            self.run_record.run_timer.latest_msg()
            # Process and save the message
            new_msg_obj = await self.process_message(msg)
            # If the message was updated, update the high water mark
            if new_msg_obj is not None:
                high_water_mark.bump_high_water_mark(new_msg_obj.datetime)
            # Check for deleted messages
            msg_id = msg.id
            missing_ids = self.missing_message_ids(msg_id, prev_msg_id, initial_known_msg_ids)
            if missing_ids:
                logger.info("It seems like %s messages are missing from the archive, marking as deleted", len(missing_ids))
                high_water_mark.bump_high_water_mark(msg.date)
                for missing_id in missing_ids:
                    self._mark_msg_deleted(missing_id)
            prev_msg_id = msg_id
            # If the message is older than the cutoff date, stop iterating through history
            if high_water_mark.cutoff_date_met(msg.date):
                logger.info("Reached cutoff date without new message updates, stopping search through message history")
                return
        # After iterating through all messages, ensure that earlier messages have not been deleted
        final_missing_ids = self.earlier_missing_message_ids(prev_msg_id, initial_known_msg_ids)
        if final_missing_ids:
            logger.info("There are %s messages missing from the start of the chat history. Marking as deleted", len(final_missing_ids))
            for missing_id in final_missing_ids:
                self._mark_msg_deleted(missing_id)

    @staticmethod
    def missing_message_ids(msg_id: int, prev_msg_id: Optional[int], known_msg_ids: Iterable[int]) -> list[int]:
        if prev_msg_id is None:
            return []
        if (prev_msg_id - msg_id) <= 1:
            return []
        if msg_id not in known_msg_ids or prev_msg_id not in known_msg_ids:
            return []
        return [i for i in known_msg_ids if msg_id < i < prev_msg_id]

    def earlier_missing_message_ids(self, msg_id: Optional[int], known_msg_ids: Iterable[int]) -> list[int]:
        if msg_id is None:
            return []
        return [i for i in known_msg_ids if i < msg_id]

    async def _archive_history(self):
        # Archive admin log
        if self.behaviour.check_admin_log:
            try:
                await self._archive_admin_log()
            except telethon.errors.rpcerrorlist.ChatAdminRequiredError as e:
                logger.warning("Do not have sufficient permissions to archive admin log of chat.")
            except telethon.errors.rpcbaseerrors.BadRequestError as e:
                if e.message == "CHANNEL_MONOFORUM_UNSUPPORTED":
                    logger.info("Admin log not supported for monoforum chats (DMs to channels)")
                else:
                    raise e
        # Gather messages from chat
        if self.behaviour.archive_history:
            await self._archive_message_history()

    async def archive_chat(self) -> None:
        logger.info("Starting archive of chat %s", self.chat_id)
        # Mark the archive run as started
        self.run_record.target_type = self.dialog.chat_type
        self.run_record.run_timer.start()
        # Connect to chat database
        await self.connect_db()
        # Push chat peer to the peer data fetcher
        await self._archive_chat_data()
        # Start the chat watcher
        watch_task: Optional[asyncio.Task] = None
        if self.behaviour.follow_live:
            logger.info("Following live chat")
            watch_task = asyncio.create_task(self.watch_chat())
        # Gather data from admin log and chat messages
        if self.behaviour.needs_archive_run():
            await self._archive_history()
        # Continue watching if relevant
        if self.behaviour.follow_live:
            logger.info("Chat history archive complete, watching live updates")
            await watch_task
        # Disconnect the database, and wait for whatever subsystems need waiting for before that
        await self.disconnect_db()
        self.run_record.run_timer.end()
        self.run_record.mark_complete()
        logger.info("Chat archive complete %s", self.chat_id)

    async def watch_chat(self) -> None:
        # This method is only used when archiving a singular chat target. It is not very good and cannot be shut down
        self.client.add_event_handler(self.on_live_new_message, events.NewMessage(chats=self.chat_id))
        self.client.add_event_handler(self.on_live_edit_message, events.MessageEdited(chats=self.chat_id))
        self.client.add_event_handler(self.on_live_delete_message, events.MessageDeleted())
        await self.client.run_until_disconnected()

    async def on_live_new_message(self, event: events.NewMessage.Event) -> None:
        logger.info("New message received")
        self.run_record.run_timer.latest_msg()
        await self.process_message(event.message)

    async def on_live_edit_message(self, event: events.MessageEdited.Event) -> None:
        logger.info("Edited message received")
        self.run_record.run_timer.latest_msg()
        await self.process_message(event.message)

    async def on_live_delete_message(self, event: events.MessageDeleted.Event) -> None:
        # Telegram does not send information about where a message was deleted if it occurs in private conversations
        # with other users or in small group chats, because message IDs are unique and you can identify the chat with
        # the message ID alone if you saved it previously.
        logger.info("Message deletion event received with %s message IDs", len(event.deleted_ids))
        if event.chat_id == self.chat_id or (event.chat_id is None and await self.is_small_chat()):
            for msg_id in event.deleted_ids:
                self.run_record.run_timer.latest_msg()
                self._mark_msg_deleted(msg_id)

    def _mark_msg_deleted(self, msg_id: int) -> None:
        msg_objs = self.chat_db.get_messages(msg_id)
        if not msg_objs:
            return
        logger.debug("Found %s records in chat ID %s matching deleted message ID %s", len(msg_objs), self.chat_id, msg_id)
        latest_msg_obj = Message.latest_copy_of_resource(msg_objs)
        if latest_msg_obj.deleted:
            return
        deleted_msg = latest_msg_obj.mark_deleted()
        self.chat_db.save_message(deleted_msg)
