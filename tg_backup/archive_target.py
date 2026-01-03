import asyncio
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
from tg_backup.utils.cut_off_date import CutOffDate
from tg_backup.utils.dialog_type import DialogType
from tg_backup.utils.missing_values import missing_ids_within_range, missing_ids_before_value

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
        # Set up the cutoffs for archival overlap. One starting from the newest known message and working back, and one
        # starting from the oldest known message and working back (in case a previous archival stopped early)
        cutoffs = [
            CutOffDate.from_newest_msg_after_cutoff(self),
            CutOffDate.from_oldest_msg(self),
        ]
        # Setup initial known message IDs, for detecting deleted messages
        initial_known_msg_ids = self.known_msg_ids()
        logger.info("Chat ID %s currently contains %s messages", self.chat_id, len(initial_known_msg_ids))
        # Setup the lowest seen message ID, for marking deleted messages at the start of the chat
        lowest_seen_msg_id: Optional[int] = None
        # For each cutoff, archive the history
        for cutoff in cutoffs:
            # Ensure this cutoff needs checking
            if cutoff is None:
                logger.info("Skipping check of oldest messages in chat ID %s, as chat started with no messages", self.chat_id)
                continue
            # When archiving newest messages, log where the cutoff date is gonna be
            if cutoff.offset_id is 0 and cutoff.cutoff_date() is not None:
                logger.info("Archiving message history for chat ID %s until cutoff date %s", self.chat_id, cutoff.cutoff_date())
            # Reset the lowest seen message ID
            lowest_seen_msg_id = None
            # Check all new messages until cutoff
            num_processed_msgs = 0
            async for msg in self.client.iter_messages(chat_entity, offset_id=cutoff.offset_id):
                self.run_record.run_timer.latest_msg()
                num_processed_msgs += 1
                # Process and save the message
                new_msg_obj = await self.process_message(msg)
                # If the message was updated, update the cutoff with the next known datetime
                if new_msg_obj is not None:
                    cutoff.bump_known_datetime(new_msg_obj.datetime)
                # Check for deleted messages
                msg_id = msg.id
                missing_ids = missing_ids_within_range(initial_known_msg_ids, lowest_seen_msg_id, msg_id)
                if missing_ids:
                    logger.info("It seems like %s messages are missing from the archive, marking as deleted", len(missing_ids))
                    cutoff.bump_known_datetime(msg.date)
                    for missing_id in missing_ids:
                        self._mark_msg_deleted(missing_id)
                # Update the previous message ID
                lowest_seen_msg_id = msg_id
                # If the message is older than the cutoff date, stop iterating through history
                if cutoff.cutoff_date_met(msg.date):
                    logger.info("Reached cutoff date without new message updates, stopping search through message history")
                    break
            # Log how many messages were processed by that iteration
            logger.info("Archiver processed %s messages in chat ID %s, starting from offset ID %s until cutoff date %s", num_processed_msgs, self.chat_id, cutoff.offset_id, cutoff.cutoff_date())
        # After checking through all messages (or messages at top and bottom of chat), mark any earlier known messages as deleted
        final_missing_ids = missing_ids_before_value(initial_known_msg_ids, lowest_seen_msg_id)
        if final_missing_ids:
            logger.info("There are %s messages missing from the start of the chat history. Marking as deleted", len(final_missing_ids))
            for missing_id in final_missing_ids:
                self._mark_msg_deleted(missing_id)

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
