import asyncio
import dataclasses
import datetime
import logging
import os
from typing import Optional, TYPE_CHECKING

import telethon
from prometheus_client import Counter, Histogram
from telethon import TelegramClient
from telethon.errors import StickersetInvalidError, LocationInvalidError, FileReferenceExpiredError
from telethon.tl.functions.messages import GetStickerSetRequest
from telethon.tl.types import DocumentAttributeSticker, InputStickerSetID, DocumentAttributeFilename, Document

from tg_backup.archive_target import ArchiveTarget
from tg_backup.database.core_database import CoreDatabase
from tg_backup.models.abstract_resource import save_if_not_duplicate
from tg_backup.models.sticker import Sticker
from tg_backup.models.sticker_set import StickerSet
from tg_backup.models.subsystem_queue_entry import StickerDownloaderQueueEntry
from tg_backup.subsystems.abstract_subsystem import TimedCache, AbstractTargetQueuedSubsystem, ArchiveRunQueue, \
    QueueInfo, QueueEntry
from tg_backup.subsystems.media_msg_refresh_cache import MessageRefreshCache

if TYPE_CHECKING:
    from tg_backup.archiver import Archiver


logger = logging.getLogger(__name__)

stickers_processed_count = Counter(
    "tgbackup_stickerdownloader_stickers_processed_count",
    "Total number of stickers which have been picked from the queue by the StickerDownloader",
)
sticker_sets_processed_count = Counter(
    "tgbackup_stickerdownloader_sticker_sets_processed_count",
    "Total number of sticker sets which have been processed and queued by the StickerDownloader",
)
sticker_set_data_failure_count = Counter(
    "tgbackup_stickerdownloader_sticker_set_data_failure_count",
    "Total number of sticker sets for which data failed to be fetched",
)
sticker_set_size_histogram = Histogram(
    "tgbackup_stickerdownloader_sticket_set_size",
    "Histogram over the number of stickers in a sticker set, of the sticker sets processed",
    buckets=[0, 10, 20, 30, 40, 50, 60, 70, 80, 90, 100, 110, 120, 130],
)


@dataclasses.dataclass
class StickerQueueInfo:
    chat_id: int
    archive_target: ArchiveTarget


@dataclasses.dataclass
class StickerQueueEntry:
    sticker_doc: Optional[Document]
    message: Optional[telethon.types.Message] # Needed for message refresher
    direct_from_msg: bool # Whether the sticker is from the message, or from a sticker set
    _storable_entry: Optional[StickerDownloaderQueueEntry] = None

    @property
    def message_id(self) -> int:
        if self.message is None:
            return self.storable_entry.message_id
        return self.message.id

    @property
    def sticker_id(self) -> int:
        if self.sticker_doc is None:
            return self.storable_entry.sticker_id
        return StickerDownloader._find_sticker_id(self.sticker_doc)

    @property
    def storable_entry(self) -> StickerDownloaderQueueEntry:
        if self._storable_entry is None:
            self._storable_entry = StickerDownloaderQueueEntry(
                None,
                self.message.id,
                self.sticker_doc.id,
                self.direct_from_msg,
            )
        return self._storable_entry


class StickerDownloader(AbstractTargetQueuedSubsystem[StickerQueueInfo, StickerQueueEntry]):
    CACHE_EXPIRY = datetime.timedelta(days=1)
    STICKER_SET_CACHE_EXPIRY = datetime.timedelta(minutes=5)

    def __init__(
            self,
            archiver: "Archiver",
            client: TelegramClient,
            core_db: CoreDatabase,
            message_refresher: MessageRefreshCache,
    ) -> None:
        super().__init__(archiver, client)
        self.core_db = core_db
        self.message_refresher = message_refresher
        self._seen_sticker_set_ids = TimedCache[int, None](self.CACHE_EXPIRY) # Which sticker sets have been seen and listed
        self._seen_sticker_ids = TimedCache[int, None](self.CACHE_EXPIRY) # Which stickers have already been saved in the database
        self._sticker_set_fetch_cache = TimedCache[int, telethon.tl.types.messages.StickerSet](self.STICKER_SET_CACHE_EXPIRY)

    def is_sticker_set_cached(self, sticker_set_id: int) -> bool:
        return self._seen_sticker_set_ids.is_resource_id_cached(sticker_set_id)

    def cache_sticker_set_id(self, sticker_set_id: int) -> None:
        self._seen_sticker_set_ids.cache_resource_id(sticker_set_id)

    def is_sticker_cached(self, sticker_id: int) -> bool:
        return self._seen_sticker_ids.is_resource_id_cached(sticker_id)

    def cache_sticker_id(self, sticker_id: int) -> None:
        self._seen_sticker_ids.cache_resource_id(sticker_id)

    @staticmethod
    def _find_sticker_id(sticker_doc: Document) -> Optional[int]:
        return sticker_doc.id if hasattr(sticker_doc, "id") else None

    @staticmethod
    def _find_sticker_set_id(sticker_doc: Document) -> Optional[int]:
        input_sticker_set = StickerDownloader._find_input_sticker_set(sticker_doc)
        return StickerDownloader._find_input_sticker_set_id(input_sticker_set)

    @staticmethod
    def _find_sticker_in_set_by_id(
            sticker_set: telethon.tl.types.messages.StickerSet,
            sticker_id: int,
    ) -> Optional[Document]:
        for sticker_doc in sticker_set.documents:
            if StickerDownloader._find_sticker_id(sticker_doc) == sticker_id:
                return sticker_doc
        return None

    @staticmethod
    def _find_file_ext(sticker_doc: Document) -> Optional[str]:
        if hasattr(sticker_doc, "attributes"):
            for attr in sticker_doc.attributes:
                if isinstance(attr, DocumentAttributeFilename):
                    if hasattr(attr, "file_name"):
                        return attr.file_name.split(".")[-1]
        return None

    @staticmethod
    def _find_input_sticker_set(sticker_doc: Document) -> Optional[InputStickerSetID]:
        if hasattr(sticker_doc, "attributes"):
            for attr in sticker_doc.attributes:
                if isinstance(attr, DocumentAttributeSticker):
                    return attr.stickerset
        return None

    @staticmethod
    def _find_input_sticker_set_id(input_sticker_set: Optional[InputStickerSetID]) -> Optional[int]:
        return input_sticker_set.id if hasattr(input_sticker_set, "id") else None

    async def _fetch_sticker_set(
            self,
            input_sticker_set: InputStickerSetID,
    ) -> Optional[telethon.tl.types.messages.StickerSet]:
        sticker_set_id = self._find_input_sticker_set_id(input_sticker_set)
        try:
            sticker_set = await self.client(GetStickerSetRequest(
                stickerset=input_sticker_set,
                hash=0,
            ))
            self._sticker_set_fetch_cache.cache_resource_value(sticker_set_id, sticker_set)
            return sticker_set
        except StickersetInvalidError:
            sticker_set_data_failure_count.inc()
            logger.warning("Could not fetch sticker set: %s. Pack may have been deleted", sticker_set_id)
            return None

    async def _process_sticker_set(
            self,
            sticker_doc: Document,
            queue: ArchiveRunQueue[StickerQueueInfo, StickerQueueEntry],
            queue_entry: StickerQueueEntry,
    ):
        # Find sticker set
        input_sticker_set = self._find_input_sticker_set(sticker_doc)
        if input_sticker_set is None:
            return
        sticker_set_id = self._find_input_sticker_set_id(input_sticker_set)
        if self.is_sticker_set_cached(sticker_set_id):
            return
        sticker_sets_processed_count.inc()
        # Fetch sticker set data
        sticker_set = await self._fetch_sticker_set(input_sticker_set)
        if sticker_set is None:
            return
        sticker_set_obj = StickerSet.from_sticker_set(sticker_set)
        # Save sticker set to database
        save_if_not_duplicate(
            sticker_set_obj,
            self.archiver.config.default_behaviour.cleanup_duplicates,
            self.core_db.save_sticker_set,
            self.core_db.get_sticker_sets,
            self.core_db.delete_sticker_sets,
        )
        # Observe the size
        sticker_set_size_histogram.observe(len(sticker_set.documents))
        logger.info("Sticker set ID %s contains %s stickers", sticker_set_id, len(sticker_set.documents))
        # Put the rest of the pack in the queue
        for sticker_doc in sticker_set.documents:
            await self.queue_sticker(
                queue.queue_key,
                queue_entry.message,
                sticker_doc,
                queue.info.archive_target,
                False,
            )
        self.cache_sticker_set_id(sticker_set_id)

    async def _do_process(self) -> None:
        queue, queue_entry = self._get_next_in_queue()
        sticker_doc = queue_entry.sticker_doc
        sticker_id = self._find_sticker_id(sticker_doc) or queue_entry.storable_entry.sticker_id
        stickers_processed_count.inc()
        # Check if sticker has been saved
        if self.is_sticker_cached(sticker_id):
            queue.queue.task_done()
            queue.info.archive_target.chat_db.delete_subsystem_queue_entry(queue_entry.storable_entry.queue_entry_id)
            return
        # Check if the sticker doc needs refreshing
        if queue_entry.sticker_doc is None and queue_entry.storable_entry.sticker_id is not None:
            queue_entry.sticker_doc = await self._refresh_sticker_doc(queue, queue_entry)
            if queue_entry.sticker_doc is None:
                logger.info("Stored queue entry for sticker ID %s on message ID %s could not be refreshed into sticker", queue_entry.sticker_id, queue_entry.message_id)
                queue.queue.task_done()
                queue.info.archive_target.chat_db.delete_subsystem_queue_entry(queue_entry.storable_entry.queue_entry_id)
        # Get sticker file path
        sticker_file_path = self._sticker_file_path(sticker_doc)
        # Download sticker
        if not os.path.exists(sticker_file_path):
            sticker_doc = await self._download_sticker(
                queue,
                queue_entry,
                sticker_doc,
            )
        # Create storable sticker object
        sticker_obj = Sticker.from_sticker(sticker_doc)
        # Save to database
        logger.info("Saving sticker ID %s to database", sticker_id)
        save_if_not_duplicate(
            sticker_obj,
            self.archiver.config.default_behaviour.cleanup_duplicates,
            self.core_db.save_sticker,
            self.core_db.get_stickers,
            self.core_db.delete_stickers,
        )
        # Update cache
        self.cache_sticker_id(sticker_id)
        # Process the sticker set
        await self._process_sticker_set(sticker_doc, queue, queue_entry)
        # Mark the task as done
        queue.queue.task_done()
        queue.info.archive_target.chat_db.delete_subsystem_queue_entry(queue_entry.storable_entry.queue_entry_id)

    def _sticker_file_path(self, sticker_doc: Document) -> str:
        sticker_id = self._find_sticker_id(sticker_doc)
        sticker_set_id = self._find_sticker_set_id(sticker_doc)
        sticker_file_ext = self._find_file_ext(sticker_doc) or "unknown_filetype"
        # Create sticker set directory
        sticker_set_directory = "store/stickers/Unknown/"
        if sticker_set_id is not None:
            sticker_set_directory = f"store/stickers/{sticker_set_id}"
        os.makedirs(sticker_set_directory, exist_ok=True)
        # Download sticker
        return f"{sticker_set_directory}/{sticker_id}.{sticker_file_ext}"

    async def _refresh_sticker_doc(
            self,
            queue: ArchiveRunQueue[StickerQueueInfo, StickerQueueEntry],
            queue_entry: StickerQueueEntry,
    ) -> Optional[Document]:
        # Handy shortcut variables
        chat_id = queue.info.chat_id
        archive_target = queue.info.archive_target
        old_message = queue_entry.message
        message_id = queue_entry.message_id
        sticker_id = queue_entry.sticker_id
        # Actually refresh the message
        message = await self.message_refresher.get_message(chat_id, message_id, old_message, archive_target)
        logger.info("Fetched new message for message ID %s", message_id)
        # Grab the new sticker document from the message
        new_sticker_doc = message.sticker if hasattr(message, "sticker") else None
        if new_sticker_doc is None:
            logger.info("Message ID %s no longer contains a sticker.", message_id)
            return None
        # If the sticker was originally direct from the message, use that sticker and try download again
        if queue_entry.direct_from_msg:
            logger.info("Using the updated sticker from refreshed message ID %s", message_id)
            return new_sticker_doc
        # Otherwise, look up the sticker pack again
        logger.info("Looking up sticker pack in message ID %s again to find the sticker", message_id)
        sticker_set_id = self._find_sticker_set_id(new_sticker_doc)
        set_cache_entry = self._sticker_set_fetch_cache.get_resource_id_entry(sticker_set_id)
        if set_cache_entry is None:
            input_sticker_set = self._find_input_sticker_set(new_sticker_doc)
            sticker_set = await self._fetch_sticker_set(input_sticker_set)
        else:
            sticker_set = set_cache_entry.value
        # Go through the pack to find the sticker matching this one, if exists
        matching_sticker = self._find_sticker_in_set_by_id(sticker_set, sticker_id)
        if matching_sticker is None:
            logger.info("Sticker ID %s no longer exists in that sticker set ID %s", sticker_id, sticker_set_id)
            return None
        logger.info("Re-trying download with updated sticker ID %s document from set", sticker_id)
        return matching_sticker

    async def _download_sticker(
            self,
            queue: ArchiveRunQueue[StickerQueueInfo, StickerQueueEntry],
            queue_entry: StickerQueueEntry,
            sticker_doc: Document,
    ):
        # Loop until it's done
        while True:
            # Get basic sticker stuff
            sticker_id = self._find_sticker_id(sticker_doc)
            sticker_set_id = self._find_sticker_set_id(sticker_doc)
            # Check if file already exists
            sticker_file_path = self._sticker_file_path(sticker_doc)
            if os.path.exists(sticker_file_path):
                return sticker_doc
            logger.info("Downloading sticker, ID: %s, set ID: %s", sticker_id, sticker_set_id)
            try:
                # noinspection PyTypeChecker
                await self.client.download_media(sticker_doc, sticker_file_path)
                return sticker_doc
            except (FileReferenceExpiredError, LocationInvalidError):
                # Try refreshing the message
                message = queue_entry.message
                logger.warning("Sticker reference expired for message ID %s, will refresh message", message.id)
                new_sticker_doc = await self._refresh_sticker_doc(queue, queue_entry)
                if new_sticker_doc is None:
                    return sticker_doc
                sticker_doc = new_sticker_doc
            except Exception as e:
                logger.error("Failed to download sticker, (will retry) error:", exc_info=e)
                await asyncio.sleep(60)

    async def queue_sticker(
            self,
            queue_key: str,
            sticker_msg: telethon.types.Message,
            sticker_doc: Document,
            archive_target: ArchiveTarget,
            sticker_direct_from_msg: bool = True,
    ) -> None:
        if sticker_doc is None:
            return
        queue_info = StickerQueueInfo(archive_target.chat_id, archive_target)
        queue_entry = StickerQueueEntry(sticker_doc, sticker_msg, sticker_direct_from_msg)
        await self._add_queue_entry(queue_key, queue_info, queue_entry)
        archive_target.chat_db.save_subsystem_queue_entry(queue_entry.storable_entry)

    async def initialise_new_queue(self, new_queue: ArchiveRunQueue[StickerQueueInfo, StickerQueueEntry]) -> None:
        stored_queue_entries = new_queue.info.archive_target.chat_db.list_subsystem_queue_entries(StickerDownloaderQueueEntry.SUBSYSTEM_NAME)
        logger.info("Loading %s StickerDownloader queue entries from chat database for chat ID %s", len(stored_queue_entries), new_queue.info.chat_id)
        for stored_entry in stored_queue_entries:
            stored_media_entry = StickerDownloaderQueueEntry.from_generic(stored_entry)
            queue_entry = StickerQueueEntry(None, None, stored_media_entry.direct_from_msg, stored_media_entry)
            await self._add_queue_entry(new_queue.queue_key, new_queue.info, queue_entry)

    async def wait_until_queue_empty(self, queue_key: Optional[str]) -> None:
        return await self._wait_for_queue_and_message_refresher(queue_key, self.message_refresher)
