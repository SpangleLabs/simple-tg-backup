import asyncio
import dataclasses
import datetime
import logging
import os
from typing import Optional, TYPE_CHECKING

from prometheus_client import Counter
from telethon import TelegramClient
from telethon.errors import StickersetInvalidError
from telethon.tl.functions.messages import GetStickerSetRequest
from telethon.tl.types import DocumentAttributeSticker, InputStickerSetID, DocumentAttributeFilename, Document

from tg_backup.database.core_database import CoreDatabase
from tg_backup.models.abstract_resource import save_if_not_duplicate
from tg_backup.models.sticker import Sticker
from tg_backup.models.sticker_set import StickerSet
from tg_backup.subsystems.abstract_subsystem import AbstractSubsystem, TimedCache
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


@dataclasses.dataclass
class StickerQueueEntry:
    sticker_doc: Document


class StickerDownloader(AbstractSubsystem):
    CACHE_EXPIRY = datetime.timedelta(days=1)

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
        self.queue: asyncio.Queue[StickerQueueEntry] = asyncio.Queue()
        self._seen_sticker_set_ids = TimedCache[int, None](self.CACHE_EXPIRY) # Which sticker sets have been seen and listed
        self._seen_sticker_ids = TimedCache[int, None](self.CACHE_EXPIRY) # Which stickers have already been saved in the database

    def is_sticker_set_cached(self, sticker_set_id: int) -> bool:
        return self._seen_sticker_set_ids.is_resource_id_cached(sticker_set_id)

    def cache_sticker_set_id(self, sticker_set_id: int) -> None:
        self._seen_sticker_set_ids.cache_resource_id(sticker_set_id)

    def is_sticker_cached(self, sticker_id: int) -> bool:
        return self._seen_sticker_ids.is_resource_id_cached(sticker_id)

    def cache_sticker_id(self, sticker_id: int) -> None:
        self._seen_sticker_ids.cache_resource_id(sticker_id)

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

    async def _process_sticker_set(self, input_sticker_set: InputStickerSetID):
        if input_sticker_set is None:
            return
        sticker_set_id = input_sticker_set.id if hasattr(input_sticker_set, "id") else None
        if self.is_sticker_set_cached(sticker_set_id):
            return
        sticker_sets_processed_count.inc()
        # Fetch sticker set data
        try:
            sticker_set = await self.client(GetStickerSetRequest(
                stickerset=input_sticker_set,
                hash=0,
            ))
        except StickersetInvalidError:
            sticker_set_data_failure_count.inc()
            logger.warning("Could not fetch sticker set: %s. Pack may have been deleted", sticker_set_id)
            return
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
        # Put the rest of the pack in the queue
        for sticker_doc in sticker_set.documents:
            await self.queue_sticker(sticker_doc)
        self.cache_sticker_set_id(sticker_set_id)

    async def _do_process(self) -> None:
        queue_entry = self.queue.get_nowait()
        sticker_doc = queue_entry.sticker_doc
        if sticker_doc is None:
            return
        sticker_id = sticker_doc.id if hasattr(sticker_doc, "id") else None
        stickers_processed_count.inc()
        # Check if sticker has been saved
        if self.is_sticker_cached(sticker_id):
            return
        # Find sticker set and file extension
        input_sticker_set = self._find_input_sticker_set(sticker_doc)
        sticker_set_id = input_sticker_set.id if hasattr(input_sticker_set, "id") else None
        sticker_file_ext = self._find_file_ext(sticker_doc) or "unknown_filetype"
        # Create storable sticker object
        sticker_obj = Sticker.from_sticker(sticker_doc)
        # Create sticker set directory
        sticker_set_directory = "store/stickers/Unknown/"
        if sticker_set_id is not None:
            sticker_set_directory = f"store/stickers/{sticker_set_id}"
        os.makedirs(sticker_set_directory, exist_ok=True)
        # Download sticker
        sticker_file_path = f"{sticker_set_directory}/{sticker_id}.{sticker_file_ext}"
        if not os.path.exists(sticker_file_path):
            while True:
                logger.info("Downloading sticker, ID: %s, set ID: %s", sticker_id, sticker_set_id)
                try:
                    # noinspection PyTypeChecker
                    await self.client.download_media(sticker_doc, sticker_file_path)
                except Exception as e:
                    logger.error("Failed to download sticker, (will retry) error:", exc_info=e)
                    await asyncio.sleep(60) # TODO: sometimes it gets LocationInvalid?? What then?
                else:
                    break
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
        if input_sticker_set is not None:
            await self._process_sticker_set(input_sticker_set)

    def queue_size(self) -> int:
        return self.queue.qsize()

    async def queue_sticker(self, sticker_doc: Document) -> None:
        if sticker_doc is None:
            return
        await self.queue.put(StickerQueueEntry(sticker_doc))

# File storage
# - `store/stickers/` Stickers get stored in a separate directory to chats
# - `store/stickers/<pack_id>/` Each sticker pack gets a directory by pack ID
# - `store/stickers/<pack_id>/<sticker_id>.webp` Each sticker is stored by sticker ID