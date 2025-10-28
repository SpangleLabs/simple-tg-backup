import dataclasses
import logging
import os
import pathlib

import telethon
from prometheus_client import Counter
from telethon import TelegramClient
from telethon.tl.types import DocumentAttributeFilename, MessageMediaPhoto, MessageMediaDocument, MessageMediaWebPage, \
    MessageMediaGeo, MessageMediaGeoLive, MessageMediaPoll, MessageMediaDice, MessageMediaContact, MessageMediaToDo, \
    MessageMediaGiveaway, MessageMediaGiveawayResults, MessageMediaPaidMedia, MessageMediaStory, MessageMediaInvoice, \
    MessageMediaVenue, MessageMediaGame

from tg_backup.database.chat_database import ChatDatabase
from tg_backup.subsystems.abstract_subsystem import AbstractTargetQueuedSubsystem

logger = logging.getLogger(__name__)

media_processed_count = Counter(
    "tgbackup_mediadownloader_media_processed_count",
    "Total number of media-containing messages which have been picked from the queue by the MediaDownloader",
)
media_downloaded_count = Counter(
    "tgbackup_mediadownloader_media_downloaded_count",
    "Total number of media files which have been downloaded by the MediaDownloader",
)


@dataclasses.dataclass
class MediaQueueEntry:
    message: telethon.types.Message


@dataclasses.dataclass
class MediaInfo:
    media_subfolder: str
    media_type: str
    media_id: int
    file_ext: str


class MediaDownloader(AbstractTargetQueuedSubsystem[MediaQueueEntry]):
    MEDIA_NO_ACTION_NEEDED = [MessageMediaGeo, MessageMediaGeoLive, MessageMediaDice, MessageMediaToDo]
    MEDIA_TO_DO = [MessageMediaWebPage, MessageMediaPoll, MessageMediaContact]
    MEDIA_IGNORE = [MessageMediaGiveaway, MessageMediaGiveawayResults, MessageMediaPaidMedia, MessageMediaStory, MessageMediaGame, MessageMediaInvoice, MessageMediaVenue]
    UNKNOWN_FILE_EXT = "unknown_filetype"
    MEDIA_FOLDER = "media"

    def __init__(self, client: TelegramClient) -> None:
        super().__init__(client)
        self.queue: asyncio.Queue[MediaQueueEntry] = asyncio.Queue()

    def _parse_media_info(self, msg: telethon.types.Message, chat_id: int) -> Optional[MediaInfo]:
        # Skip if not media
        if not hasattr(msg, "media"):
            return None
        # Start checking media type
        media_ext = self.UNKNOWN_FILE_EXT
        if hasattr(msg, "media"):
            media_type = type(msg.media)
            media_type_name = media_type.__name__
            if isinstance(msg.media, MessageMediaPhoto):
                if msg.media.photo is None:
                    logger.info("This timed photo has expired, cannot archive")
                    return None
                media_id = msg.media.photo.id
                media_ext = "jpg"
                return MediaInfo(self.MEDIA_FOLDER, media_type_name, media_id, media_ext)
            if isinstance(msg.media, MessageMediaDocument):
                if msg.media.document is None:
                    logger.info("This timed document has expired, cannot archive")
                    return None
                media_id = msg.media.document.id
                media_ext = self._document_file_ext(msg.media.document) or media_ext
                return MediaInfo(self.MEDIA_FOLDER, media_type_name, media_id, media_ext)
            if media_type in self.MEDIA_NO_ACTION_NEEDED:
                logger.info("No action needed for data-only media type: %s", media_type_name)
                return None
            if media_type in self.MEDIA_TO_DO:
                logger.info(
                    "Media type not yet implemented: %s, chat ID: %s, msg ID: %s, date %s",
                    media_type_name, chat_id, getattr(msg, "id", None), getattr(msg, "date", None)
                ) # TODO: Implement these
                return None
            if media_type in self.MEDIA_IGNORE:
                logger.info(
                    "Media type ignored: %s, chat ID: %s, msg ID: %s, date %s",
                    media_type_name, chat_id, getattr(msg, "id", None), getattr(msg, "date", None)
                )
                return None
            logger.warning(
                "Unknown media type! %s, chat ID: %s, msg ID: %s, date %s",
                media_type_name, chat_id, getattr(msg, "id", None), getattr(msg, "date", None)
            )
            return None
        return None

    @staticmethod
    def _document_file_ext(doc: telethon.tl.types.Document) -> Optional[str]:
        for attr in doc.attributes:
            if type(attr) == DocumentAttributeFilename:
                return attr.file_name.split(".")[-1]

    async def _do_process(self) -> None:
        chat_queue, queue_entry = self._get_next_in_queue()
        chat_id = chat_queue.chat_id
        media_processed_count.inc()
        # Determine media info
        media_info = self._parse_media_info(queue_entry.message, chat_id)
        if media_info is None:
            return
        # Construct file path
        target_filename = f"{media_info.media_id}.{media_info.file_ext}"
        target_path = pathlib.Path("store") / "chats" / f"{chat_id}" / media_info.media_subfolder / target_filename
        os.makedirs(target_path.parent, exist_ok=True)
        if os.path.exists(target_path):
            logger.info("Skipping download of pre-existing file")
            return
        # Download the media
        logger.info("Downloading media, type: %s, ID: %s", media_info.media_type, media_info.media_id)
        await self.client.download_media(queue_entry.message, str(target_path))
        media_downloaded_count.inc()
        logger.info("Media download complete, type: %s, ID: %s", media_info.media_type, media_info.media_id)

    async def queue_media(
            self,
            queue_key: str,
            chat_id: int,
            chat_db: ChatDatabase,
            message: telethon.types.Message,
    ) -> None:
        entry = MediaQueueEntry(message)
        await self._add_queue_entry(queue_key, chat_id, chat_db, entry)
