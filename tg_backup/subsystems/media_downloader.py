import asyncio
import dataclasses
import logging
import os
from typing import Optional

import telethon
from prometheus_client import Counter
from telethon import TelegramClient
from telethon.tl.types import DocumentAttributeFilename, MessageMediaPhoto, MessageMediaDocument, MessageMediaWebPage, \
    MessageMediaGeo, MessageMediaGeoLive, MessageMediaPoll, MessageMediaDice, MessageMediaContact, MessageMediaToDo, \
    MessageMediaGiveaway, MessageMediaGiveawayResults, MessageMediaPaidMedia, MessageMediaStory, MessageMediaInvoice, \
    MessageMediaVenue, MessageMediaGame

from tg_backup.subsystems.abstract_subsystem import AbstractSubsystem

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
    chat_id: int
    message: telethon.types.Message


@dataclasses.dataclass
class MediaInfo:
    media_type: str
    media_id: int
    file_ext: str


class MediaDownloader(AbstractSubsystem):
    MEDIA_NO_ACTION_NEEDED = [MessageMediaGeo, MessageMediaGeoLive, MessageMediaDice, MessageMediaToDo]
    MEDIA_TO_DO = [MessageMediaWebPage, MessageMediaPoll, MessageMediaContact]
    MEDIA_IGNORE = [MessageMediaGiveaway, MessageMediaGiveawayResults, MessageMediaPaidMedia, MessageMediaStory, MessageMediaGame, MessageMediaInvoice, MessageMediaVenue]

    def __init__(self, client: TelegramClient) -> None:
        super().__init__(client)
        self.queue: asyncio.Queue[MediaQueueEntry] = asyncio.Queue()

    def _parse_media_info(self, msg: telethon.types.Message, chat_id: int) -> Optional[MediaInfo]:
        # Skip if not media
        if not hasattr(msg, "media"):
            return None
        # Start checking media type
        media_ext = "unknown_filetype"
        if hasattr(msg, "media"):
            media_type = type(msg.media)
            media_type_name = media_type.__name__
            if isinstance(msg.media, MessageMediaPhoto):
                if msg.media.photo is None:
                    logger.info("This timed photo has expired, cannot archive")
                    return None
                media_id = msg.media.photo.id
                media_ext = "jpg"
                return MediaInfo(media_type_name, media_id, media_ext)
            if isinstance(msg.media, MessageMediaDocument):
                if msg.media.document is None:
                    logger.info("This timed document has expired, cannot archive")
                    return None
                media_id = msg.media.document.id
                media_ext = self._document_file_ext(msg.media.document) or media_ext
                return MediaInfo(media_type_name, media_id, media_ext)
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
        queue_entry = self.queue.get_nowait()
        media_processed_count.inc()
        # Determine media folder
        chat_id = queue_entry.chat_id
        media_dir = f"store/chats/{chat_id}/media"
        os.makedirs(media_dir, exist_ok=True)
        # Determine media info
        media_info = self._parse_media_info(queue_entry.message, queue_entry.chat_id)
        if media_info is None:
            return
        # Construct file path
        target_path = f"{media_dir}/{media_info.media_id}.{media_info.file_ext}"
        if os.path.exists(target_path):
            logger.info("Skipping download of pre-existing file")
            return
        # Download the media
        logger.info("Downloading media, type: %s, ID: %s", media_info.media_type, media_info.media_id)
        await self.client.download_media(queue_entry.message, target_path)
        media_downloaded_count.inc()
        logger.info("Media download complete, type: %s, ID: %s", media_info.media_type, media_info.media_id)

    def queue_size(self) -> int:
        return self.queue.qsize()

    async def queue_media(self, chat_id: int, message: telethon.types.Message) -> None:
        if message is None:
            return
        await self.queue.put(MediaQueueEntry(chat_id, message))
