import asyncio
import dataclasses
import logging
import os
from typing import Optional

import telethon
from telethon import TelegramClient
from telethon.tl.types import DocumentAttributeFilename

from tg_backup.subsystems.abstract_subsystem import AbstractSubsystem

logger = logging.getLogger(__name__)


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
    def __init__(self, client: TelegramClient) -> None:
        super().__init__(client)
        self.queue: asyncio.Queue[MediaQueueEntry] = asyncio.Queue()
        self.seen_media_ids = set()

    def _parse_media_info(self, msg: object) -> Optional[MediaInfo]:
        # Skip if not media
        if not hasattr(msg, "media"):
            return None
        # Start checking media type
        media_ext = "unknown_filetype"
        if hasattr(msg, "media"):
            media_type = type(msg.media).__name__
            if hasattr(msg.media, "photo"):
                media_id = msg.media.photo.id
                media_ext = "jpg"
                return MediaInfo(media_type, media_id, media_ext)
            if hasattr(msg.media, "document"):
                media_id = msg.media.document.id
                for attr in msg.media.document.attributes:
                    if type(attr) == DocumentAttributeFilename:
                        media_ext = attr.file_name.split(".")[-1]
                return MediaInfo(media_type, media_id, media_ext)
            if hasattr(msg.media, "webpage"):
                logger.info("Downloading web page previews not currently supported")
                return None
            raise ValueError(f"Unrecognised media type: {media_type}")
        return None

    async def _do_process(self) -> None:
        queue_entry = self.queue.get_nowait()
        # Determine media folder
        chat_id = queue_entry.chat_id
        media_dir = f"store/chats/{chat_id}/media/"
        os.makedirs(media_dir, exist_ok=True)
        # Determine media info
        media_info = self._parse_media_info(queue_entry.message)
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
        logger.info("Media download complete, type: %s, ID: %s", media_info.media_type, media_info.media_id)
        logger.info("There are %s remaining items in the media queue", self.queue.qsize())

    async def queue_media(self, chat_id: int, message: telethon.types.Message) -> None:
        if message is None:
            return
        await self.queue.put(MediaQueueEntry(chat_id, message))
