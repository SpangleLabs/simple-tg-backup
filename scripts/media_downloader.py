import asyncio
import logging
import os

from telethon import TelegramClient
from telethon.tl.types import DocumentAttributeFilename


logger = logging.getLogger(__name__)


class MediaDownloader:
    def __init__(self, client: TelegramClient) -> None:
        self.client = client
        self.queue: asyncio.Queue[object] = asyncio.Queue()
        self.running = False
        self.stop_when_empty = False
        self.seen_media_ids = set()

    async def run(self) -> None:
        return
        self.running = True
        while self.running:
            try:
                media = self.queue.get_nowait()
            except asyncio.QueueEmpty:
                if self.stop_when_empty:
                    logger.info("Queue is empty, shutting down media downloader")
                    return
                await asyncio.sleep(1)
                continue
            media_type = type(media).__name__
            media_ext = ".unknown_filetype"
            if hasattr(media, "photo"):
                media_id = media.photo.id
                media_ext = ".jpg"
            elif hasattr(media, "document"):
                media_id = media.document.id
                for attr in media.document.attributes:
                    if type(attr) == DocumentAttributeFilename:
                        media_ext = "." + attr.file_name.split(".")[-1]
            elif hasattr(media, "webpage"):
                continue  # TODO: download them, in the fullness of time.
            else:
                raise ValueError(f"Unrecognised media type: {media_type}")
            target_path = f"store/media/{media_id}{media_ext}"
            if os.path.exists(target_path):
                # TODO: maybe not this?
                logger.info("Skipping download of pre-existing file")
                continue
            # Download the media
            logger.info("Downloading media, type: %s, ID: %s", media_type, media_id)
            await self.client.download_media(media, target_path)
            logger.info("Media download complete, type: %s, ID: %s", media_type, media_id)
            logger.info("There are %s remaining items in the media queue", self.queue.qsize())

    def stop(self) -> None:
        self.running = False

    def mark_as_filled(self) -> None:
        self.stop_when_empty = True

    async def queue_media(self, media: object) -> None:
        if media is None:
            return
        await self.queue.put(media)
