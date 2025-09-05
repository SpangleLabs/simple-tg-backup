import asyncio
import json
import logging
import os
import sys
from logging.handlers import TimedRotatingFileHandler

from telethon.tl.types import ChannelAdminLogEventActionDeleteMessage, DocumentAttributeFilename

from telethon import TelegramClient

import click

logger = logging.getLogger(__name__)


def setup_logging(log_level: str = "INFO") -> None:
    os.makedirs("logs", exist_ok=True)
    formatter = logging.Formatter("{asctime}:{levelname}:{name}:{message}", style="{")

    base_logger = logging.getLogger()
    base_logger.setLevel(log_level.upper())
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    base_logger.addHandler(console_handler)
    file_handler = TimedRotatingFileHandler("logs/backups.log", when="midnight")
    file_handler.setFormatter(formatter)
    base_logger.addHandler(file_handler)


class MediaDownloader:
    def __init__(self, client: TelegramClient) -> None:
        self.client = client
        self.queue: asyncio.Queue[object] = asyncio.Queue()
        self.running = False
        self.stop_when_empty = False
        self.seen_media_ids = set()

    async def run(self) -> None:
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



async def storable_object(obj: object, media_dl: MediaDownloader, **kwargs) -> dict:
    # TODO: move this into a class, so media_dl doesn't need passing
    data = {
        "type": type(obj).__name__,
        "id": obj.id if hasattr(obj, "id") else None,
        "str": str(obj)
    }
    if hasattr(obj, "date"):
        data["date"] = obj.date.isoformat()
    if hasattr(obj, "text"):
        data["text"] = obj.text
    if hasattr(obj, "media"):
        data["media"] = await storable_object(obj.media, media_dl)
        await media_dl.queue_media(obj.media)
    return data | kwargs


async def archive_chat(conf_data: dict, chat_id: int) -> None:
    tg_client = TelegramClient("simple_backup", conf_data["client"]["api_id"], conf_data["client"]["api_hash"])
    await tg_client.start()
    # Create the media downloader
    media_dl = MediaDownloader(tg_client)
    media_dl_task = asyncio.create_task(media_dl.run())
    # Get chat data
    chat = await tg_client.get_entity(chat_id)
    basic_data = {
        "chat": await storable_object(chat, media_dl),
        "admin_events": [],
        "messages": [],
    }
    logger.info("Got chat data: %s", chat)
    # Gather data from admin log
    async for evt in tg_client.iter_admin_log(chat):
        logger.info("Processing admin event ID: %s", evt.id)
        basic_data["admin_events"].append(await storable_object(evt, media_dl))
        evt_type = type(evt.action)
        if evt_type == ChannelAdminLogEventActionDeleteMessage:
            msg = evt.action.message
            basic_data["messages"].append(await storable_object(msg, media_dl, deleted=True))
    # Gather messages from chat
    async for msg in tg_client.iter_messages(chat):
        logger.info("Processing message ID: %s", msg.id)
        basic_data["messages"].append(await storable_object(msg, media_dl))
    # Store the message data
    os.makedirs("store", exist_ok=True)
    with open(f"store/{chat_id}.json", "w") as f:
        json.dump(basic_data, f, indent=2)
    # Wait for media downloader to complete
    logger.info("Awaiting completion of media downloader")
    media_dl.mark_as_filled()
    await media_dl_task


@click.command()
@click.option("--log-level", type=str, help="Log level for the logger", default="INFO")
@click.option("--chat-id", type=int, help="ID of the telegram chat to emergency save deleted messages", required=True)
def main(log_level: str, chat_id: int) -> None:
    setup_logging(log_level)
    with open("config.json") as f:
        conf_data = json.load(f)
    asyncio.run(archive_chat(conf_data, chat_id))


if __name__ == "__main__":
    main()
