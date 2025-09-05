import asyncio
import base64
import datetime
import json
import os
from typing import Optional

from telethon import TelegramClient
from telethon.tl.types import ChannelAdminLogEventActionDeleteMessage

from scripts.emergency_backup import logger
from scripts.config import Config
from scripts.media_downloader import MediaDownloader


def encode_json_extra(value: object) -> str:
    if isinstance(value, bytes):
        return base64.b64encode(value).decode('ascii')
    elif isinstance(value, datetime.datetime):
        return value.isoformat()
    else:
        raise ValueError(f"Unrecognised type to encode: {value}")


class Archiver:
    def __init__(self, conf: Config) -> None:
        self.client = TelegramClient("simple_backup", conf.client.api_id, conf.client.api_hash)
        self.started = False
        self.media_dl = MediaDownloader(self.client)
        self.media_dl_task: Optional[asyncio.Task] = None

    async def start(self) -> None:
        await self.client.start()
        asyncio.create_task(self.media_dl.run())
        self.started = True

    async def storable_object(self, obj: object, **kwargs) -> dict:
        data = {
            "type": type(obj).__name__,
            "id": obj.id if hasattr(obj, "id") else None,
            "str": str(obj),
            "dict": obj.to_dict() if hasattr(obj, "to_dict") else None,
        }
        if hasattr(obj, "date"):
            data["date"] = obj.date.isoformat()
        if hasattr(obj, "text"):
            data["text"] = obj.text
        if hasattr(obj, "media"):
            data["media"] = await self.storable_object(obj.media)
            await self.media_dl.queue_media(obj.media)
        return data | kwargs

    async def archive_chat(self, chat_id: int) -> None:
        await self.start()
        # Get chat data
        chat = await self.client.get_entity(chat_id)
        basic_data = {
            "chat": await storable_object(chat, media_dl),
            "admin_events": [],
            "messages": [],
        }
        logger.info("Got chat data: %s", chat)
        # Gather data from admin log
        async for evt in self.client.iter_admin_log(chat):
            logger.info("Processing admin event ID: %s", evt.id)
            basic_data["admin_events"].append(await self.storable_object(evt))
            evt_type = type(evt.action)
            if evt_type == ChannelAdminLogEventActionDeleteMessage:
                msg = evt.action.message
                basic_data["messages"].append(await self.storable_object(msg, deleted=True))
        # Gather messages from chat
        async for msg in self.client.iter_messages(chat):
            logger.info("Processing message ID: %s", msg.id)
            basic_data["messages"].append(await self.storable_object(msg))
        # Store the message data
        os.makedirs("store", exist_ok=True)
        with open(f"store/{chat_id}.json", "w") as f:
            json.dump(basic_data, f, indent=2, default=encode_json_extra)
        # Wait for media downloader to complete
        logger.info("Awaiting completion of media downloader")
        self.media_dl.mark_as_filled()
        await self.media_dl_task
