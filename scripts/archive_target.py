import base64
import datetime
import json
import logging
import os
from typing import TYPE_CHECKING

from telethon.tl.types import ChannelAdminLogEventActionDeleteMessage

from scripts.config import BehaviourConfig

if TYPE_CHECKING:
    from scripts.archiver import Archiver


logger = logging.getLogger(__name__)



def encode_json_extra(value: object) -> str:
    if isinstance(value, bytes):
        return base64.b64encode(value).decode('ascii')
    elif isinstance(value, datetime.datetime):
        return value.isoformat()
    else:
        raise ValueError(f"Unrecognised type to encode: {value}")


class ArchiveTarget:
    def __init__(self, chat_id: int, behaviour: BehaviourConfig, archiver: "Archiver") -> None:
        self.chat_id = chat_id
        self.behaviour = behaviour
        self.archiver = archiver
        self.client = archiver.client

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
            data["media"] = await self.storable_object(obj.media, behaviour)
            if self.behaviour.download_media:
                await self.archiver.media_dl.queue_media(obj.media)
        return data | kwargs

    async def archive_chat(self) -> None:
        # Get chat data
        chat = await self.client.get_entity(self.chat_id)
        basic_data = {
            "chat": await self.storable_object(chat),
            "admin_events": [],
            "messages": [],
        }
        logger.info("Got chat data: %s", chat)
        # Gather data from admin log
        if self.behaviour.check_admin_log:
            async for evt in self.client.iter_admin_log(chat):
                logger.info("Processing admin event ID: %s", evt.id)
                basic_data["admin_events"].append(await self.storable_object(evt))
                evt_type = type(evt.action)
                if evt_type == ChannelAdminLogEventActionDeleteMessage:
                    msg = evt.action.message
                    basic_data["messages"].append(await self.storable_object(msg, deleted=True))
        # Gather messages from chat
        if self.behaviour.archive_history:
            async for msg in self.client.iter_messages(chat):
                logger.info("Processing message ID: %s", msg.id)
                basic_data["messages"].append(await self.storable_object(msg))
        # Store the message data
        os.makedirs("store", exist_ok=True)
        with open(f"store/{self.chat_id}.json", "w") as f:
            json.dump(basic_data, f, indent=2, default=encode_json_extra)
