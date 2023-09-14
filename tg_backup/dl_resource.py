import dataclasses
import logging
from abc import abstractmethod
from typing import Dict, Any, Optional, List

from telethon import TelegramClient
from telethon.errors import ChannelPrivateError, FileReferenceExpiredError
from telethon.tl.functions.channels import GetFullChannelRequest
from telethon.tl.functions.messages import GetFullChatRequest
from telethon.tl.functions.users import GetFullUserRequest
from telethon.tl.patched import Message
from telethon.tl.types import InputPhotoFileLocation

from tg_backup.config import OutputConfig, StorableData
from tg_backup.tg_utils import get_from_obj_by_path

logger = logging.getLogger(__name__)


@dataclasses.dataclass
class DLResource:
    msg: Message
    json_path: str
    raw_data: Dict

    @abstractmethod
    def __hash__(self) -> int:
        raise NotImplementedError

    @abstractmethod
    def __eq__(self, other) -> bool:
        raise NotImplementedError

    @abstractmethod
    async def download(self, client: TelegramClient, output: OutputConfig) -> None:
        raise NotImplementedError


@dataclasses.dataclass
class DLResourcePeerID(DLResource):
    """This seems to be just for bot IDs really"""
    peer_id: int

    def __hash__(self) -> int:
        return hash(("peer_id", self.peer_id))

    def __eq__(self, other) -> bool:
        return any([
            isinstance(other, DLResourcePeerID) and self.peer_id == other.peer_id,
            isinstance(other, DLResourcePeerUser) and self.peer_id == other.user_id,
            isinstance(other, DLResourcePeerChat) and self.peer_id == other.chat_id,
            isinstance(other, DLResourcePeerChannel) and self.peer_id == other.channel_id,
        ])

    async def download(self, client: TelegramClient, output: OutputConfig) -> None:
        chat_data = output.chats.load_chat(self.peer_id)
        if chat_data:
            return
        user_request = GetFullUserRequest(self.peer_id)
        user_full = await client(user_request)
        user_full_data = user_full.to_dict()
        output.chats.save_chat(self.peer_id, StorableData(user_full_data))


@dataclasses.dataclass
class DLResourcePeerUser(DLResource):
    user_id: int

    def __hash__(self) -> int:
        return hash(("peer_user", self.user_id))

    def __eq__(self, other) -> bool:
        return isinstance(other, DLResourcePeerUser) and self.user_id == other.user_id

    async def download(self, client: TelegramClient, output: OutputConfig) -> None:
        chat_data = output.chats.load_chat(self.user_id)
        if chat_data:
            return
        user_request = GetFullUserRequest(self.user_id)
        user_full = await client(user_request)
        user_full_data = user_full.to_dict()
        output.chats.save_chat(self.user_id, StorableData(user_full_data))


@dataclasses.dataclass
class DLResourcePeerChat(DLResource):
    chat_id: int

    def __hash__(self) -> int:
        return hash(("peer_chat", self.chat_id))

    def __eq__(self, other) -> bool:
        return isinstance(other, DLResourcePeerChat) and self.chat_id == other.chat_id

    async def download(self, client: TelegramClient, output: OutputConfig) -> None:
        chat_data = output.chats.load_chat(self.chat_id)
        if chat_data:
            return
        chat_request = GetFullChatRequest(self.chat_id)
        chat_full = await client(chat_request)
        chat_full_data = chat_full.to_dict()
        output.chats.save_chat(self.chat_id, StorableData(chat_full_data))


@dataclasses.dataclass
class DLResourcePeerChannel(DLResource):
    channel_id: int

    def __hash__(self) -> int:
        return hash(("peer_channel", self.channel_id))

    def __eq__(self, other) -> bool:
        return isinstance(other, DLResourcePeerChannel) and self.channel_id == other.channel_id

    async def download(self, client: TelegramClient, output: OutputConfig) -> None:
        chat_data = output.chats.load_chat(self.channel_id)
        if chat_data:
            return
        chat_request = GetFullChannelRequest(self.channel_id)
        try:
            chat_full = await client(chat_request)
        except ChannelPrivateError:
            logger.warning(
                "Could not download channel %s referenced in message %s. Channel is private.",
                self.channel_id,
                self.msg.id,
            )
            return
        chat_full_data = chat_full.to_dict()
        output.chats.save_chat(self.channel_id, StorableData(chat_full_data))


@dataclasses.dataclass
class DLResourceMedia(DLResource):
    media_id: int
    access_hash: int
    file_reference: bytes


@dataclasses.dataclass
class DLResourcePhoto(DLResourceMedia):
    photo_size_type: str

    def __hash__(self) -> int:
        return hash(("photo", self.media_id))

    def __eq__(self, other) -> bool:
        return isinstance(other, DLResourcePhoto) and self.media_id == other.media_id

    async def download(self, client: TelegramClient, output: OutputConfig) -> None:
        if output.photos.photo_exists(self.media_id):
            return
        with output.photos.open_photo(self.media_id) as f:
            out_path = None
            try:
                out_path = await client.download_media(self.msg, f)
            except FileReferenceExpiredError:
                pass
            if not out_path:
                msg_data = (self.msg.input_chat, self.msg.id) if self.msg.input_chat else None
                try:
                    input_photo = InputPhotoFileLocation(self.media_id, self.access_hash, self.file_reference, self.photo_size_type)
                    await client._download_file(input_photo, f, msg_data=msg_data)
                except FileReferenceExpiredError:
                    logger.debug("File reference expired, re-fetching message")
                    new_msg = client.get_messages(self.msg.input_chat, ids=self.msg.id)
                    new_file_ref = get_from_obj_by_path(new_msg, f"{self.json_path}.file_reference")
                    input_photo = InputPhotoFileLocation(self.media_id, self.access_hash, new_file_ref, self.photo_size_type)
                    await client._download_file(input_photo, f, msg_data=msg_data)
        output.photos.save_metadata(self.media_id, StorableData(self.raw_data))


@dataclasses.dataclass
class DLResourceDocument(DLResourceMedia):

    def __hash__(self) -> int:
        return hash(("document", self.media_id))

    def __eq__(self, other) -> bool:
        return isinstance(other, DLResourceDocument) and self.media_id == other.media_id

    def file_ext(self) -> str:
        for attr in self.raw_data["attributes"]:
            if attr["_"] == "DocumentAttributeFilename":
                return attr["file_name"].split(".")[-1]
        if self.raw_data["mime_type"]:
            return self.raw_data["mime_type"].split("/")[-1]
        return "unknown"

    async def download(self, client: TelegramClient, output: OutputConfig) -> None:
        file_ext = self.file_ext()
        if output.documents.file_exists(self.media_id):
            return
        with output.documents.open_file(self.media_id, file_ext) as f:
            out_path = await client.download_media(self.msg, f)
            if not out_path:
                raise ValueError(f"Failed to download document from message: {self.msg.id}")
        output.documents.save_metadata(self.media_id, StorableData(self.raw_data))


@dataclasses.dataclass
class DLResourceMediaUnknown(DLResourceMedia):
    pass


def search_for_resources(msg: Message, raw_data: Any, json_path: str) -> Optional[List[DLResource]]:
    if isinstance(raw_data, List):
        resources = []
        for n, item in enumerate(raw_data):
            item_resources = search_for_resources(msg, item, f"{json_path}[{n}]")
            if item_resources:
                resources.extend(item_resources)
        return resources
    if not isinstance(raw_data, Dict):
        return None
    resources = []
    user_id = raw_data.get("user_id")
    if user_id:
        resources.append(DLResourcePeerUser(msg, json_path, raw_data, user_id))
    chat_id = raw_data.get("chat_id")
    if chat_id:
        resources.append(DLResourcePeerChat(msg, json_path, raw_data, chat_id))
    channel_id = raw_data.get("channel_id")
    if channel_id:
        resources.append(DLResourcePeerChannel(msg, json_path, raw_data, channel_id))
    maybe_id = raw_data.get("id")
    access_hash = raw_data.get("access_hash")
    file_ref = raw_data.get("file_reference")
    if maybe_id and access_hash and file_ref:
        if raw_data["_"] == "Photo":
            photo_size = raw_data["sizes"][-1]["type"]
            resources.append(DLResourcePhoto(msg, json_path, raw_data, maybe_id, access_hash, file_ref, photo_size))
        elif raw_data["_"] == "Document":
            resources.append(DLResourceDocument(msg, json_path, raw_data, maybe_id, access_hash, file_ref))
        else:
            resources.append(DLResourceMediaUnknown(msg, json_path, raw_data, maybe_id, access_hash, file_ref))
    # Check for nested resources
    for key, value in raw_data.items():
        item_resources = search_for_resources(msg, value, f"{json_path}.{key}")
        if item_resources:
            resources.extend(item_resources)
    return resources


def resources_in_msg(msg: Message, msg_data: Dict) -> List[DLResource]:
    resources = []
    # Handle "via_bot_id" key
    via_bot_id = msg_data.get("via_bot_id")
    if via_bot_id:
        resources.append(DLResourcePeerID(msg, ".via_bot_id", msg_data, via_bot_id))
    # Search for others
    resources += search_for_resources(msg, msg_data, "")
    return resources
