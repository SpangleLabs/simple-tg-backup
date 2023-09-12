import dataclasses
from abc import abstractmethod
from typing import Dict, Any, Optional, List

from telethon import TelegramClient
from telethon.tl.functions.channels import GetFullChannelRequest
from telethon.tl.functions.messages import GetFullChatRequest
from telethon.tl.functions.users import GetFullUserRequest
from telethon.tl.types import InputPhoto, Photo, InputPhotoFileLocation, InputUser, InputFile, InputDocument, \
    InputDocumentFileLocation

from tg_backup.config import OutputConfig, StorableData


@dataclasses.dataclass
class DLResource:
    json_path: str
    raw_data: Dict

    @abstractmethod
    def __hash__(self) -> int:
        raise NotImplementedError

    @abstractmethod
    async def download(self, client: TelegramClient, output: OutputConfig) -> None:
        raise NotImplementedError


@dataclasses.dataclass
class DLResourcePeerID(DLResource):
    peer_id: int

    def __hash__(self) -> int:
        return hash(("peer_id", self.peer_id))


@dataclasses.dataclass
class DLResourcePeerUser(DLResource):
    user_id: int

    def __hash__(self) -> int:
        return hash(("peer_user", self.user_id))

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

    async def download(self, client: TelegramClient, output: OutputConfig) -> None:
        chat_data = output.chats.load_chat(self.channel_id)
        if chat_data:
            return
        chat_request = GetFullChannelRequest(self.channel_id)
        chat_full = await client(chat_request)
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

    async def download(self, client: TelegramClient, output: OutputConfig) -> None:
        if output.photos.photo_exists(self.media_id):
            return
        with output.photos.open_file(self.media_id) as f:
            input_photo = InputPhotoFileLocation(self.media_id, self.access_hash, self.file_reference, self.photo_size_type)
            await client.download_file(input_photo, f)
        output.photos.save_metadata(self.media_id, StorableData(self.raw_data))


@dataclasses.dataclass
class DLResourceDocument(DLResourceMedia):

    def __hash__(self) -> int:
        return hash(("document", self.media_id))

    def file_ext(self) -> str:
        for attr in self.raw_data["attributes"]:
            if attr["_"] == "DocumentAttributeFilename":
                return attr["file_name"].split(".")[-1]
        if self.raw_data["mime_type"]:
            return self.raw_data["mime_type"].split("/")[-1]
        return "unknown"

    async def download(self, client: TelegramClient, output: OutputConfig) -> None:
        file_ext = self.file_ext()  # TODO You know, maybe just use the message object? It might refresh the file ref
        if output.documents.file_exists(self.media_id, file_ext):
            return
        with output.documents.open_file(self.media_id, file_ext) as f:
            input_doc = InputDocumentFileLocation(self.media_id, self.access_hash, self.file_reference, "")
            await client.download_file(input_doc, f)
        output.documents.save_metadata(self.media_id, StorableData(self.raw_data))


@dataclasses.dataclass
class DLResourceMediaUnknown(DLResourceMedia):
    pass


def search_for_resources(raw_data: Any, json_path: str) -> Optional[List[DLResource]]:
    if isinstance(raw_data, List):
        resources = []
        for n, item in enumerate(raw_data):
            item_resources = search_for_resources(item, f"{json_path}[{n}]")
            if item_resources:
                resources.extend(item_resources)
        return resources
    if not isinstance(raw_data, Dict):
        return None
    resources = []
    user_id = raw_data.get("user_id")
    if user_id:
        resources.append(DLResourcePeerUser(json_path, raw_data, user_id))
    chat_id = raw_data.get("chat_id")
    if chat_id:
        resources.append(DLResourcePeerChat(json_path, raw_data, chat_id))
    channel_id = raw_data.get("channel_id")
    if channel_id:
        resources.append(DLResourcePeerChannel(json_path, raw_data, channel_id))
    maybe_id = raw_data.get("id")
    access_hash = raw_data.get("access_hash")
    file_ref = raw_data.get("file_reference")
    if maybe_id and access_hash and file_ref:
        if raw_data["_"] == "Photo":
            photo_size = raw_data["sizes"][-1]["type"]
            resources.append(DLResourcePhoto(json_path, raw_data, maybe_id, access_hash, file_ref, photo_size))
        elif raw_data["_"] == "Document":
            resources.append(DLResourceDocument(json_path, raw_data, maybe_id, access_hash, file_ref))
        else:
            resources.append(DLResourceMediaUnknown(json_path, raw_data, maybe_id, access_hash, file_ref))
    # Check for nested resources
    for key, value in raw_data.items():
        item_resources = search_for_resources(value, f"{json_path}.{key}")
        if item_resources:
            resources.extend(item_resources)
    return resources


def resources_in_msg(msg_data: Dict) -> List[DLResource]:
    resources = []
    # Handle "via_bot_id" key
    via_bot_id = msg_data.get("via_bot_id")
    if via_bot_id:
        resources.append(DLResourcePeerID(".via_bot_id", msg_data, via_bot_id))
    # Search for others
    resources += search_for_resources(msg_data, "")
    return resources
