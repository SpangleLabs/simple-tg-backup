import dataclasses
import logging
from typing import Dict, Optional, List, Any

from telethon.tl.custom import Message

logger = logging.getLogger(__name__)


@dataclasses.dataclass
class DLResource:
    json_path: str
    raw_data: Dict


@dataclasses.dataclass
class DLResourcePeerID(DLResource):
    peer_id: int


@dataclasses.dataclass
class DLResourcePeerUser(DLResource):
    user_id: int


@dataclasses.dataclass
class DLResourcePeerChat(DLResource):
    chat_id: int


@dataclasses.dataclass
class DLResourcePeerChannel(DLResource):
    channel_id: int


@dataclasses.dataclass
class DLResourceMedia(DLResource):
    media_id: int
    access_hash: int
    file_reference: bytes


@dataclasses.dataclass
class DLResourcePhoto(DLResourceMedia):
    pass


@dataclasses.dataclass
class DLResourceDocument(DLResourceMedia):
    pass


@dataclasses.dataclass
class DLResourceMediaUnknown(DLResourceMedia):
    pass


@dataclasses.dataclass
class EncodedMessage:
    raw_data: Dict
    downloadable_resources: List[DLResource]


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
            resources.append(DLResourcePhoto(json_path, raw_data, maybe_id, access_hash, file_ref))
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


def encode_message(msg: Message) -> EncodedMessage:
    msg_data = msg.to_dict()
    resources = resources_in_msg(msg_data)
    print(resources)
    resource_types = [type(r).__name__ for r in resources]
    print(f"Found {len(resources)} resources: {resource_types}")
    if "DLResourceMediaUnknown" in resource_types:
        raise ValueError(f"Found unknown media: {resources}")
    return EncodedMessage(
        msg_data,
        resources
    )
