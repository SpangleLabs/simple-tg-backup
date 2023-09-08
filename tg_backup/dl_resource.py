import dataclasses
from typing import Dict, Any, Optional, List


@dataclasses.dataclass
class DLResource:
    json_path: str
    raw_data: Dict


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


@dataclasses.dataclass
class DLResourcePeerChat(DLResource):
    chat_id: int

    def __hash__(self) -> int:
        return hash(("peer_chat", self.chat_id))


@dataclasses.dataclass
class DLResourcePeerChannel(DLResource):
    channel_id: int

    def __hash__(self) -> int:
        return hash(("peer_channel", self.channel_id))


@dataclasses.dataclass
class DLResourceMedia(DLResource):
    media_id: int
    access_hash: int
    file_reference: bytes


@dataclasses.dataclass
class DLResourcePhoto(DLResourceMedia):

    def __hash__(self) -> int:
        return hash(("photo", self.media_id))


@dataclasses.dataclass
class DLResourceDocument(DLResourceMedia):

    def __hash__(self) -> int:
        return hash(("document", self.media_id))


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
