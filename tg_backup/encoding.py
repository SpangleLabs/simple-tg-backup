import dataclasses
import logging
from typing import Dict, List

from telethon.tl.custom import Message

from tg_backup.dl_resource import DLResource, resources_in_msg

logger = logging.getLogger(__name__)


@dataclasses.dataclass
class EncodedMessage:
    raw_data: Dict
    downloadable_resources: List[DLResource]


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
