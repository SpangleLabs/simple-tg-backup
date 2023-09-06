import asyncio
import json
import logging
from typing import Dict

from telethon import TelegramClient
from telethon.tl.custom import Message
from telethon.tl.functions.messages import GetHistoryRequest
from telethon.tl.types import PeerUser, MessageEntityUrl
from telethon.tl.types.messages import Messages
from tqdm import tqdm

logger = logging.getLogger(__name__)


def load_config() -> Dict:
    with open("config.json") as f:
        return json.load(f)


async def get_message_count(client, entity, latest_id=0):
    get_history = GetHistoryRequest(
        peer=entity,
        offset_id=0,
        offset_date=None,
        add_offset=0,
        limit=1,
        max_id=0,
        min_id=latest_id or 0,
        hash=0
    )
    history = await client(get_history)
    if isinstance(history, Messages):
        count = len(history.messages)
    else:
        count = history.count
    return count


def get_chat_name(entity):
    if hasattr(entity, "title"):
        return f"#{entity.title}"
    else:
        return get_user_name(entity) or str(entity.id)


def get_user_name(user):
    if hasattr(user, "title"):
        return f"#{user.title}"
    full_name = (user.first_name or "") + ("" if user.last_name is None else " " + user.last_name)
    if full_name == "":
        return "DELETED_ACCOUNT"
    return full_name.replace(" ", "_")


def encode_peer_id(peer_id: PeerUser) -> Dict:
    if isinstance(peer_id, PeerUser):
        return {
            "type": "peer_user",
            "user_id": peer_id.user_id
        }
    raise ValueError(f"Unrecognised Peer ID type: {peer_id}")


def encode_entity(entity: MessageEntityUrl) -> Dict:
    if isinstance(entity, MessageEntityUrl):
        return {
            "type": "message_entity_url",
            "length": entity.length,
            "offset": entity.offset,
        }
    raise ValueError(f"Unrecognised entity type: {entity}")


def encode_message(msg: Message) -> Dict:
    raw_fields = ["id", "button_count", "edit_hide", "from_scheduled", "is_reply", "legacy", "media_unread", "mentioned", "message", "noforwards", "out", "pinned", "post", "sender_id", "silent"]
    encode_fields = {
        "date": lambda d: d.isoformat(),
        "entities": lambda entities: [encode_entity(entity) for entity in entities],
        "peer_id": encode_peer_id,
    }
    unexpected_value = ["action", "action_entities", "audio", "buttons", "contact", "dice", "document", "edit_date", "file", "forward", "forwards", "from_id", "fwd_from", "game", "geo", "gif", "grouped_id", "invoice", "media", "photo", "poll", "post_author", "reactions", "replies", "reply_markup", "reply_to", "reply_to_msg_id", "restriction_reason", "sticker", "ttl_period", "venue", "via_bot", "via_bot_id", "via_input_bot", "video", "video_note", "views", "voice", "web_preview"]
    skip_fields = ["chat", "chat_id", "client", "input_chat", "input_sender", "raw_text", "sender", "text"]
    expected_value = {
        "is_channel": False,
        "is_group": False,
        "is_private": True,
    }
    output = {}
    # Parse raw fields, which encode as they are
    for raw_field in raw_fields:
        output[raw_field] = msg.__getattribute__(raw_field)
    # Handle encoded fields, which have encode functions
    for field_name, encode_func in encode_fields.items():
        output[field_name] = encode_func(msg.__getattribute__(field_name))
    # Handle null-only fields, where I'm not sure what they do
    for field_name in unexpected_value:
        value = msg.__getattribute__(field_name)
        if value is not None:
            logger.critical("Encountered non-null value when checking message ID %s, field %s", msg.id, field_name)
            raise ValueError(f"Expected null value, got: {value}")
    # Check if expected value fields are as expected
    for field_name, value in expected_value.items():
        actual_value = msg.__getattribute__(field_name)
        if actual_value != value:
            raise ValueError(f"Expected value {value} but got {actual_value} for field {field_name} in message ID: {msg.id}")
    # Check message text raw text, because... err? TODO
    if msg.message != msg.text or msg.text != msg.raw_text or msg.raw_text != msg.message:
        raise ValueError(f"Message ID: {msg.id}, message, text, and raw_text do not match")
    # Check if any other fields existed
    known_fields = raw_fields + list(encode_fields.keys()) + unexpected_value + skip_fields
    all_fields = [field for field in msg.__dict__.keys() if not field.startswith("_")]
    unknown_fields = set(all_fields) - set(known_fields)
    if unknown_fields:
        logger.critical("Unrecognised fields were encountered on message ID %s: %s", msg.id, unknown_fields)
        raise ValueError("Unrecognised field was encountered")
    # return data
    return output


async def backup_target(client: TelegramClient, target: Dict) -> None:
    chat_id = target["chat_id"]
    last_message_id = 0  # TODO: Store for incremental backups

    entity = await client.get_entity(chat_id)
    count = await get_message_count(client, entity, last_message_id)
    chat_name = get_chat_name(entity)
    latest_id = None
    logger.info("Backing up target chat: %s", chat_name)
    print(f"- Updating {chat_name} logs")
    with tqdm(total=count) as bar:
        async for message in client.iter_messages(entity):
            if latest_id is None:
                latest_id = message.id
            if last_message_id is not None and message.id <= last_message_id:
                logger.info(f"- Caught up on %s", chat_name)
                break
            else:
                print(json.dumps(encode_message(message)))
            bar.update(1)

    last_message_id = latest_id


def main() -> None:
    conf = load_config()
    client = TelegramClient('simple_backup', conf["client"]["api_id"], conf["client"]["api_hash"])
    client.start()
    loop = asyncio.get_event_loop()
    for target_conf in conf["backup_targets"]:
        loop.run_until_complete(backup_target(client, target_conf))
    logger.info("All backups complete")


if __name__ == '__main__':
    main()