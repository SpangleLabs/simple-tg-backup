import logging
from typing import Dict

from telethon.tl.custom import Message
from telethon.tl.types import PeerUser, MessageEntityUrl, MessageMediaWebPage, WebPage

logger = logging.getLogger(__name__)


def encode_peer_id(peer_id: PeerUser) -> Dict:
    if isinstance(peer_id, PeerUser):
        return {
            "_type": "peer_user",
            "user_id": peer_id.user_id
        }
    raise ValueError(f"Unrecognised Peer ID type: {peer_id}")


def encode_entity(entity: MessageEntityUrl) -> Dict:
    if isinstance(entity, MessageEntityUrl):
        return {
            "_type": "message_entity_url",
            "length": entity.length,
            "offset": entity.offset,
        }
    raise ValueError(f"Unrecognised entity type: {entity}")


def encode_webpage(webpage: WebPage) -> Dict:
    if isinstance(webpage, WebPage):
        if webpage.hash != 0:
            raise ValueError(f"Webpage hash unexpected: {webpage.hash}")
        if webpage.cached_page is not None:
            raise ValueError(f"Webpage cached_page unexpected: {webpage.cached_page}")
        if webpage.attributes is not None:
            raise ValueError(f"Webpage attributes unexpected: {webpage.attributes}")
        if webpage.photo is not None:
            raise ValueError(f"Webpage photo unexpected: {webpage.photo}")
        if webpage.document is not None:
            raise ValueError(f"Webpage document unexpected: {webpage.document}")
        return {
            "_type": "web_page",
            "id": webpage.id,
            "url": webpage.url,
            "display_url": webpage.display_url,
            "type": webpage.type,
            "site_name": webpage.site_name,
            "title": webpage.title,
            "description": webpage.description,
            "embed_url": webpage.embed_url,
            "embed_type": webpage.embed_type,
            "embed_width": webpage.embed_width,
            "embed_height": webpage.embed_height,
            "duration": webpage.duration,
            "author": webpage.author,
        }
    raise ValueError(f"Unrecognised webpage type: {webpage}")


def encode_media(media: MessageMediaWebPage) -> Dict:
    if isinstance(media, MessageMediaWebPage):
        return {
            "_type": "message_media_web_page",
            "webpage": encode_webpage(media.webpage),
        }
    raise ValueError(f"Unrecognised media type: {media}")


def encode_message(msg: Message) -> Dict:
    raw_fields = ["id", "button_count", "edit_hide", "from_scheduled", "is_reply", "legacy", "media_unread", "mentioned", "message", "noforwards", "out", "pinned", "post", "sender_id", "silent"]
    encode_fields = {
        "date": lambda d: d.isoformat(),
        "entities": lambda entities: [encode_entity(entity) for entity in entities],
        "peer_id": encode_peer_id,
        "media": encode_media,
    }
    unexpected_value = ["action", "action_entities", "audio", "buttons", "contact", "dice", "document", "edit_date", "forward", "forwards", "from_id", "fwd_from", "game", "geo", "gif", "grouped_id", "invoice", "photo", "poll", "post_author", "reactions", "replies", "reply_markup", "reply_to", "reply_to_msg_id", "restriction_reason", "sticker", "ttl_period", "venue", "via_bot", "via_bot_id", "via_input_bot", "video", "video_note", "views", "voice", "web_preview"]
    skip_fields = [
        "chat",  # backing up a chat, so this is the same for every message
        "chat_id",  # backing up a chat, so this is the same for every message
        "input_chat",  # backing up a chat, so this is the same for every message
        "client",  # can't backup the telegram client
        "input_sender",  # peer_id covers this
        "sender",  # peer_id covers this
        "raw_text",  # covered by message
        "text",  # covered by message
        "file",  # covered by media
    ]
    expected_value = {
        "is_channel": False,
        "is_group": False,
        "is_private": True,
    }
    known_fields = raw_fields + list(encode_fields.keys()) + unexpected_value + skip_fields + list(expected_value.keys())
    # Pre-game check
    if len(set(known_fields)) != len(known_fields):
        raise ValueError("Duplicate fields in list!")
    # Start building output
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
            raise ValueError(f"Expected null value for {field_name}, got: {value}")
    # Check if expected value fields are as expected
    for field_name, value in expected_value.items():
        actual_value = msg.__getattribute__(field_name)
        if actual_value != value:
            raise ValueError(f"Expected value {value} but got {actual_value} for field {field_name} in message ID: {msg.id}")
    # Check message text raw text, because... err? TODO
    if msg.message != msg.text or msg.text != msg.raw_text or msg.raw_text != msg.message:
        raise ValueError(f"Message ID: {msg.id}, message, text, and raw_text do not match")
    # Check if any other fields existed
    all_fields = [field for field in msg.__dict__.keys() if not field.startswith("_")]
    unknown_fields = set(all_fields) - set(known_fields)
    if unknown_fields:
        logger.critical("Unrecognised fields were encountered on message ID %s: %s", msg.id, unknown_fields)
        raise ValueError("Unrecognised field was encountered")
    # return data
    return output
