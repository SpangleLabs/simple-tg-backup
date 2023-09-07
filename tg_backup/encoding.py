import base64
import logging
from typing import Dict, Optional, Union

from telethon.tl.custom import Message
from telethon.tl.types import PeerUser, MessageEntityUrl, MessageMediaWebPage, WebPage, Photo, PhotoSize, \
    PhotoStrippedSize, PhotoSizeProgressive, MessageReplyHeader, MessageEntityMention, WebPageEmpty

logger = logging.getLogger(__name__)


def encode_peer_id(peer_id: Optional[PeerUser]) -> Optional[Dict]:
    if peer_id is None:
        return None
    if isinstance(peer_id, PeerUser):
        return {
            "_type": "peer_user",
            "user_id": peer_id.user_id
        }
    raise ValueError(f"Unrecognised Peer ID type: {peer_id}")


def encode_entity(entity: Union[MessageEntityUrl, MessageEntityMention]) -> Dict:
    if isinstance(entity, MessageEntityUrl):
        return {
            "_type": "message_entity_url",
            "length": entity.length,
            "offset": entity.offset,
        }
    if isinstance(entity, MessageEntityMention):
        return {
            "_type": "message_entity_mention",
            "length": entity.length,
            "offset": entity.offset,
        }
    raise ValueError(f"Unrecognised entity type: {entity}")


def encode_photo_size(photo_size: Union[PhotoSize, PhotoStrippedSize, PhotoSizeProgressive]) -> Dict:
    if isinstance(photo_size, PhotoSize):
        return {
            "_type": "photo_size",
            "type": photo_size.type,
            "w": photo_size.w,
            "h": photo_size.h,
            "size": photo_size.size,
        }
    if isinstance(photo_size, PhotoStrippedSize):
        return {
            "_type": "photo_stripped_size",
            "type": photo_size.type,
            "bytes": base64.b64encode(photo_size.bytes).decode(),
        }
    if isinstance(photo_size, PhotoSizeProgressive):
        return {
            "_type": "photo_size_progressive",
            "type": photo_size.type,
            "w": photo_size.w,
            "h": photo_size.h,
            "sizes": photo_size.sizes,
        }
    raise ValueError(f"Unrecognised photo size type: {photo_size}")


def encode_photo(photo: Photo) -> Dict:
    # TODO: Also return a downloadable resource object
    if isinstance(photo, Photo):
        if photo.video_sizes is not None:
            raise ValueError(f"Photo has unexpected video sizes: {photo.video_sizes}")
        return {
            "_type": "photo",
            "id": photo.id,
            "access_hash": photo.access_hash,
            "file_reference": base64.b64encode(photo.file_reference).decode(),
            "date": photo.date.isoformat(),
            "sizes": [encode_photo_size(s) for s in photo.sizes],
            "dc_id": photo.dc_id,
            "has_stickers": photo.has_stickers,
        }
    raise ValueError(f"Unrecognised photo type: {photo}")


def encode_webpage(webpage: Union[WebPage, WebPageEmpty]) -> Dict:
    if isinstance(webpage, WebPage):
        if webpage.hash != 0:
            raise ValueError(f"Webpage hash unexpected: {webpage.hash}")
        if webpage.cached_page is not None:
            raise ValueError(f"Webpage cached_page unexpected: {webpage.cached_page}")
        if webpage.attributes is not None:
            raise ValueError(f"Webpage attributes unexpected: {webpage.attributes}")
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
            "photo": encode_photo(webpage.photo),
        }
    if isinstance(webpage, WebPageEmpty):
        return {
            "_type": "web_page_empty",
            "id": webpage.id,
        }
    raise ValueError(f"Unrecognised webpage type: {webpage}")


def encode_media(media: Optional[MessageMediaWebPage]) -> Optional[Dict]:
    if media is None:
        return None
    if isinstance(media, MessageMediaWebPage):
        return {
            "_type": "message_media_web_page",
            "webpage": encode_webpage(media.webpage),
        }
    raise ValueError(f"Unrecognised media type: {media}")


def encode_message_reply_header(header: Optional[MessageReplyHeader]) -> Optional[Dict]:
    if header is None:
        return None
    if isinstance(header, MessageReplyHeader):
        return {
            "_type": "message_reply_header",
            "reply_to_msg_id": header.reply_to_msg_id,
            "reply_to_scheduled": header.reply_to_scheduled,
            "forum_topic": header.forum_topic,
            "reply_to_peer_id": encode_peer_id(header.reply_to_peer_id),
            "reply_to_top_id": header.reply_to_top_id,
        }
    raise ValueError(f"Unrecognised message reply header type: {header}")


def encode_message(msg: Message) -> Dict:
    raw_fields = ["id", "button_count", "edit_hide", "from_scheduled", "is_reply", "legacy", "media_unread", "mentioned", "message", "noforwards", "out", "pinned", "post", "sender_id", "silent"]
    encode_fields = {
        "date": lambda d: d.isoformat(),
        "entities": lambda entities: [encode_entity(entity) for entity in entities] if entities is not None else None,
        "peer_id": encode_peer_id,
        "media": encode_media,
        "edit_date": lambda d: d.isoformat() if d is not None else None,
        "reply_to": encode_message_reply_header,
    }
    unexpected_value = ["action", "action_entities", "audio", "buttons", "contact", "dice", "document", "forward", "forwards", "from_id", "fwd_from", "game", "geo", "gif", "grouped_id", "invoice", "poll", "post_author", "reactions", "replies", "reply_markup", "restriction_reason", "sticker", "ttl_period", "venue", "via_bot", "via_bot_id", "via_input_bot", "video", "video_note", "views", "voice"]
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
        "photo",  # covered by media.photo or media.webpage.photo
        "web_preview",  # covered by media.webpage
        "reply_to_msg_id",  # covered by reply_to.reply_to_msg_id
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
