import base64
import logging
from typing import Dict, Optional, Union

from telethon.tl import TLObject
from telethon.tl.custom import Message
from telethon.tl.types import PeerUser, MessageEntityUrl, MessageMediaWebPage, WebPage, Photo, PhotoSize, \
    PhotoStrippedSize, PhotoSizeProgressive, MessageReplyHeader, MessageEntityMention, WebPageEmpty, MessageReactions, \
    MessageMediaPhoto, MessageEntityTextUrl, MessageFwdHeader, PeerChannel, Document, DocumentAttributeFilename, Page, \
    PageBlockTitle

logger = logging.getLogger(__name__)


def encode_peer_id(peer_id: Union[None, PeerUser, PeerChannel]) -> Optional[Dict]:
    if peer_id is None:
        return None
    if isinstance(peer_id, PeerUser):
        return {
            "_type": "peer_user",
            "user_id": peer_id.user_id
        }
    if isinstance(peer_id, PeerChannel):
        return {
            "_type": "peer_channel",
            "channel_id": peer_id.channel_id,
        }
    raise ValueError(f"Unrecognised Peer ID type: {peer_id}")


def encode_entity(entity: Union[MessageEntityUrl, MessageEntityMention, MessageEntityTextUrl]) -> Dict:
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
    if isinstance(entity, MessageEntityTextUrl):
        return {
            "_type": "message_entity_text_url",
            "length": entity.length,
            "offset": entity.offset,
            "url": entity.url,
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


def encode_photo(photo: Optional[Photo]) -> Optional[Dict]:
    # TODO: Also return a downloadable resource object
    if photo is None:
        return None
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


def encode_document_attribute(attribute: DocumentAttributeFilename) -> Dict:
    if isinstance(attribute, DocumentAttributeFilename):
        return {
            "_type": "document_attribute_filename",
            "file_name": attribute.file_name,
        }
    raise ValueError(f"Unrecognised document attribute type: {attribute}")


def encode_document(document: Optional[Document]) -> Optional[Dict]:
    # TODO: Also return a downloadable resource object
    if document is None:
        return None
    if isinstance(document, Document):
        if document.video_thumbs is not None:
            raise ValueError("Unexpected video thumbs")
        return {
            "_type": "document",
            "id": document.id,
            "access_hash": document.access_hash,
            "file_reference": base64.b64encode(document.file_reference).decode(),
            "date": document.date.isoformat(),
            "mime_type": document.mime_type,
            "size": document.size,
            "dc_id": document.dc_id,
            "attributes": [encode_document_attribute(a) for a in document.attributes],
            "thumbs": [encode_photo_size(s) for s in document.thumbs] if document.thumbs is not None else None,
        }
    raise ValueError(f"Unrecognised document type: {document}")


def encode_page_block(block: Union[PageBlockTitle]) -> Dict:
    if isinstance(block, PageBlockTitle):
        raise ValueError("Haven't defined how to encode page blocks yet, sorry")
        return {
            "_type": "page_block_title",
            # TODO
        }
    raise ValueError(f"Unrecognised page block type: {block}")


def encode_page(page: Optional[Page]) -> Optional[Dict]:
    if page is None:
        return None
    if isinstance(page, Page):
        return {
            "_type": "page",
            "url": page.url,
            "blocks": [encode_page_block(b) for b in page.blocks],
            "photos": [encode_photo(p) for p in page.photos],
            "documents": [encode_document(d) for d in page.documents],
            "part": page.part,
            "rtl": page.rtl,
            "v2": page.v2,
            "views": page.views,
        }



def encode_webpage(webpage: Union[WebPage, WebPageEmpty]) -> Dict:
    if isinstance(webpage, WebPage):
        if webpage.cached_page is not None:
            raise ValueError(f"Webpage cached_page unexpected: {webpage.cached_page}")
        if webpage.attributes is not None:
            raise ValueError(f"Webpage attributes unexpected: {webpage.attributes}")
        return {
            "_type": "web_page",
            "id": webpage.id,
            "url": webpage.url,
            "display_url": webpage.display_url,
            "hash": webpage.hash,
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
            "document": encode_document(webpage.document),
        }
    if isinstance(webpage, WebPageEmpty):
        return {
            "_type": "web_page_empty",
            "id": webpage.id,
        }
    raise ValueError(f"Unrecognised webpage type: {webpage}")


def encode_media(media: Union[None, MessageMediaWebPage, MessageMediaPhoto]) -> Optional[Dict]:
    if media is None:
        return None
    if isinstance(media, MessageMediaWebPage):
        return {
            "_type": "message_media_web_page",
            "webpage": encode_webpage(media.webpage),
        }
    if isinstance(media, MessageMediaPhoto):
        return {
            "_type": "message_media_photo",
            "spoiler": media.spoiler,
            "photo": encode_photo(media.photo),
            "ttl_seconds": media.ttl_seconds,
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


def encode_message_fwd_header(header: Optional[MessageFwdHeader]) -> Optional[Dict]:
    if header is None:
        return None
    if isinstance(header, MessageFwdHeader):
        if header.from_id != header.saved_from_peer:
            raise ValueError("Expected from ID and saved from peer to match")
        if header.psa_type is not None:
            raise ValueError("Didn't expect to see psa_type set")
        return {
            "_type": "message_fwd_header",
            "date": header.date.isoformat(),
            "imported": header.imported,
            "from_id": encode_peer_id(header.from_id),
            "from_name": header.from_name,
            "channel_post": header.channel_post,
            "post_author": header.post_author,
            "saved_from_peer": encode_peer_id(header.saved_from_peer),
            "saved_from_msg_id": header.saved_from_msg_id,
            "psa_type": header.psa_type,
        }
    raise ValueError(f"Unrecognised message forward header type: {header}")


def encode_reactions(reactions: Optional[MessageReactions]) -> Optional[Dict]:
    if reactions is None:
        return None
    if isinstance(reactions, MessageReactions):
        if reactions.results:
            raise ValueError(f"Didn't expect actual results: {reactions.results}")
        if reactions.recent_reactions is not None:
            raise ValueError(f"Didn't expect to see recent reactions: {reactions.recent_reactions}")
        return {
            "_type": "message_reactions",
            "results": reactions.results,
            "min": reactions.min,
            "can_see_list": reactions.can_see_list,
            "recent_reactions": reactions.recent_reactions,
        }
    raise ValueError(f"Unrecognised reactions type: {reactions}")


def encode_tl_object(obj: Optional[TLObject]) -> Optional[Dict]:
    if obj is None:
        return None
    obj.to_json()
    return obj.to_dict()


def encode_message(msg: Message) -> Dict:
    raw_fields = ["id", "button_count", "date", "edit_date", "edit_hide", "from_scheduled", "grouped_id", "is_reply", "legacy", "media_unread", "mentioned", "message", "noforwards", "out", "pinned", "post", "sender_id", "silent", "views", "forwards"]
    encode_fields = {
        "entities": lambda entities: None if entities is None else [encode_tl_object(entity) for entity in entities],
        "peer_id": encode_tl_object,
        "media": encode_tl_object,
        "reply_to": encode_tl_object,
        "reactions": encode_tl_object,
        "fwd_from": encode_tl_object,
    }
    unexpected_value = ["action", "action_entities", "audio", "buttons", "contact", "dice", "from_id", "game", "geo", "invoice", "poll", "post_author", "replies", "reply_markup", "restriction_reason", "ttl_period", "venue", "via_bot", "via_bot_id", "via_input_bot", "video_note", "voice"]
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
        "forward",  # covered by fwd_from
        "document",  # covered by media.document or media.webpage.document
        "video",  # covered by media.document or media.webpage.document
        "gif",  # covered by media.document or media.webpage.document
        "sticker",  # covered by media.document
    ]
    expected_value = {
        "is_channel": False,
        "is_group": False,
        "is_private": True,
    }
    known_fields = raw_fields + list(encode_fields.keys()) + unexpected_value + skip_fields + list(expected_value.keys())
    # Pre-game check
    _seen_fields = set()
    duplicate_fields = [x for x in known_fields if x in _seen_fields or _seen_fields.add(x)]
    if duplicate_fields:
        raise ValueError(f"Duplicate fields in list: {duplicate_fields}")
    # Start building output
    output = {}
    # Parse raw fields, which encode as they are
    for raw_field in raw_fields:
        output[raw_field] = msg.__getattribute__(raw_field)
    # Handle encoded fields, which have encode functions
    for field_name, encode_func in encode_fields.items():
        output[field_name] = encode_func(msg.__getattribute__(field_name))
    # Handle null-only fields, where I'm not sure what they do
    unexpected_field_values = {}
    for field_name in unexpected_value:
        value = msg.__getattribute__(field_name)
        if value is not None:
            unexpected_field_values[field_name] = value
    if unexpected_field_values:
        logger.critical("Encountered non-null values when checking message ID %s, fields: %s", msg.id, unexpected_field_values)
        raise ValueError(f"Expected null value for {unexpected_field_values.keys()}, got: {unexpected_field_values}")
    # Check if expected value fields are as expected
    for field_name, value in expected_value.items():
        actual_value = msg.__getattribute__(field_name)
        if actual_value != value:
            raise ValueError(f"Expected value {value} but got {actual_value} for field {field_name} in message ID: {msg.id}")
    # Check if any other fields existed
    all_fields = [field for field in msg.__dict__.keys() if not field.startswith("_")]
    unknown_fields = set(all_fields) - set(known_fields)
    if unknown_fields:
        logger.critical("Unrecognised fields were encountered on message ID %s: %s", msg.id, unknown_fields)
        raise ValueError("Unrecognised field was encountered")
    # return data
    return output
