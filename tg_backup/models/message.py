import datetime
from typing import Optional

import telethon
from telethon.tl.types import DocumentAttributeSticker

from tg_backup.models.abstract_resource import DeduplicatableAbstractResource
from tg_backup.utils.parse_str_repr import StrReprObj


class Message(DeduplicatableAbstractResource):
    def __init__(
            self,
            archive_datetime: datetime.datetime,
            archive_tl_schema_layer: int,
            resource_id: int,
            resource_type: str,
            str_repr: str,
            dict_repr: Optional[dict],
    ) -> None:
        super().__init__(archive_datetime, archive_tl_schema_layer, resource_id, resource_type, str_repr, dict_repr)
        self.datetime: Optional[datetime.datetime] = None
        self.text: Optional[str] = None
        self.media_id: Optional[int] = None
        self.user_id: Optional[int] = None
        self.sticker_id: Optional[int] = None
        self.sticker_set_id: Optional[int] = None
        self.deleted: bool = False
        self.edit_datetime: Optional[datetime.datetime] = None
        self.web_page_id: Optional[int] = None

    @classmethod
    def from_msg(cls, msg: telethon.types.Message, deleted: bool = False) -> "Message":
        obj = cls.from_storable_object(msg)
        if hasattr(msg, "date"):
            obj.datetime = msg.date
        if hasattr(msg, "message"):
            obj.text = msg.message
        # Handle stickers
        if hasattr(msg, "sticker"):
            if msg.sticker is not None:
                if hasattr(msg.sticker, "id"):
                    obj.sticker_id = msg.sticker.id
                if hasattr(msg.sticker, "attributes"):
                    for attr in msg.sticker.attributes:
                        if isinstance(attr, DocumentAttributeSticker):
                            if hasattr(attr, "stickerset"):
                                if hasattr(attr.stickerset, "id"):
                                    obj.sticker_set_id = attr.stickerset.id
        # Handle non-sticker media
        if hasattr(msg, "media") and obj.sticker_id is None:
            if hasattr(msg.media, "photo"):
                if hasattr(msg.media.photo, "id"):
                    obj.media_id = msg.media.photo.id
            if hasattr(msg.media, "document"):
                if hasattr(msg.media.document, "id"):
                    obj.media_id = msg.media.document.id
            # Handle web pages
            if hasattr(msg.media, "webpage"):
                if hasattr(msg.media.webpage, "id"):
                    obj.web_page_id = msg.media.webpage.id
        # Handle users
        if hasattr(msg, "from_id"):
            if hasattr(msg.from_id, "user_id"):
                obj.user_id = msg.from_id.user_id
        # Handle whether it was deleted or edited
        obj.deleted = deleted
        if hasattr(msg, "edit_date"):
            obj.edit_datetime = msg.edit_date
        return obj

    def mark_deleted(self) -> "Message":
        self.deleted = True
        self.archive_datetime = datetime.datetime.now(datetime.timezone.utc)
        return self

    def no_useful_difference(self, other: "Message") -> bool:
        return all([
            self.resource_id == other.resource_id,
            self.deleted == other.deleted,
            self.edit_datetime == other.edit_datetime,
            # self.str_repr == other.str_repr, # Can't check the string reference, as file references and pts values will change
            self.text == other.text,
            self.media_id == other.media_id,
            self.sticker_id == other.sticker_id,
            self.web_page_id == other.web_page_id,
            self.archive_tl_schema_layer == other.archive_tl_schema_layer,
        ])

    def sort_key_for_copies_of_resource(self) -> tuple[bool, bool, datetime.datetime, int, datetime.datetime]:
        """
        This method returns a tuple which can be used as a sort key, for multiple saved copies of a message, to
        understand the timeline of the individual message.
        It should not be used to compare different messages.
        """
        return (
            not self.deleted, # Deleted is newer than undeleted
            self.edit_datetime is not None, # Unedited is before edited
            self.edit_datetime, # Sort by edit time
            self.archive_tl_schema_layer, # Sort by schema layer
            self.archive_datetime, # Sort by archive time, though probably they're identical objects
        )

    @classmethod
    def from_str_repr_obj(
            cls,
            archive_datetime: datetime.datetime,
            msg_str_obj: StrReprObj,
            deleted: bool = False,
    ) -> "Message":
        # noinspection PyUnresolvedReferences
        schema_layer = telethon.tl.alltlobjects.LAYER
        obj = Message(
            archive_datetime=archive_datetime,
            archive_tl_schema_layer=schema_layer,
            resource_id=msg_str_obj.values_dict["id"],
            resource_type=msg_str_obj.class_name,
            str_repr=msg_str_obj.to_str(),
            dict_repr=msg_str_obj.to_dict(),
        )
        if msg_str_obj.has("date"):
            obj.datetime = msg_str_obj.get("date")
        if msg_str_obj.has("message"):
            obj.text = msg_str_obj.get("message")
        # Handle stickers
        if msg_str_obj.has("sticker") and msg_str_obj.get("sticker") is not None:
            if msg_str_obj.get("sticker").has("id"):
                obj.sticker_id = msg_str_obj.get("sticker").get("id")
            if msg_str_obj.get("sticker").has("attributes"):
                for attr in msg_str_obj.get("sticker").get("attributes"):
                    if attr.class_name == "DocumentAttributeSticker":
                        if attr.has("stickerset"):
                            if attr.get("stickerset").has("id"):
                                obj.sticker_set_id = attr.get("stickerset").get("id")
        # Handle non-sticker media
        if msg_str_obj.has("media") and msg_str_obj.get("media") is not None and obj.sticker_id is None:
            if msg_str_obj.get("media").has("photo") and msg_str_obj.get("media").get("photo") is not None:
                if msg_str_obj.get("media").get("photo").has("id"):
                    obj.media_id = msg_str_obj.get("media").get("photo").get("id")
            if msg_str_obj.get("media").has("document"):
                if msg_str_obj.get("media").get("document").has("id"):
                    obj.media_id = msg_str_obj.get("media").get("document").get("id")
            if msg_str_obj.get("media").has("webpage"):
                if msg_str_obj.get("media").get("webpage").has("id"):
                    obj.webpage_id = msg_str_obj.get("media").get("webpage").get("id")
        # Handle users
        if msg_str_obj.has("from_id") and msg_str_obj.get("from_id") is not None:
            if msg_str_obj.get("from_id").has("user_id"):
                obj.user_id = msg_str_obj.get("from_id").get("user_id")
        # Handle whether it was deleted or edited
        obj.deleted = deleted
        if msg_str_obj.has("edit_date"):
            obj.edit_datetime = msg_str_obj.get("edit_date")
        return obj
