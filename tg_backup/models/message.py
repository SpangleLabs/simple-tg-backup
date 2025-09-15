import datetime
from typing import Optional

import telethon
from telethon.tl.types import DocumentAttributeSticker

from tg_backup.models.abstract_resource import AbstractResource


class Message(AbstractResource):
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

    def refers_to_same_msg(self, other: "Message") -> bool:
        return self.resource_id == other.resource_id

    def no_useful_difference(self, other: "Message") -> bool:
        return all([
            self.resource_id == other.resource_id,
            self.deleted == other.deleted,
            self.edit_datetime == other.edit_datetime,
            self.str_repr == other.str_repr,
            self.archive_tl_schema_layer == other.archive_tl_schema_layer,
        ])

    def sort_key_for_copies_of_message(self) -> tuple[bool, bool, datetime.datetime, int, datetime.datetime]:
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
    def all_refer_to_same_message(cls, messages: list["Message"]) -> bool:
        if len(messages) == 0:
            raise ValueError("There are no messages, so they cannot all refer to the same event")
        if len(messages) == 1:
            return True
        first = messages[0]
        for msg in messages[1:]:
            if not first.refers_to_same_msg(msg):
                return False
        return True

    @classmethod
    def latest_copy_of_message(cls, messages: list["Message"]) -> Optional["Message"]:
        if len(messages) == 0:
            return None
        if not cls.all_refer_to_same_message(messages):
            raise ValueError("These events do not all refer to the same message")
        sorted_messages = sorted(messages, key=lambda m: m.sort_key_for_copies_of_message())
        return sorted_messages[-1]
