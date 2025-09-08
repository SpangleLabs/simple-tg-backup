import datetime
from typing import Optional

import telethon

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
        self.deleted: bool = False
        self.edit_datetime: Optional[datetime.datetime] = None

    @classmethod
    def from_msg(cls, msg: telethon.types.Message, deleted: bool = False) -> "Message":
        obj = cls.from_storable_object(msg)
        if hasattr(msg, "date"):
            obj.datetime = msg.date
        if hasattr(msg, "message"):
            obj.text = msg.message
        if hasattr(msg, "media"):
            if hasattr(msg.media, "photo"):
                if hasattr(msg.media.photo, "id"):
                    obj.media_id = msg.media.photo.id
            if hasattr(msg.media, "document"):
                if hasattr(msg.media.document, "id"):
                    obj.media_id = msg.media.document.id
        if hasattr(msg, "from_id"):
            if hasattr(msg.from_id, "user_id"):
                obj.user_id = msg.from_id.user_id
        obj.deleted = deleted
        if hasattr(msg, "edit_date"):
            obj.edit_datetime = msg.edit_date
        return obj
