import datetime
from typing import Optional

from telethon.tl.types import Document, DocumentAttributeFilename, DocumentAttributeSticker

from tg_backup.models.abstract_resource import AbstractResource


class Sticker(AbstractResource):
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
        self.sticker_set_id: Optional[int] = None
        self.emoji: Optional[str] = None
        self.file_name: Optional[str] = None
        self.sticker_upload_date: Optional[datetime.datetime] = None

    @classmethod
    def from_sticker(cls, sticker: Document) -> "Sticker":
        sticker_obj = cls.from_storable_object(sticker)
        if hasattr(sticker, "date"):
            sticker_obj.date = sticker.date
        if hasattr(sticker, "attributes") and sticker.attributes:
            for attr in sticker.attributes:
                if isinstance(attr, DocumentAttributeFilename):
                    sticker_obj.file_name = attr.file_name
                if isinstance(attr, DocumentAttributeSticker):
                    sticker_obj.emoji = attr.alt
                    if hasattr(attr.stickerset, "id"):
                        sticker_obj.sticker_set_id = attr.stickerset.id
        return sticker_obj
