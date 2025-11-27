import datetime
from typing import Optional, Self, Any

from telethon.tl.types import Document, DocumentAttributeFilename, DocumentAttributeSticker

from tg_backup.models.abstract_resource import DeduplicatableAbstractResource


class Sticker(DeduplicatableAbstractResource):

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

    def refers_to_same_resource(self, other: Self) -> bool:
        return all([
            self.resource_id == other.resource_id,
            self.sticker_set_id == other.sticker_set_id,
        ])

    def sort_key_for_copies_of_resource(self) -> Any:
        return (
            self.archive_tl_schema_layer, # Sort by schema layer
            self.archive_datetime, # Sort by archive time
        )

    def no_useful_difference(self, other: Self) -> bool:
        return all([
            self.resource_id == other.resource_id,
            self.sticker_set_id == other.sticker_set_id,
            self.emoji == other.emoji,
            self.file_name == other.file_name,
            self.sticker_upload_date == other.sticker_upload_date,
            self.archive_tl_schema_layer == other.archive_tl_schema_layer,
        ])
