import datetime
from typing import Optional, Self

import telethon.tl.types

from tg_backup.models.abstract_resource import DeduplicatableAbstractResource


class StickerSet(DeduplicatableAbstractResource):
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
        self.handle: Optional[str] = None
        self.title: Optional[str] = None
        self.sticker_count: Optional[int] = None

    @classmethod
    def from_sticker_set(cls, sticker_set: telethon.tl.types.messages.StickerSet) -> "StickerSet":
        set_obj = cls.from_storable_object(sticker_set)
        if hasattr(sticker_set, "set"):
            if hasattr(sticker_set.set, "id"):
                set_obj.resource_id = sticker_set.set.id
            if hasattr(sticker_set.set, "short_name"):
                set_obj.handle = sticker_set.set.short_name
            if hasattr(sticker_set.set, "title"):
                set_obj.title = sticker_set.set.title
            if hasattr(sticker_set.set, "count"):
                set_obj.sticker_count = sticker_set.set.count
        return set_obj

    def sort_key_for_copies_of_resource(self) -> tuple[int, datetime.datetime]:
        return (
            self.archive_tl_schema_layer, # Sort by schema layer
            self.archive_datetime, # Sort by archive time
        )

    def no_useful_difference(self, other: Self) -> bool:
        return all([
            self.resource_id == other.resource_id,
            self.handle == other.handle,
            self.title == other.title,
            self.sticker_count == other.sticker_count,
            self.archive_tl_schema_layer == other.archive_tl_schema_layer,
        ])
