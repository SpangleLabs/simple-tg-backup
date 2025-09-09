import datetime
from typing import Optional

import telethon.tl.types

from tg_backup.models.abstract_resource import AbstractResource


class StickerSet(AbstractResource):
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
