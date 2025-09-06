import datetime
from typing import Optional

from telethon import hints

from scripts.models.abstract_resource import AbstractResource


class Chat(AbstractResource):
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
        self.title: Optional[str] = None

    @classmethod
    def from_chat_entity(cls, obj: hints.Entity) -> "Chat":
        chat = cls.from_storable_object(obj)
        if hasattr(chat, "title"):
            chat.title = chat.title
        return chat
