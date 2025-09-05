from telethon import hints

from scripts.models.abstract_resource import AbstractResource


class Chat(AbstractResource):

    @classmethod
    def from_chat_entity(cls, obj: hints.Entity) -> "Chat":
        return cls.from_storable_object(obj)
