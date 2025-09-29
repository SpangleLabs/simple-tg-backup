import datetime
from typing import Optional

import telethon
from telethon import hints
from telethon.tl.types.messages import ChatFull

from tg_backup.models.abstract_resource import AbstractResource


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
        self.creation_date: Optional[datetime.datetime] = None
        self.is_creator: Optional[bool] = None
        self.have_left: Optional[bool] = None
        self.broadcast_channel: Optional[bool] = None
        self.participants_count: Optional[int] = None
        self.about: Optional[str] = None
        self.username: Optional[str] = None
        self.other_usernames: Optional[list[str]] = None
        self.migrated_to_chat_id: Optional[int] = None
        self.migrated_from_chat_id: Optional[int] = None
        self.linked_chat_id: Optional[int] = None

    @classmethod
    def from_chat_entity(cls, obj: hints.Entity) -> "Chat":
        chat = cls.from_storable_object(obj)
        if hasattr(obj, "title"):
            chat.title = obj.title
        return chat

    @classmethod
    def from_full_chat(cls, full: ChatFull) -> "Chat":
        # Construct the storable chat object
        chat_obj = cls.from_storable_object(full)
        # Dissect the object into the two parts
        full_chat = full.full_chat if hasattr(full, "full_chat") else None
        chat = full.chats[0] if hasattr(full, "chats") and len(full.chats) > 0 else None
        # Set the chat ID properly
        if chat_obj.resource_id is None and full_chat is not None:
            chat_obj.resource_id = full_chat.id
        if chat_obj.resource_id is None and chat is not None:
            chat_obj.resource_id = chat.id
        # Parse everything from the messages.ChatFull object
        if hasattr(full_chat, "about"):
            chat_obj.about = full_chat.about
        if hasattr(full_chat, "migrated_from_chat_id"):
            chat_obj.migrated_from_chat_id = full_chat.migrated_from_chat_id
        if hasattr(full_chat, "linked_chat_id"):
            chat_obj.linked_chat_id = full_chat.linked_chat_id
        # Parse everything from the Chat object
        if hasattr(chat, "title"):
            chat_obj.title = chat.title
        if hasattr(chat, "date"):
            chat_obj.creation_date = chat.date
        if hasattr(chat, "creator"):
            chat_obj.is_creator = chat.creator
        if hasattr(chat, "left"):
            chat_obj.have_left = chat.left
        if isinstance(chat, telethon.tl.types.Chat):
            chat_obj.broadcast_channel = False
        elif hasattr(chat, "broadcast_channel"):
            chat_obj.broadcast_channel = chat.broadcast_channel
        if hasattr(chat, "participants_count"):
            chat_obj.participants_count = chat.participants_count
        if hasattr(chat, "username"):
            chat_obj.username = chat.username
        if hasattr(chat, "usernames"):
            # TODO: this will probably fail tbh
            chat_obj.usernames = chat.usernames
        if hasattr(chat, "migrated_to"):
            chat_obj.migrated_to_chat_id = chat.migrated_to.channel_id
        # TODO: profile photos?
        return chat_obj
