import datetime
from typing import Optional

import telethon.tl.custom.dialog
import telethon.tl.types

from tg_backup.models.abstract_resource import AbstractResource
from tg_backup.utils.chat_matcher import ChatData
from tg_backup.utils.dialog_type import DialogType


class Dialog(AbstractResource):
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
        self.chat_type: DialogType = DialogType.UNKNOWN
        self.name: Optional[str] = None
        self.pinned: Optional[bool] = None
        self.archived_chat: Optional[bool] = None
        self.last_msg_date: Optional[datetime.datetime] = None
        self.first_seen: Optional[datetime.datetime] = None
        self.last_seen: Optional[datetime.datetime] = None
        self.needs_takeout: Optional[bool] = None
        self.used_takeout: Optional[bool] = None

    def chat_data(self) -> ChatData:
        return ChatData(
            chat_id=self.resource_id,
            chat_type=self.chat_type,
            title=self.name,
        )

    def last_seen_msg_age(self) -> Optional[datetime.timedelta]:
        # When this dialog was last seen, how old was the last message there?
        if self.last_seen is None:
            return None
        if self.last_msg_date is None:
            return None
        return self.last_seen - self.last_msg_date

    @classmethod
    def from_dialog(cls, dialog: telethon.tl.custom.dialog.Dialog) -> "Dialog":
        obj = cls.from_storable_object(dialog)
        # Set dialog type as user or group by the bools
        if hasattr(dialog, "is_user") and dialog.is_user:
            obj.chat_type = DialogType.USER
        if hasattr(dialog, "is_group") and dialog.is_group:
            obj.chat_type = DialogType.GROUP
        # Set dialog type more accurately by the entity type
        if hasattr(dialog, "entity"):
            if isinstance(dialog.entity, telethon.tl.types.Channel):
                if dialog.entity.broadcast:
                    obj.chat_type = DialogType.CHANNEL
                else:
                    obj.chat_type = DialogType.LARGE_GROUP
            elif isinstance(dialog.entity, telethon.tl.types.User):
                obj.chat_type = DialogType.USER
            elif isinstance(dialog.entity, (telethon.tl.types.Chat, telethon.tl.types.ChatForbidden)):
                obj.chat_type = DialogType.SMALL_GROUP
        # Set other dialog attributes
        if hasattr(dialog, "name"):
            obj.name = dialog.name
        if hasattr(dialog, "pinned"):
            obj.pinned = dialog.pinned
        if hasattr(dialog, "archived"):
            obj.archived_chat = dialog.archived
        if hasattr(dialog, "date"):
            obj.last_msg_date = dialog.date
        obj.first_seen = datetime.datetime.now(datetime.timezone.utc)
        obj.last_seen = datetime.datetime.now(datetime.timezone.utc)
        # Not storing these:
        # dialog.draft (The currently drafted message)
        # dialog.unread_count (Current unread message count)
        # dialog.unread_mentions_count (Count of unread mentions)
        return obj
