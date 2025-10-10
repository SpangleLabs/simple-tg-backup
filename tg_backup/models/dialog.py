import datetime
from typing import Optional

import telethon.tl.custom.dialog

from tg_backup.models.abstract_resource import AbstractResource
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

    @classmethod
    def from_dialog(cls, dialog: telethon.tl.custom.dialog.Dialog) -> "Dialog":
        obj = cls.from_storable_object(dialog)
        if hasattr(dialog, "is_user"):
            obj.chat_type = DialogType.USER if dialog.is_user else DialogType.GROUP
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
