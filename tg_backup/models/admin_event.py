import datetime
from typing import Optional

import telethon

from tg_backup.models.abstract_resource import AbstractResource
from tg_backup.utils.parse_str_repr import StrReprObj


class AdminEvent(AbstractResource):
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
        self.message_id: Optional[int] = None

    @classmethod
    def from_event(cls, evt: object) -> "AdminEvent":
        obj = cls.from_storable_object(evt)
        if hasattr(evt, "date"):
            obj.datetime = evt.date
        if hasattr(evt, "action"):
            if hasattr(evt.action, "message"):
                if hasattr(evt.action.message, "id"):
                    obj.message_id = evt.action.message.id
            if hasattr(evt.action, "prev_message"):
                if hasattr(evt.action.prev_message, "id"):
                    obj.message_id = evt.action.prev_message.id
        return obj

    @classmethod
    def from_str_repr_obj(
            cls,
            archive_datetime: datetime.datetime,
            evt_str_obj: StrReprObj,
    ) -> "AdminEvent":
        # noinspection PyUnresolvedReferences
        schema_layer = telethon.tl.alltlobjects.LAYER
        obj = AdminEvent(
            archive_datetime=archive_datetime,
            archive_tl_schema_layer=schema_layer,
            resource_id=evt_str_obj.values_dict["id"],
            resource_type=evt_str_obj.class_name,
            str_repr=evt_str_obj.to_str(),
            dict_repr=evt_str_obj.to_dict(),
        )
        if evt_str_obj.has("date"):
            obj.datetime = evt_str_obj.get("date")
        if evt_str_obj.has("action"):
            if evt_str_obj.get("action").has("message"):
                if evt_str_obj.get("action").get("message").has("id"):
                    obj.message_id = evt_str_obj.get("action").get("message").get("id")
            if evt_str_obj.get("action").has("prev_message"):
                if evt_str_obj.get("action").get("prev_message").has("id"):
                    obj.message_id = evt_str_obj.get("action").get("prev_message").get("id")
        return obj
