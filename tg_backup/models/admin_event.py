import datetime
from typing import Optional

from tg_backup.models.abstract_resource import AbstractResource


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
