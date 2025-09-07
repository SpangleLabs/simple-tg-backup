import dataclasses
from abc import ABC
import datetime
from typing import Optional, TypeVar

import telethon

Resource = TypeVar("Resource", bound="AbstractResource")

@dataclasses.dataclass
class AbstractResource(ABC):
    def __init__(
            self,
            archive_datetime: datetime.datetime,
            archive_tl_schema_layer: int,
            resource_id: int,
            resource_type: str,
            str_repr: str,
            dict_repr: Optional[dict],
    ) -> None:
        self.archive_datetime = archive_datetime
        self.archive_tl_schema_layer = archive_tl_schema_layer
        self.resource_id = resource_id
        self.resource_type = resource_type
        self.str_repr = str_repr
        self.dict_repr = dict_repr

    @classmethod
    def from_storable_object(cls: type[Resource], obj: object) -> Resource:
        # noinspection PyUnresolvedReferences
        scheme_layer = telethon.tl.alltlobjects.LAYER
        return cls(
            archive_datetime=datetime.datetime.now(datetime.timezone.utc),
            archive_tl_schema_layer=scheme_layer,
            resource_id=obj.id if hasattr(obj, 'id') else None,
            resource_type=type(obj).__name__,
            str_repr=str(obj),
            dict_repr=obj.to_dict() if hasattr(obj, 'to_dict') else None,
        )