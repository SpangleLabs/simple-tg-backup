import dataclasses
from abc import ABC
import datetime
from typing import Optional


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
