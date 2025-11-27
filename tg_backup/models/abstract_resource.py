import dataclasses
from abc import ABC, abstractmethod
import datetime
from typing import Optional, TypeVar, Self, Any

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


class DeduplicatableAbstractResource(AbstractResource):

    @abstractmethod
    def sort_key_for_copies_of_resource(self) -> Any:
        """
        This method returns a tuple which can be used as a sort key, for multiple saved copies of a resource, to
        understand the timeline of the individual resource.
        It should not be used to compare different resources.
        """
        raise NotImplementedError()

    @abstractmethod
    def no_useful_difference(self, other: Self) -> bool:
        raise NotImplementedError()

    @classmethod
    def remove_redundant_copies(cls, resources: list[Self]) -> list[Self]:
        sorted_objs = sorted(resources, key=lambda r: r.sort_key_for_copies_of_resource())
        last_obj = sorted_objs[0]
        cleaned_objs = [last_obj]
        for obj in sorted_objs[1:]:
            if obj.no_useful_difference(last_obj):
                continue
            cleaned_objs.append(obj)
            last_obj = obj
        return cleaned_objs

    def refers_to_same_resource(self, other: Self) -> bool:
        return self.resource_id == other.resource_id

    @classmethod
    def all_refer_to_same_resource(cls, resources: list[Self]) -> bool:
        if len(resources) == 0:
            raise ValueError(f"There are no {cls.__name__} records, so they cannot all refer to the same object")
        if len(resources) == 1:
            return True
        first = resources[0]
        for msg in resources[1:]:
            if not first.refers_to_same_resource(msg):
                return False
        return True

    @classmethod
    def latest_copy_of_resource(cls, resources: list[Self]) -> Optional[Self]:
        if len(resources) == 0:
            return None
        if not cls.all_refer_to_same_resource(resources):
            raise ValueError("These records do not all refer to the same {cls.__name__}")
        sorted_objs = sorted(resources, key=lambda m: m.sort_key_for_copies_of_resource())
        return sorted_objs[-1]


SpecificResource = TypeVar("SpecificResource", bound="AbstractResource")
def group_by_id(objs: list[SpecificResource]) -> dict[int, list[SpecificResource]]:
    result = {}
    for obj in objs:
        if obj.resource_id not in result:
            result[obj.resource_id] = []
        result[obj.resource_id].append(obj)
    return result
