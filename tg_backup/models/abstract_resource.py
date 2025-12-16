import dataclasses
import logging
from abc import ABC, abstractmethod
import datetime
from typing import Optional, TypeVar, Self, Any, Callable

import telethon


logger = logging.getLogger(__name__)

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
        It should be set up such that whe sorted by this key, resources are sorted from oldest to newest.
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


SpecificDeduplicatable = TypeVar("SpecificDeduplicatable", bound="DeduplicatableAbstractResource")
def cleanup_existing_duplicates(
        old_resources: list[SpecificDeduplicatable],
        db_delete_all_func: Callable[[int], None],
        db_save_func: Callable[[SpecificDeduplicatable], None],
) -> None:
    if len(old_resources) < 2:
        return
    resource_type: type[DeduplicatableAbstractResource] = type(old_resources[0])
    resource_id = old_resources[0].resource_id
    if not resource_type.all_refer_to_same_resource(old_resources):
        raise ValueError(f"These {resource_type.__name__} records do not all refer to the same {resource_id}")
    cleaned_objs = resource_type.remove_redundant_copies(old_resources)
    if len(cleaned_objs) == len(old_resources):
        return
    logger.info(
        "Cleaning up redundant %s %s copies for %s ID: %s",
        len(old_resources) - len(cleaned_objs),
        resource_type.__name__,
        resource_type.__name__,
        resource_id
    )
    db_delete_all_func(resource_id)
    for sticker_obj in cleaned_objs:
        db_save_func(sticker_obj)
    return


def save_if_not_duplicate(
        new_resource: SpecificDeduplicatable,
        cleanup_duplicates: bool,
        db_save_func: Callable[[SpecificDeduplicatable], None],
        db_list_all_func: Callable[[int], list[SpecificDeduplicatable]],
        db_delete_all_func: Callable[[int], None],
):
    resource_type = type(new_resource)
    resource_name = resource_type.__name__
    resource_id = new_resource.resource_id
    # Fetch the list of existing records for this resource
    old_resource_objs = db_list_all_func(new_resource.resource_id)
    # Cleanup duplicate stored records if applicable
    if cleanup_duplicates and len(old_resource_objs) >= 2:
        cleanup_existing_duplicates(old_resource_objs, db_delete_all_func, db_save_func)
    # Get the latest copy of the resource and see if the new one needs saving
    latest_saved_resource_obj = resource_type.latest_copy_of_resource(old_resource_objs)
    if new_resource.no_useful_difference(latest_saved_resource_obj):
        logger.debug("Already have %s ID %s archived sufficiently", resource_name, resource_id)
    else:
        logger.info("%s ID %s is sufficiently different to archived copies as to deserve re-saving", resource_name, resource_id)
