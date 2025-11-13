from typing import Callable, Iterable

from typing_extensions import TypeVar

Item = TypeVar("Item")
ItemKey = TypeVar("ItemKey")


def partition_list(items: Iterable[Item], condition: Callable[[Item], ItemKey]) -> dict[ItemKey, list[Item]]:
    results = {}
    for item in items:
        item_key = condition(item)
        if item_key not in results:
            results[item_key] = []
        results[item_key].append(item)
    return results


def split_list(items: Iterable[Item], condition: Callable[[Item], bool]) -> tuple[list[Item], list[Item]]:
    results = partition_list(items, condition)
    return results.get(True), results.get(False)
