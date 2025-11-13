from typing import NewType, Callable, Iterable

Item = NewType("Item")
ItemKey = NewType("ItemKey")

def split_list(items: Iterable[Item], condition: Callable[[Item], ItemKey]) -> dict[ItemKey, list[Item]]:
    results = {}
    for item in items:
        item_key = condition(item)
        if item_key not in results:
            results[item_key] = []
        results[item_key].append(item)
    return results
