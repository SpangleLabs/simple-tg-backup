from typing import Optional, Iterable


def missing_ids_within_range(known_ids: Iterable[int], min_id: Optional[int], max_id: Optional[int]) -> list[int]:
    if min_id is None or max_id is None:
        # If one end of the range isn't known, return no missing values
        return []
    if abs(min_id - max_id) <= 1:
        # If there's no gap between IDs, there can't be any missing
        return []
    if min_id not in known_ids or max_id not in known_ids:
        # If ends of range aren't even in the known IDs list, we can't say what's missing
        return []
    # If the range endpoints are the wrong way around, flip them
    if min_id > max_id:
        min_id, max_id = max_id, min_id
    # Otherwise, list the IDs within the known IDs, within the range
    return [i for i in known_ids if min_id < i < max_id]


def missing_ids_before_value(known_ids: Iterable[int], end_id: Optional[int]) -> list[int]:
    if end_id is None:
        return []
    return [i for i in known_ids if i < end_id]
