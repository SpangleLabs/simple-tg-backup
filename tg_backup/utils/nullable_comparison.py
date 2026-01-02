import datetime
from typing import Optional, Union

from typing_extensions import TypeVar

T = TypeVar("T", bound=Union[int, float, datetime.datetime, datetime.timedelta])

def nullable_minimum(t1: Optional[T], t2: Optional[T]) -> Optional[T]:
    if t1 is None:
        return t2
    if t2 is None:
        return t1
    return min(t1, t2)

def nullable_maximum(t1: Optional[T], t2: Optional[T]) -> Optional[T]:
    if t1 is None:
        return t2
    if t2 is None:
        return t1
    return max(t1, t2)
