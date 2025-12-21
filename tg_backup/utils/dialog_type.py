from enum import Enum


class DialogType(Enum):
    USER = "user"
    GROUP = "group"
    SMALL_GROUP = "small_group"
    LARGE_GROUP = "large_group"
    CHANNEL = "channel"
    UNKNOWN = "unknown"

    @classmethod
    def from_str(cls, s: str) -> "DialogType":
        if s.lower() == "chat":
            return cls.GROUP
        return cls(s.lower())
