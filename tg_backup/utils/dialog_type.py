from enum import Enum


class DialogType(Enum): # TODO: move to utils
    USER = "user"
    GROUP = "group"
    CHANNEL = "channel"
    UNKNOWN = "unknown"

    @classmethod
    def from_str(cls, s: str) -> "DialogType":
        if s.lower() == "chat":
            return cls.GROUP
        return cls(s.lower())
