import dataclasses
import datetime
from typing import Optional

import yaml

from tg_backup.config import BehaviourConfig
from tg_backup.database.abstract_database import storable_date, parsable_date
from tg_backup.models.dialog import Dialog
from tg_backup.utils.chat_matcher import ChatData


STORE_FILE = "archive_settings.yaml"


@dataclasses.dataclass
class ChatSettingsEntry:
    chat_id: int
    data: Optional[ChatData]
    data_last_update: Optional[datetime.datetime]
    archive: Optional[bool]
    behaviour: Optional[BehaviourConfig]

    def is_all_defaults(self) -> bool:
        return self.archive is None and self.behaviour is None

    def to_dict(self) -> dict:
        return {
            "chat_id": self.chat_id,
            "chat_data": self.data.to_dict(),
            "data_last_update": storable_date(self.data_last_update),
            "archive": self.archive,
            "behaviour": self.behaviour.to_dict() if self.behaviour else None,
        }

    @classmethod
    def from_dict(cls, data: dict) -> "ChatSettingsEntry":
        return cls(
            chat_id=data["chat_id"],
            data=ChatData.from_dict(data["chat_data"]),
            data_last_update=parsable_date(data["data_last_update"]),
            archive=data["archive"],
            behaviour=BehaviourConfig.from_dict(data["behaviour"]) if data.get("behaviour") else None,
        )


@dataclasses.dataclass
class NewChatsFilter:
    filter: str
    archive: bool
    behaviour: Optional[BehaviourConfig]

    def to_dict(self) -> dict:
        return {
            "filter": self.filter,
            "archive": self.archive,
            "behaviour": self.behaviour.to_dict() if self.behaviour else None,
        }

    @classmethod
    def from_dict(cls, data: dict) -> "NewChatsFilter":
        return cls(
            filter=data["filter"],
            archive=data["archive"],
            behaviour=BehaviourConfig.from_dict(data["behaviour"]) if data.get("behaviour") else None,
        )

@dataclasses.dataclass
class ChatSettingsStore:
    default_archive: bool
    default_behaviour: Optional[BehaviourConfig] # Necessary?
    chat_settings: dict[int, ChatSettingsEntry]
    new_chat_filters: list[NewChatsFilter]

    def cleanup_redundant_entries(self) -> None:
        del_ids = []
        for chat_id, settings_entry in self.chat_settings.items():
            if settings_entry.is_all_defaults():
                del_ids.append(chat_id)
        for chat_id in del_ids:
            del self.chat_settings[chat_id]

    def set_chat_archive(self, chat_id: int, dialog: Dialog, archive: Optional[bool]) -> None:
        if chat_id not in self.chat_settings:
            chat_data = ChatData(chat_id, dialog.chat_type, None, dialog.name, None)
            self.chat_settings[chat_id] = ChatSettingsEntry(chat_id, chat_data, dialog.last_seen, archive, None)
        else:
            self.chat_settings[chat_id].archive = archive

    def to_dict(self) -> dict:
        return {
            "default_archive": self.default_archive,
            "default_behaviour": self.default_behaviour.to_dict() if self.default_behaviour is not None else None,
            "chat_settings": [entry.to_dict() for entry in self.chat_settings.values()],
            "new_chat_filters": [entry.to_dict() for entry in self.new_chat_filters]
        }

    def save_to_file(self) -> None:
        self.cleanup_redundant_entries()
        with open(STORE_FILE, "w") as f:
            yaml.dump(self.to_dict(), f, sort_keys=False)

    @classmethod
    def load_from_file(cls) -> "ChatSettingsStore":
        # Load and parse file
        try:
            with open(STORE_FILE, "r") as f:
                data = yaml.safe_load(f)
        except FileNotFoundError:
            data = {}
        behaviour = BehaviourConfig.from_dict(data["default_behaviour"]) if data.get("default_behaviour") else None
        chat_settings = {}
        for data_entry in data.get("chat_settings", []):
            settings = ChatSettingsEntry.from_dict(data_entry)
            chat_settings[settings.chat_id] = settings
        chat_filters = []
        for data_entry in data.get("new_chat_filters", []):
            chat_filter = NewChatsFilter.from_dict(data_entry)
            chat_filters.append(chat_filter)
        # Construct the settings store
        return cls(
            data.get("default_archive", False),
            behaviour,
            chat_settings,
            chat_filters,
        )