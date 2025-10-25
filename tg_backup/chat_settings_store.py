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
    archive: Optional[bool]
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
            chat_data = dialog.chat_data()
            self.chat_settings[chat_id] = ChatSettingsEntry(chat_id, chat_data, dialog.last_seen, archive, None)
        else:
            self.chat_settings[chat_id].archive = archive

    def should_archive_chat(self, dialog: Dialog) -> bool:
        chat_settings = self.chat_settings.get(dialog.resource_id)
        if chat_settings is not None and chat_settings.archive is not None:
            return chat_settings.archive
        return self.default_archive

    def behaviour_for_chat(self, chat_id: int, fallback: BehaviourConfig) -> BehaviourConfig:
        chat_settings = self.chat_settings.get(chat_id)
        default_fallback = BehaviourConfig.merge(self.default_behaviour, fallback)
        if chat_settings is None or chat_settings.behaviour is None:
            return default_fallback
        return BehaviourConfig.merge(chat_settings.behaviour, default_fallback)

    def list_archive_enabled(self, dialogs: list[Dialog], default_behaviour: BehaviourConfig) -> list[Dialog]:
        should_archive: list[Dialog] = []
        for dialog in dialogs:
            if self.should_archive_chat(dialog):
                should_archive.append(dialog)
        return should_archive

    def list_follow_live(self, dialogs: list[Dialog], default_behaviour: BehaviourConfig) -> list[Dialog]:
        follow_live: list[Dialog] = []
        for dialog in self.list_archive_enabled(dialogs, default_behaviour):
            behaviour = self.behaviour_for_chat(dialog.resource_id, default_behaviour)
            if behaviour.follow_live:
                follow_live.append(dialog)
        return follow_live

    def list_needs_archive_run(self, dialogs: list[Dialog], default_behaviour: BehaviourConfig) -> list[Dialog]:
        needs_archive_run: list[Dialog] = []
        for dialog in self.list_archive_enabled(dialogs, default_behaviour):
            behaviour = self.behaviour_for_chat(dialog.resource_id, default_behaviour)
            if behaviour.needs_archive_run():
                needs_archive_run.append(dialog)
        return needs_archive_run

    def behaviour_for_dialogs(
            self,
            dialogs: list[Dialog],
            default_behaviour: BehaviourConfig,
            override_behaviour: Optional[BehaviourConfig] = None,
    ) -> dict[int, BehaviourConfig]:
        result = {}
        for dialog in dialogs:
            behaviour = BehaviourConfig.merge(
                override_behaviour,
                self.behaviour_for_chat(dialog.resource_id, default_behaviour),
            )
            result[dialog.resource_id] = behaviour
        return result

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