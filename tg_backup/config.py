import base64
import dataclasses
import datetime
import json
import os
from abc import ABC
from typing import Dict, List, Optional, BinaryIO

import dateutil.parser
import isodate
import telethon
from croniter import croniter

# noinspection PyUnresolvedReferences
SCHEME_LAYER = telethon.tl.alltlobjects.LAYER


def encode_json_extra(value: object) -> str:
    if isinstance(value, bytes):
        return base64.b64encode(value).decode('ascii')
    elif isinstance(value, datetime.datetime):
        return value.isoformat()
    else:
        raise ValueError(f"Unrecognised type to encode: {value}")


@dataclasses.dataclass
class StorableData:
    raw_data: Dict
    tl_scheme_layer: int = SCHEME_LAYER
    dl_date: datetime.datetime = dataclasses.field(default_factory=lambda: datetime.datetime.now(datetime.timezone.utc))

    def to_json(self) -> Dict:
        return {
            "dl_date": self.dl_date,
            "tl_scheme_layer": self.tl_scheme_layer,
            "raw_data": self.raw_data,
        }

    @classmethod
    def from_json(cls, data: Dict) -> "StorableData":
        return cls(
            data["raw_data"],
            data["tl_scheme_layer"],
            data["dl_date"],
        )


@dataclasses.dataclass
class ClientConfig:
    api_id: int
    api_hash: str

    @classmethod
    def from_json(cls, data: Dict) -> "ClientConfig":
        return cls(
            data["api_id"],
            data["api_hash"],
        )


@dataclasses.dataclass
class TargetState:
    latest_msg_id: Optional[int]
    latest_start_time: Optional[datetime.datetime]
    latest_end_time: Optional[datetime.datetime]
    tl_scheme_layer: Optional[int]

    def to_json(self) -> Dict:
        return {
            "latest_msg_id": self.latest_msg_id,
            "latest_start_time": self.latest_start_time,
            "latest_end_time": self.latest_end_time,
            "tl_scheme_layer": self.tl_scheme_layer,
        }

    @classmethod
    def from_json(cls, data: Optional[Dict]) -> "TargetState":
        if not data:
            return cls(None, None, None, None)
        latest_start_time, latest_end_time = None, None
        if latest_start_str := data["latest_start_time"]:
            latest_start_time = dateutil.parser.parse(latest_start_str)
        if latest_end_str := data["latest_end_time"]:
            latest_end_time = dateutil.parser.parse(latest_end_str)
        return cls(
            data["latest_msg_id"],
            latest_start_time,
            latest_end_time,
            data["tl_scheme_layer"],
        )


@dataclasses.dataclass
class LocationConfig:
    folder: str

    @classmethod
    def from_json(cls, data: Dict) -> "LocationConfig":
        return cls(
            data["folder"],
        )

    @classmethod
    def from_json_or_default(cls, data: Optional[Dict], default: Optional["LocationConfig"]) -> "LocationConfig":
        if data:
            return cls.from_json(data)
        if default:
            return default
        raise ValueError("Location has not been configured")


@dataclasses.dataclass
class MessageLocationConfig(LocationConfig):

    def load_state(self, chat_id: int) -> TargetState:  # TODO: database storage
        data = None
        try:
            with open(f"{self.folder}/{chat_id}/state.json", "r") as f:
                data = json.load(f)
        except FileNotFoundError:
            pass
        return TargetState.from_json(data)

    def save_state(self, chat_id: int, state: TargetState) -> None:
        chat_folder = f"{self.folder}/{chat_id}"
        os.makedirs(chat_folder, exist_ok=True)
        with open(f"{chat_folder}/state.json", "w") as f:
            json.dump(state.to_json(), f, default=encode_json_extra)

    def message_exists(self, chat_id: int, msg_id: int) -> bool:
        return os.path.exists(f"{self.folder}/{chat_id}/{msg_id}.json")

    def save_message(self, chat_id: int, msg_id: int, msg_data: StorableData) -> None:
        chat_folder = f"{self.folder}/{chat_id}"
        os.makedirs(chat_folder, exist_ok=True)
        with open(f"{chat_folder}/{msg_id}.json", "w") as f:
            json.dump(msg_data.to_json(), f, default=encode_json_extra)


@dataclasses.dataclass
class ChatsLocationConfig(LocationConfig):

    def load_chat(self, peer_id: int) -> Optional[StorableData]:
        try:
            with open(f"{self.folder}/{peer_id}.json", "r") as f:
                return StorableData.from_json(json.load(f))
        except FileNotFoundError:
            return None

    def save_chat(self, peer_id: int, peer_data: StorableData) -> None:
        os.makedirs(self.folder, exist_ok=True)
        with open(f"{self.folder}/{peer_id}.json", "w") as f:
            json.dump(peer_data.to_json(), f, default=encode_json_extra)


@dataclasses.dataclass
class DocumentLocationConfig(LocationConfig):

    def open_file(self, media_id: int, file_ext: str) -> BinaryIO:
        os.makedirs(self.folder, exist_ok=True)
        return open(f"{self.folder}/{media_id}.{file_ext}", "wb")

    def save_metadata(self, media_id: int, data: StorableData) -> None:
        os.makedirs(self.folder, exist_ok=True)
        with open(f"{self.folder}/{media_id}_meta.json", "w") as f:
            json.dump(data.to_json(), f, default=encode_json_extra)

    def file_exists(self, media_id: int) -> bool:
        return os.path.exists(f"{self.folder}/{media_id}_meta.json")


@dataclasses.dataclass
class PhotosLocationConfig(DocumentLocationConfig):

    def open_photo(self, media_id: int) -> BinaryIO:
        return super().open_file(media_id, "jpg")

    def photo_exists(self, media_id: int) -> bool:
        return super().file_exists(media_id)


@dataclasses.dataclass
class OutputConfig:
    messages: MessageLocationConfig
    chats: ChatsLocationConfig
    photos: PhotosLocationConfig
    documents: DocumentLocationConfig

    @classmethod
    def from_json(cls, data: Dict, default: Optional["OutputConfig"] = None) -> "OutputConfig":
        messages = MessageLocationConfig.from_json_or_default(
            data.get("metadata"),
            default.messages if default else None
        )
        chats = ChatsLocationConfig.from_json_or_default(data.get("chats"), default.chats if default else None)
        photos = PhotosLocationConfig.from_json_or_default(data.get("photos"), default.photos if default else None)
        documents = DocumentLocationConfig.from_json_or_default(data.get("documents"), default.documents if default else None)
        return cls(
            messages,
            chats,
            photos,
            documents,
        )


class ScheduleConfig(ABC):
    run_once = False

    @classmethod
    def from_json(cls, data: Optional[Dict]) -> "ScheduleConfig":
        if not data:
            return ScheduleConfigOnce()
        if period_str := data.get("period"):
            return ScheduleConfigPeriod(isodate.parse_duration(period_str))
        if cron_str := data.get("cron"):
            return ScheduleConfigCron(croniter(cron_str))
        # TODO: Add a live schedule, that keeps track of a chat and updates it as messages come in
        raise ValueError("Schedule is not defined")

    def next_run_time(self, latest_run: datetime.datetime) -> datetime.datetime:
        raise NotImplementedError


class ScheduleConfigOnce(ScheduleConfig):
    run_once = True


class ScheduleConfigPeriod(ScheduleConfig):
    def __init__(self, period: datetime.timedelta) -> None:
        self.period = period

    def next_run_time(self, latest_run: datetime.datetime) -> datetime.datetime:
        return latest_run + self.period


class ScheduleConfigCron(ScheduleConfig):
    def __init__(self, cron: croniter) -> None:
        self.cron = cron

    def next_run_time(self, latest_run: datetime.datetime) -> datetime.datetime:
        return self.cron.get_next(datetime.datetime, latest_run)


@dataclasses.dataclass
class TargetConfig:
    chat_id: int
    output: OutputConfig
    schedule: ScheduleConfig

    @classmethod
    def from_json(cls, data: Dict, default_output: OutputConfig, default_schedule: ScheduleConfig) -> "TargetConfig":
        chat_id = data["chat_id"]
        output = OutputConfig.from_json(data.get("output", {}), default=default_output)
        schedule = default_schedule
        if schedule_conf := data.get("schedule"):
            schedule = ScheduleConfig.from_json(schedule_conf)
        return cls(chat_id, output, schedule)


@dataclasses.dataclass
class BackupConfig:
    client: ClientConfig
    targets: List[TargetConfig]
    output: OutputConfig
    schedule: ScheduleConfig

    @classmethod
    def from_json(cls, data: Dict) -> "BackupConfig":
        client = ClientConfig.from_json(data["client"])
        output = OutputConfig.from_json(data["output"])
        schedule = ScheduleConfig.from_json(data.get("schedule"))
        targets = [
            TargetConfig.from_json(target_data, output, schedule) for target_data in data["backup_targets"]
        ]
        return cls(client, targets, output, schedule)


def load_config() -> BackupConfig:
    with open("config.json") as f:
        data = json.load(f)
    return BackupConfig.from_json(data)
