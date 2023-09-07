import dataclasses
import json
import os
from typing import Dict, List, Optional


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

    def to_json(self) -> Dict:
        return {
            "latest_msg_id": self.latest_msg_id,
        }

    @classmethod
    def from_json(cls, data: Dict) -> "TargetState":
        return cls(
            data["latest_msg_id"]
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
class MetadataLocationConfig(LocationConfig):

    def load_state(self) -> TargetState:
        os.makedirs(self.folder, exist_ok=True)
        try:
            with open(f"{self.folder}/state.json", "r") as f:
                return TargetState.from_json(json.load(f))
        except FileNotFoundError:
            return TargetState(None)

    def save_state(self, state: TargetState) -> None:
        os.makedirs(self.folder, exist_ok=True)
        with open(f"{self.folder}/state.json", "w") as f:
            json.dump(state.to_json(), f)


@dataclasses.dataclass
class OutputConfig:
    metadata: MetadataLocationConfig
    chats: LocationConfig
    photos: LocationConfig
    documents: LocationConfig

    @classmethod
    def from_json(cls, data: Dict, default: Optional["OutputConfig"] = None) -> "OutputConfig":
        metadata = MetadataLocationConfig.from_json_or_default(
            data.get("metadata"),
            default.metadata if default else None
        )
        chats = LocationConfig.from_json_or_default(data.get("chats"), default.chats if default else None)
        photos = LocationConfig.from_json_or_default(data.get("photos"), default.photos if default else None)
        documents = LocationConfig.from_json_or_default(data.get("documents"), default.documents if default else None)
        return cls(
            metadata,
            chats,
            photos,
            documents,
        )


@dataclasses.dataclass
class TargetConfig:
    chat_id: int
    output: OutputConfig

    @classmethod
    def from_json(cls, data: Dict, default_output: OutputConfig) -> "TargetConfig":
        chat_id = data["chat_id"]
        output = OutputConfig.from_json(data.get("output", {}), default=default_output)
        return cls(chat_id, output)


@dataclasses.dataclass
class BackupConfig:
    client: ClientConfig
    targets: List[TargetConfig]
    output: OutputConfig

    @classmethod
    def from_json(cls, data: Dict) -> "BackupConfig":
        client = ClientConfig.from_json(data["client"])
        output = OutputConfig.from_json(data["output"])
        targets = [
            TargetConfig.from_json(target_data, output) for target_data in data["backup_targets"]
        ]
        return cls(client, targets, output)


def load_config() -> BackupConfig:
    with open("config.json") as f:
        data = json.load(f)
    return BackupConfig.from_json(data)
