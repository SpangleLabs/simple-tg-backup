import dataclasses
import json
from typing import Optional


@dataclasses.dataclass
class TelegramClientConfig:
    api_id: int
    api_hash: str

    @classmethod
    def from_dict(cls, data: dict) -> "TelegramClientConfig":
        return cls(
            api_id=data["api_id"],
            api_hash=data["api_hash"],
        )


@dataclasses.dataclass
class BehaviourConfig:
    download_media: Optional[bool]
    check_admin_log: Optional[bool]
    follow_live: Optional[bool]
    archive_history: Optional[bool]

    @classmethod
    def merge(cls, b1: "BehaviourConfig", b2: "BehaviourConfig") -> "BehaviourConfig":
        return BehaviourConfig(
            download_media=b1.download_media if b1.download_media is not None else b2.download_media,
            check_admin_log=b1.check_admin_log if b1.check_admin_log is not None else b2.check_admin_log,
            follow_live=b1.follow_live if b1.follow_live is not None else b2.follow_live,
            archive_history=b1.archive_history if b1.archive_history is not None else b2.archive_history,
        )

    @classmethod
    def from_dict(cls, data: dict) -> "BehaviourConfig":
        return cls(
            download_media=data.get("download_media"),
            check_admin_log=data.get("check_admin_log"),
            follow_live=data.get("follow_live"),
            archive_history=data.get("archive_history"),
        )


DEFAULT_BEHAVIOUR = BehaviourConfig(
    download_media=True,
    check_admin_log=True,
    follow_live=False,
    archive_history=True,
)


@dataclasses.dataclass
class Config:
    client: TelegramClientConfig
    default_behaviour: BehaviourConfig

    @classmethod
    def from_dict(cls, data: dict) -> "Config":
        if behaviour_config := data.get("default_behaviour"):
            default_behaviour = BehaviourConfig.merge(
                BehaviourConfig.from_dict(behaviour_config),
                DEFAULT_BEHAVIOUR,
            )
        return cls(
            client=TelegramClientConfig.from_dict(data["client"]),
            default_behaviour=default_behaviour,
        )


def load_config() -> Config:
    with open("config.json") as f:
        conf_data = json.load(f)
    return Config.from_dict(conf_data)
