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
    """
    Configures the behaviour of the archiver. Could be the default configuration, or the configuration for an individual
    dialog.
    By the time it is passed to an ArchiveTarget to archive a dialog, all values should be non-None.

    Attributes:
        download_media: Whether to download media shared within a chat.
        check_admin_log: Whether to scrape the admin logs for events and records of deleted messages.
        follow_live: Whether to watch the dialog for live updates, and record them as they happen.
        archive_history: Whether to scrape the message history of the dialog.
        cleanup_duplicates: Whether to remove duplicate messages from the database while archiving chat history.
        msg_history_overlap_days: When archiving the message history of a chat, how much of the history should be
            processed. If set to zero (0), the entire chat history will be processed. If set to a positive integer, then
            the archiver will halt early once it has scraped this many days worth of messages which have been seen
            before, without seeing any new edits or deletions.
    """
    download_media: Optional[bool] = dataclasses.field(default=None)
    check_admin_log: Optional[bool] = dataclasses.field(default=None)
    follow_live: Optional[bool] = dataclasses.field(default=None)
    archive_history: Optional[bool] = dataclasses.field(default=None)
    cleanup_duplicates: Optional[bool] = dataclasses.field(default=None)
    msg_history_overlap_days: Optional[int] = dataclasses.field(default=None)

    def needs_archive_run(self) -> bool:
        return self.archive_history is True or self.check_admin_log is True

    @classmethod
    def merge(cls, b1: Optional["BehaviourConfig"], b2: "BehaviourConfig") -> "BehaviourConfig":
        if b1 is None:
            return b2
        return BehaviourConfig(
            download_media=b1.download_media if b1.download_media is not None else b2.download_media,
            check_admin_log=b1.check_admin_log if b1.check_admin_log is not None else b2.check_admin_log,
            follow_live=b1.follow_live if b1.follow_live is not None else b2.follow_live,
            archive_history=b1.archive_history if b1.archive_history is not None else b2.archive_history,
            cleanup_duplicates=b1.cleanup_duplicates if b1.cleanup_duplicates is not None else b2.cleanup_duplicates,
            msg_history_overlap_days=b1.msg_history_overlap_days if b1.msg_history_overlap_days is not None else b2.msg_history_overlap_days,
        )

    def to_dict(self) -> dict:
        return {
            "download_media": self.download_media,
            "check_admin_log": self.check_admin_log,
            "follow_live": self.follow_live,
            "archive_history": self.archive_history,
            "cleanup_duplicates": self.cleanup_duplicates,
            "msg_history_overlap_days": self.msg_history_overlap_days,
        }

    @classmethod
    def from_dict(cls, data: dict) -> "BehaviourConfig":
        return cls(
            download_media=data.get("download_media"),
            check_admin_log=data.get("check_admin_log"),
            follow_live=data.get("follow_live"),
            archive_history=data.get("archive_history"),
            cleanup_duplicates=data.get("cleanup_duplicates"),
            msg_history_overlap_days=data.get("msg_history_overlap_days"),
        )


DEFAULT_BEHAVIOUR = BehaviourConfig(
    download_media=True,
    check_admin_log=True,
    follow_live=False,
    archive_history=True,
    cleanup_duplicates=False,
    msg_history_overlap_days=3,
)


@dataclasses.dataclass
class Config:
    client: TelegramClientConfig
    default_behaviour: BehaviourConfig

    @classmethod
    def from_dict(cls, data: dict) -> "Config":
        default_behaviour = DEFAULT_BEHAVIOUR
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
