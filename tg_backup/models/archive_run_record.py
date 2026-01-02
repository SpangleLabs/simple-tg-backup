import dataclasses
import datetime
import logging
import uuid
from typing import Optional, TYPE_CHECKING

from tg_backup.config import BehaviourConfig
from tg_backup.utils.dialog_type import DialogType
from tg_backup.utils.nullable_comparison import nullable_minimum, nullable_maximum

if TYPE_CHECKING:
    from tg_backup.database.core_database import CoreDatabase

logger = logging.getLogger(__name__)


@dataclasses.dataclass
class ArchiveRunStats:
    record: "ArchiveRunRecord"
    messages_seen: int = dataclasses.field(default=0)
    messages_saved: int = dataclasses.field(default=0)
    media_seen: int = dataclasses.field(default=0)
    admin_events_seen: int = dataclasses.field(default=0)

    def inc_messages_seen(self) -> None:
        self.messages_seen += 1
        self.record.save()

    def inc_messages_saved(self) -> None:
        self.messages_saved += 1
        self.record.save()

    def inc_media_seen(self) -> None:
        self.media_seen += 1
        self.record.save()

    def inc_admin_events_seen(self) -> None:
        self.admin_events_seen += 1
        self.record.save()

    def to_dict(self) -> dict:
        return {
            "messages_seen": self.messages_seen,
            "messages_saved": self.messages_saved,
            "media_seen": self.media_seen,
            "admin_events_seen": self.admin_events_seen,
        }

    @classmethod
    def from_dict(cls, record: "ArchiveRunRecord", data: dict) -> "ArchiveRunStats":
        return cls(
            record=record,
            messages_seen=data["messages_seen"],
            messages_saved=data["messages_saved"],
            media_seen=data["media_seen"],
            admin_events_seen=data["admin_events_seen"],
        )


@dataclasses.dataclass
class ArchiveRunTimer:
    start_time: Optional[datetime.datetime]
    latest_msg_time: Optional[datetime.datetime]
    end_time: Optional[datetime.datetime]
    record: "ArchiveRunRecord"

    def start(self) -> None:
        if self.end_time is None:
            self.end_time = None
        if self.start_time is not None:
            return
        self.start_time = datetime.datetime.now(datetime.timezone.utc)
        self.record.save(force=True)

    def latest_msg(self) -> None:
        self.latest_msg_time = datetime.datetime.now(datetime.timezone.utc)
        self.record.save()

    def has_ended(self) -> bool:
        return self.end_time is not None

    def end(self) -> None:
        self.end_time = datetime.datetime.now(datetime.timezone.utc)
        self.record.save(force=True)

    def duration(self) -> Optional[datetime.timedelta]:
        if self.start_time is None:
            return None
        if self.end_time is None:
            return None
        return self.end_time - self.start_time

    def duration_str(self) -> str:
        if self.start_time is None:
            return ""
        if self.end_time is None:
            return "Running"
        duration = self.duration()
        if duration is None:
            return "Error calculating duration"
        hours, remainder = divmod(int(duration.total_seconds()), 3600)
        minutes, seconds = divmod(remainder, 60)
        if hours > 0:
            return f"{hours}h {minutes}m {seconds}s"
        return f"{minutes}m {seconds}s"


class ArchiveRunRecord:
    def __init__(
            self,
            target_type: DialogType,
            target_id: int,
            core_db: "CoreDatabase",
            time_queued: Optional[datetime.datetime] = None,
            run_time_start: Optional[datetime.datetime] = None,
            run_time_latest: Optional[datetime.datetime] = None,
            run_time_end: Optional[datetime.datetime] = None,
            behaviour_config: Optional[BehaviourConfig] = None,
            completed: bool = False,
            failure_reason: Optional[str] = None,
            archive_stats: Optional[ArchiveRunStats] = None,
            archive_run_id: Optional[str] = None,
    ) -> None:
        self.target_type = target_type
        self.target_id = target_id
        self.core_db = core_db
        self.time_queued = time_queued or datetime.datetime.now(datetime.timezone.utc)
        self.run_timer = ArchiveRunTimer(
            start_time=run_time_start,
            latest_msg_time=run_time_latest,
            end_time=run_time_end,
            record=self,
        )
        self.behaviour_config = behaviour_config
        self.completed = completed
        self.failure_reason = failure_reason
        self.archive_stats = archive_stats or ArchiveRunStats(self)
        self.archive_run_id = archive_run_id or str(uuid.uuid4())
        self.last_saved: datetime.datetime = datetime.datetime.now(datetime.timezone.utc)
        self.save(force=True)

    def save(self, force: bool = False) -> None:
        logger.debug("Record save request, force=%s", force)
        if not force:
            now_time = datetime.datetime.now(datetime.timezone.utc)
            save_age = (now_time - self.last_saved)
            if save_age < datetime.timedelta(minutes=3):
                return
        # If database isn't connected, skip
        if not self.core_db.is_connected() and not self.completed:
            logger.debug("Database not yet connected, skipping save")
            return
        logger.debug("Actually saving record")
        self.last_saved = datetime.datetime.now(datetime.timezone.utc)
        self.core_db.save_archive_run(self)

    def mark_queued(self) -> None:
        self.time_queued = datetime.datetime.now(datetime.timezone.utc)
        self.save(force=True)

    def mark_complete(self) -> None:
        self.completed = True
        self.run_timer.end()
        self.save(force=True)

    def mark_failed(self, failure_reason: str) -> None:
        self.failure_reason = failure_reason
        self.mark_complete()
