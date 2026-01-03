import datetime
from typing import Optional

from tg_backup.archive_target import ArchiveTarget, logger
from tg_backup.models.message import Message


class CutOffDate:
    def __init__(
            self,
            start_known_datetime: Optional[datetime.datetime],
            overlap_days: int,
            offset_id: int,
    ) -> None:
        self._overlap_days = overlap_days
        self._archive_target_started_empty = (start_known_datetime is None)
        self._oldest_known_datetime = start_known_datetime
        self.offset_id = offset_id

    @classmethod
    def _from_message(cls, message: Optional[Message], overlap_days: int, offset_id: int) -> "CutOffDate":
        return cls(
            message.datetime if message is not None else None,
            overlap_days,
            offset_id,
        )

    @classmethod
    def from_newest_msg_after_cutoff(cls, archive_target: "ArchiveTarget") -> "CutOffDate":
        latest_cutoff = archive_target.run_record.time_queued - datetime.timedelta(minutes=5)
        newest_msg = archive_target.chat_db.get_newest_message(latest_cutoff=latest_cutoff)
        return cls._from_message(newest_msg, archive_target.behaviour.msg_history_overlap_days, 0)

    @classmethod
    def from_oldest_msg(cls, archive_target: "ArchiveTarget") -> Optional["CutOffDate"]:
        oldest_msg = archive_target.chat_db.get_oldest_message()
        if oldest_msg is None:
            return None
        return cls._from_message(oldest_msg, 0, oldest_msg.id)

    def bump_known_datetime(self, known_datetime: datetime.datetime) -> None:
        """
        This is called every time a message change is detected, in order to keep going through history beyond it for
        another `overlap_days` days.
        """
        if self._oldest_known_datetime is None:
            self._oldest_known_datetime = known_datetime
        if known_datetime < self._oldest_known_datetime and not self._archive_target_started_empty:
            logger.info("Updating high water mark in chat history")
            self._oldest_known_datetime = known_datetime

    def cutoff_date(self) -> Optional[datetime.datetime]:
        overlap_days = self._overlap_days
        if overlap_days == 0:
            return None
        if self._archive_target_started_empty:
            return None
        # Only now calculate the cutoff date
        oldest_known_datetime = self._oldest_known_datetime
        if oldest_known_datetime is None:
            return None
        return oldest_known_datetime - datetime.timedelta(days=overlap_days)

    def cutoff_date_met(self, current_msg_date: datetime.datetime) -> bool:
        cutoff_date = self.cutoff_date()
        if cutoff_date is None:
            return False
        # If the current message is before the cutoff, we've gone back far enough
        return current_msg_date < cutoff_date
