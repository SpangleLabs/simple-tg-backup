import datetime
from typing import Optional

from tg_backup.archive_target import ArchiveTarget, logger


class CutOffDate:
    def __init__(self, archive_target: "ArchiveTarget") -> None:
        self.archive_target = archive_target
        self.initialised = False
        self._archive_target_started_empty = False
        self._oldest_known_datetime: Optional[datetime.datetime] = None

    def initialise(self) -> None:
        # Make sure to set a cutoff for the latest timestamp, otherwise live messages for this chat might have been
        # picked up before this archive target got to run.
        latest_cutoff = self.archive_target.run_record.time_queued - datetime.timedelta(minutes=5)
        newest_msg = self.archive_target.chat_db.get_newest_message(latest_cutoff=latest_cutoff)
        if newest_msg is None:
            self._archive_target_started_empty = True
            return
        self._oldest_known_datetime = newest_msg.datetime
        self.initialised = True

    def oldest_known_datetime(self) -> datetime.datetime:
        if not self.initialised:
            self.initialise()
        return self._oldest_known_datetime

    def bump_known_datetime(self, known_datetime: datetime.datetime) -> None:
        """
        This is called every time a message change is detected, in order to keep going through history beyond it for
        another `overlap_days` days.
        """
        if not self.initialised:
            self.initialise()
        if self._oldest_known_datetime is None:
            self._oldest_known_datetime = known_datetime
        oldest_datetime = min(self._oldest_known_datetime, known_datetime)
        if known_datetime == oldest_datetime and not self._archive_target_started_empty:
            logger.info("Updating high water mark in chat history")
        self._oldest_known_datetime = oldest_datetime

    def cutoff_date(self) -> Optional[datetime.datetime]:
        if not self.initialised:
            self.initialise()
        overlap_days = self.archive_target.behaviour.msg_history_overlap_days
        if overlap_days == 0:
            return None
        if self._archive_target_started_empty:
            return None
        # Only now calculate the cutoff date
        oldest_known_datetime = self.oldest_known_datetime()
        if oldest_known_datetime is None:
            return None
        return oldest_known_datetime - datetime.timedelta(days=overlap_days)

    def cutoff_date_met(self, current_msg_date: datetime.datetime) -> bool:
        cutoff_date = self.cutoff_date()
        if cutoff_date is None:
            return False
        # If the current message is before the cutoff, we've gone back far enough
        return current_msg_date < cutoff_date
