import asyncio
import datetime
import logging
from typing import TYPE_CHECKING, Optional

import telethon.tl.types
from prometheus_client import Counter, Summary

from tg_backup.models.dialog import Dialog

if TYPE_CHECKING:
    from tg_backup.archiver import Archiver


logger = logging.getLogger(__name__)

count_get_dialog_requests = Counter(
    "tgbackup_dialogfetcher_get_dialog_calls_count",
    "Number of times an individual dialog gets requested from the DialogFetcher",
    labelnames=["mechanism"],
)
count_get_dialog_request__cached = count_get_dialog_requests.labels(mechanism="already_cached")
count_get_dialog_request__from_db = count_get_dialog_requests.labels(mechanism="from_db")
count_get_dialog_request__newly_fetched = count_get_dialog_requests.labels(mechanism="newly_fetched")
count_get_dialog_request__not_found = count_get_dialog_requests.labels(mechanism="not_found")
list_dialogs_call_timer = Summary(
    "tgbackup_dialogfetcher_fetch_dialogs_call_time_seconds",
    "Time taken to fetch the list of dialogs from Telegram",
)


class DialogFetcher:
    MAX_DIALOG_REQUEST_RATE = datetime.timedelta(minutes=15)

    def __init__(self, archiver: "Archiver") -> None:
        self.archiver = archiver
        self.client = archiver.client
        self.core_db = archiver.core_db
        self._dialogs: dict[int, Dialog] = {}
        self._raw_dialogs = [telethon.tl.types.Dialog]
        self._latest_raw_dialog_request_time: Optional[datetime.datetime] = None
        self._fetching_raw_dialogs_lock = asyncio.Lock()

    def _can_request_dialogs_again(self) -> bool:
        if self._latest_raw_dialog_request_time is None:
            return True
        now = datetime.datetime.now(datetime.timezone.utc)
        return (now - self._latest_raw_dialog_request_time) > self.MAX_DIALOG_REQUEST_RATE

    async def fetch_dialogs_list(self) -> list[Dialog]:
        # If it was requested too recently, don't re-run it
        if not self._can_request_dialogs_again():
            return list(self._dialogs.values())
        # Acquire lock to make sure you can't fetch dialogs twice at once
        async with self._fetching_raw_dialogs_lock:
            # Check again whether it was just refreshed, in case you were waiting on the lock
            if not self._can_request_dialogs_again():
                return list(self._dialogs.values())
            dialogs: list[Dialog] = []
            # Request the list of dialogs from Telegram
            with list_dialogs_call_timer.time():
                raw_dialogs = await self.client.get_dialogs()
            self._raw_dialogs = raw_dialogs
            self._latest_raw_dialog_request_time = datetime.datetime.now(datetime.timezone.utc)
            # Log the new count
            num_dialogs = len(raw_dialogs)
            previous_num_dialogs = len(self._dialogs)
            num_change = num_dialogs - previous_num_dialogs
            logger.info("Found %s dialogs. Previously had %s. Change: %s", num_dialogs, previous_num_dialogs, num_change)
            # Convert into Dialog model objects, and save to database
            for dialog in raw_dialogs:
                dialog_obj = Dialog.from_dialog(dialog)
                dialogs.append(dialog_obj)
                self.core_db.save_dialog(dialog_obj)
                peer = dialog.dialog.peer
                await self.archiver.peer_fetcher.queue_peer(None, None, None, peer)
            # Load the dialogs list as a dict
            self._dialogs = {d.resource_id: d for d in dialogs}
            # Return the new list of dialogs
            return dialogs

    def _list_dialogs_from_db(self) -> list[Dialog]:
        db_dialogs = self.core_db.list_dialogs()
        self._dialogs = {d.resource_id: d for d in db_dialogs}
        return db_dialogs

    async def list_dialogs(self) -> list[Dialog]:
        if len(self._dialogs) > 0:
            return list(self._dialogs.values())
        # If no list has been fetched yet, fetch Dialogs from the database
        db_dialogs = self._list_dialogs_from_db()
        if len(db_dialogs) > 0:
            return db_dialogs
        # Otherwise, fetch the list of Dialogs from telegram
        tg_dialogs = await self.fetch_dialogs_list()
        return tg_dialogs

    async def get_dialog(self, chat_id: int) -> Optional[Dialog]:
        # If it's in the dictionary already, return that
        dialog = self._dialogs.get(chat_id)
        if dialog is not None:
            count_get_dialog_request__cached.inc()
            return dialog
        # If no list has been fetched yet, fetch Dialogs from the database
        if len(self._dialogs) == 0:
            self._list_dialogs_from_db()
            dialog = self._dialogs.get(chat_id)
            if dialog is not None:
                count_get_dialog_request__from_db.inc()
                return dialog
        # If we can, re-fetch the list from Telegram
        if self._can_request_dialogs_again():
            await self.fetch_dialogs_list()
        # Attempt to return from the dictionary again
        dialog = self._dialogs.get(chat_id)
        if dialog is None:
            count_get_dialog_request__not_found.inc()
        else:
            count_get_dialog_request__newly_fetched.inc()
        return dialog

    def is_fetching_dialogs(self) -> bool:
        return self._fetching_raw_dialogs_lock.locked()
