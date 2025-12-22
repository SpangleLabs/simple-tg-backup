import asyncio
import datetime
import logging
from contextlib import asynccontextmanager
from typing import TYPE_CHECKING, Optional, AsyncIterator

import telethon.tl.types
from prometheus_client import Counter, Summary
from telethon import TelegramClient

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
    labelnames=["used_takeout"],
)
list_dialogs_call_timer.labels(used_takeout="True")
list_dialogs_call_timer.labels(used_takeout="False")


class DialogFetcher:
    MAX_DIALOG_REQUEST_RATE = datetime.timedelta(minutes=15)
    MAX_TAKEOUT_REQUEST_RATE = datetime.timedelta(days=7)

    def __init__(self, archiver: "Archiver") -> None:
        self.archiver = archiver
        self.client = archiver.client
        self.core_db = archiver.core_db
        self._dialogs: dict[int, Dialog] = {}
        self._latest_raw_dialog_request_time: Optional[datetime.datetime] = None
        self._latest_takeout_time: Optional[datetime.datetime] = None
        self._fetching_raw_dialogs_lock = asyncio.Lock()

    def _can_request_dialogs_again(self) -> bool:
        if self._latest_raw_dialog_request_time is None:
            return True
        now = datetime.datetime.now(datetime.timezone.utc)
        return (now - self._latest_raw_dialog_request_time) > self.MAX_DIALOG_REQUEST_RATE

    def _can_use_takeout_again(self) -> bool:
        if self._latest_takeout_time is None:
            return True
        now = datetime.datetime.now(datetime.timezone.utc)
        return (now - self._latest_takeout_time) > self.MAX_TAKEOUT_REQUEST_RATE

    @asynccontextmanager
    async def _try_takeout_wrapper(self) -> AsyncIterator[tuple[TelegramClient, bool]]:
        # If we can't use takeout, then return a normal client
        if not self._can_use_takeout_again():
            yield self.client, False
            return
        # Try and use takeout wrapper
        try:
            async with self.client.takeout(contacts=True, users=True, chats=True, megagroups=True, channels=True) as tclient:
                logger.info("Using Takeout session")
                yield tclient, True
            logger.info("Takeout session closed")
            # If that worked, set the latest takeout time
            self._latest_takeout_time = datetime.datetime.now(datetime.timezone.utc)
            return
        # Catch takeout exception and use normal client
        except telethon.errors.TakeoutInitDelayError as e:
            time_until_takeout = e.seconds
            logger.info("Cannot use takeout wrapper to fetch dialogs for %s seconds", time_until_takeout)
            # Return a normal client
            yield self.client, False
            # Calculate when we can next use takeout
            now = datetime.datetime.now(datetime.timezone.utc)
            offset = self.MAX_TAKEOUT_REQUEST_RATE - datetime.timedelta(seconds=time_until_takeout)
            self._latest_takeout_time = now - offset
            return

    async def fetch_dialogs_list(self) -> None:
        # If it was requested too recently, don't re-run it
        if not self._can_request_dialogs_again():
            return
        # Acquire lock to make sure you can't fetch dialogs twice at once
        async with self._fetching_raw_dialogs_lock:
            # Check again whether it was just refreshed, in case you were waiting on the lock
            if not self._can_request_dialogs_again():
                return
            # Try and use takeout, if we can. Not sure why the linter dislikes this
            # noinspection PyArgumentList
            async with self._try_takeout_wrapper() as [client, used_takeout]:
                # Request the list of dialogs from Telegram
                with list_dialogs_call_timer.labels(used_takeout=str(used_takeout)).time():
                    raw_dialogs = await client.get_dialogs()
            self._latest_raw_dialog_request_time = datetime.datetime.now(datetime.timezone.utc)
            # Set up some variables for the logging
            previous_num_dialogs = len(self._dialogs)
            new_dialog_count = 0
            # Convert into Dialog model objects, and save to database
            for dialog in raw_dialogs:
                # Convert to Dialog model
                dialog_obj = Dialog.from_dialog(dialog, used_takeout)
                # Save Dialog to the database
                self.core_db.save_dialog(dialog_obj)
                # Queue up the peer for fetching
                peer = dialog.dialog.peer
                await self.archiver.peer_fetcher.queue_peer(None, None, None, peer)
                # Add to the dialogs dict
                old_dialog = self._dialogs.get(dialog_obj.resource_id)
                if old_dialog is None:
                    new_dialog_count += 1
                else:
                    dialog_obj.merge_with_old_record(old_dialog)
                self._dialogs[dialog_obj.resource_id] = dialog_obj
            # Write a log line explaining what happened
            num_raw_dialogs = len(raw_dialogs)
            logger.info(
                "Found %s raw dialogs. Previously had %s. Added %s new dialogs. %s",
                num_raw_dialogs, previous_num_dialogs, new_dialog_count,
                f" (Used takeout session)" if used_takeout else "",
            )

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
        # Otherwise, if nothing is in the database, fetch the list of Dialogs from telegram
        await self.fetch_dialogs_list()
        return list(self._dialogs.values())

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
            logger.info("Could not find dialog for chat_id %s", chat_id)
        else:
            count_get_dialog_request__newly_fetched.inc()
        return dialog

    def is_fetching_dialogs(self) -> bool:
        return self._fetching_raw_dialogs_lock.locked()
