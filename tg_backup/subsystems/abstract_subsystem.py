import asyncio
import dataclasses
import datetime
import logging
from abc import ABC, abstractmethod
from typing import Optional, TypeVar, Generic

from prometheus_client import Gauge
from telethon import TelegramClient

from tg_backup.database.chat_database import ChatDatabase
from tg_backup.utils.split_list import split_list

logger = logging.getLogger(__name__)

subsystem_queue_size = Gauge(
    "tgbackup_subsystem_queue_size",
    "Total size of each subsystem's queue",
    labelnames=["subsystem"],
)
subsystem_target_queue_count = Gauge(
    "tgbackup_subsystem_target_queue_count",
    "Number of target queues in each target queued subsystem",
    labelnames=["subsystem"],
)


class AbstractSubsystem(ABC):
    def __init__(self, client: TelegramClient):
        self.client = client
        self.running = False
        self.stop_when_empty = False
        self.task: Optional[asyncio.Task] = None
        subsystem_queue_size.labels(subsystem=self.name()).set_function(lambda: self.queue_size())

    def name(self) -> str:
        return type(self).__name__

    def start(self) -> None:
        if self.running:
            raise ValueError(f"Cannot start {self.name()}, it is already running")
        self.stop_when_empty = False
        self.task = asyncio.create_task(self.run())

    async def stop(self, fast: bool = False) -> None:
        # Shut down subsystem
        if not self.running:
            return
        logging.info("Awaiting shutdown of %s", self.name())
        if fast:
            self.abort()
        else:
            self.mark_as_filled()
        if self.task is not None:
            await self.task

    async def run(self) -> None:
        self.running = True
        while self.running:
            try:
                await self._do_process()
            except asyncio.QueueEmpty:
                if self.stop_when_empty:
                    logger.info("Queue is empty, shutting down %s", self.name())
                    self.running = False
                    return
                await asyncio.sleep(1)
                continue
            except Exception as e:
                logger.critical("Subsystem %s has failed with exception: ", self.name(), exc_info=e)
                raise e
            logger.info("There are %s remaining items in the %s queue", self.queue_size(), self.name())

    @abstractmethod
    async def _do_process(self) -> None:
        raise NotImplementedError()

    @abstractmethod
    def queue_size(self) -> int:
        raise NotImplementedError()

    def abort(self) -> None:
        self.running = False

    def mark_as_filled(self) -> None:
        self.stop_when_empty = True


QueueInfo = TypeVar("QueueInfo")
QueueEntry = TypeVar("QueueEntry")


@dataclasses.dataclass
class ArchiveRunQueue(Generic[QueueInfo, QueueEntry]):
    queue_key: Optional[str]
    info: QueueInfo
    queue: asyncio.Queue[QueueEntry]
    stop_when_empty: bool = False


class AbstractTargetQueuedSubsystem(AbstractSubsystem, ABC, Generic[QueueInfo, QueueEntry]):
    def __init__(self, client: TelegramClient):
        super().__init__(client)
        self.queues: dict[Optional[str], ArchiveRunQueue[QueueInfo, QueueEntry]] = {}
        subsystem_target_queue_count.labels(subsystem=self.name()).set_function(lambda: len(self.queues))

    def _get_next_in_queue(self) -> tuple[ArchiveRunQueue[QueueInfo, QueueEntry], QueueEntry]:
        targeted, non_targeted = split_list(self.queues.values(), lambda i: i.queue_key is not None)
        stopping, not_stopping = split_list(targeted, lambda i: i.stop_when_empty)
        queues_prioritised = stopping + not_stopping + non_targeted
        for queue in queues_prioritised:
            try:
                return queue, queue.queue.get_nowait()
            except asyncio.QueueEmpty:
                continue
        raise asyncio.QueueEmpty()

    def queue_size(self) -> int:
        return sum(queue.queue.qsize() for queue in self.queues.values())

    async def wait_until_queue_empty(self, queue_key: Optional[str]) -> None:
        if queue_key not in self.queues:
            return
        logger.info("Marking peer fetcher queue %s as stop when empty", queue_key)
        self.queues[queue_key].stop_when_empty = True
        await self.queues[queue_key].queue.join()
        del self.queues[queue_key]

    async def _add_queue_entry(
            self,
            queue_key: Optional[str],
            info: QueueInfo,
            entry: QueueEntry,
    ) -> None:
        # Set up chat queue if needed
        if queue_key not in self.queues:
            raw_queue: asyncio.Queue[QueueEntry] = asyncio.Queue()
            self.queues[queue_key] = ArchiveRunQueue(queue_key, info, raw_queue)
        # Ensure chat queue isn't being emptied
        if self.queues[queue_key].stop_when_empty:
            logger.warning(f"Adding to {self.name()} queue which is due to stop when empty")
        # Add to chat queue
        logger.info(f"Added queue entry to {self.name()} queue")
        await self.queues[queue_key].queue.put(entry)


C = TypeVar("C")


@dataclasses.dataclass
class TimedCacheEntry(Generic[C]):
    resource_id: C
    date_cached: datetime.datetime


class TimedCache(Generic[C]):
    def __init__(self, cache_expiry: datetime.timedelta) -> None:
        self.cache_expiry = cache_expiry
        self._cache: dict[C, TimedCacheEntry[C]] = {}

    def is_resource_id_cached(self, resource_id: C) -> bool:
        cache_entry = self._cache.get(resource_id)
        if cache_entry is None:
            return False
        now = datetime.datetime.now(datetime.timezone.utc)
        if (now - cache_entry.date_cached) > self.cache_expiry:
            del self._cache[resource_id]
            return False
        return True

    def cache_resource_id(self, resource_id: C) -> None:
        if not self.is_resource_id_cached(resource_id):
            now = datetime.datetime.now(datetime.timezone.utc)
            self._cache[resource_id] = TimedCacheEntry(resource_id, now)
