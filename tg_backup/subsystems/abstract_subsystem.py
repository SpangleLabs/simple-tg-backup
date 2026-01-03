import asyncio
import dataclasses
import datetime
import logging
from abc import ABC, abstractmethod
from typing import Optional, TypeVar, Generic, TYPE_CHECKING

from prometheus_client import Gauge
from telethon import TelegramClient

from tg_backup.subsystems.media_msg_refresh_cache import MessageRefreshCache
from tg_backup.utils.split_list import split_list

if TYPE_CHECKING:
    from tg_backup.archiver import Archiver


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
    def __init__(self, archiver: "Archiver", client: TelegramClient):
        self.archiver = archiver
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
    def __init__(self, archiver: "Archiver", client: TelegramClient):
        super().__init__(archiver, client)
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
        await self._wait_until_queue_empty(queue_key)

    async def _wait_until_queue_empty(self, queue_key: Optional[str]) -> None:
        if queue_key not in self.queues:
            return
        logger.info("Marking peer fetcher queue %s as stop when empty", queue_key)
        self.queues[queue_key].stop_when_empty = True
        await self.queues[queue_key].queue.join()
        del self.queues[queue_key]

    async def _wait_for_queue_and_message_refresher(self, queue_key: Optional[str], refresher: MessageRefreshCache) -> None:
        while True:
            # Get the archive target, for checking message refresher completion
            queue = self.queues.get(queue_key, None)
            archive_target = queue.info.archive_target if queue is not None else None
            # Wait for queue to empty
            await self._wait_until_queue_empty(queue_key)
            if archive_target is None:
                return
            # If message refresher is empty too, exit, otherwise wait for both queues to empty again
            if refresher.refresh_queue_size() == 0:
                return
            await refresher.wait_until_target_done(archive_target)

    async def _add_queue_entry(
            self,
            queue_key: Optional[str],
            info: QueueInfo,
            entry: QueueEntry,
    ) -> None:
        # Set up chat queue if needed
        if queue_key not in self.queues:
            raw_queue: asyncio.Queue[QueueEntry] = asyncio.Queue()
            new_queue = ArchiveRunQueue(queue_key, info, raw_queue)
            self.queues[queue_key] = new_queue
            await self.initialise_new_queue(new_queue)
        # Ensure chat queue isn't being emptied
        if self.queues[queue_key].stop_when_empty:
            logger.warning(f"Adding to {self.name()} queue which is due to stop when empty")
        # Add to chat queue
        logger.info(f"Added queue entry to {self.name()} queue")
        await self.queues[queue_key].queue.put(entry)

    async def initialise_new_queue(self, new_queue: ArchiveRunQueue[QueueInfo, QueueEntry]) -> None:
        # This allows subsystems to override initialising a new queue after it's created
        pass


CacheID = TypeVar("CacheID")
CacheValue = TypeVar("CacheValue")


@dataclasses.dataclass
class TimedCacheEntry(Generic[CacheID, CacheValue]):
    resource_id: CacheID
    date_cached: datetime.datetime
    value: CacheValue


class TimedCache(Generic[CacheID, CacheValue]):
    def __init__(self, cache_expiry: datetime.timedelta) -> None:
        self.cache_expiry = cache_expiry
        self._cache: dict[CacheID, TimedCacheEntry[CacheID, CacheValue]] = {}

    def is_resource_id_cached(self, resource_id: CacheID) -> bool:
        return self.get_resource_id_entry(resource_id) is not None

    def get_resource_id_entry(self, resource_id: CacheID) -> Optional[TimedCacheEntry[CacheID, CacheValue]]:
        cache_entry = self._cache.get(resource_id)
        if cache_entry is None:
            return None
        now = datetime.datetime.now(datetime.timezone.utc)
        if (now - cache_entry.date_cached) > self.cache_expiry:
            del self._cache[resource_id]
            return None
        return cache_entry

    def cache_resource_id(self, resource_id: CacheID) -> None:
        if not self.is_resource_id_cached(resource_id):
            now = datetime.datetime.now(datetime.timezone.utc)
            self._cache[resource_id] = TimedCacheEntry(resource_id, now, None)

    def cache_resource_value(self, resource_id: CacheID, value: CacheValue) -> None:
        now = datetime.datetime.now(datetime.timezone.utc)
        self._cache[resource_id] = TimedCacheEntry(resource_id, now, value)
