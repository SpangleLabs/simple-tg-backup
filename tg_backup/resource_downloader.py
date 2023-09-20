import asyncio
import sys
from asyncio import Queue, Task, QueueEmpty
from typing import Type, Set, List

from prometheus_client import Gauge, Counter, Summary
from telethon import TelegramClient

from tg_backup.config import OutputConfig
from tg_backup.dl_resource import DLResource
from tg_backup.target import logger

resources_in_queue = Gauge(
    "tgbackup_resource_downloader_queue_length",
    "Number of resources in the Resource Downloader queue",
)
resources_processed = Counter(
    "tgbackup_resource_downloader_processed_count",
    "Number of resources which have completed processing",
    labelnames=["resource_type"],
)
resource_revisited = Counter(
    "tgbackup_resource_downloader_skipped_already_processed_count",
    "Number of resources which were skipped as already downloaded",
    labelnames=["resource_type"],
)
resource_processors_active = Gauge(
    "tgbackup_resource_downloader_active_processors",
    "Number of currently active processors in the resource downloader",
)
resource_dl_time_taken = Summary(
    "tgbackup_resource_downloader_time_taken",
    "Amount of time taken (in seconds) for the resource downloader to do various tasks",
    labelnames=["task"],
)
resource_time_taken_waiting_for_queue = resource_dl_time_taken.labels(task="waiting for resources in queue")
resource_time_taken_skipping_revisited_resource = resource_dl_time_taken.labels(
    task="skipping resources already downloaded"
)
resource_time_taken_downloading = resource_dl_time_taken.labels(task="downloading resource")


def all_subclasses(cls: Type) -> Set[Type]:
    return set(cls.__subclasses__()).union(
        [subsubcls for subcls in cls.__subclasses__() for subsubcls in subcls.__subclasses__()]
    )


for resource_type in all_subclasses(DLResource):
    resources_processed.labels(resource_type=resource_type.__name__)
    resource_revisited.labels(resource_type=resource_type.__name__)


class ResourceDownloader:
    NUM_PROCESSORS = 2

    def __init__(self, output: OutputConfig) -> None:
        self.output = output
        self.running = False
        self.dl_queue: Queue[DLResource] = Queue()
        self.completed_resources: Set[DLResource] = set()
        self.processors: List[Task] = []
        resources_in_queue.set_function(lambda: self.dl_queue.qsize())
        resource_processors_active.set_function(lambda: len(self.processors))

    async def run(self, client: TelegramClient) -> None:
        self.running = True
        loop = asyncio.get_event_loop()
        for _ in range(self.NUM_PROCESSORS):
            processor_task = loop.create_task(self.process_queue(client))
            self.processors.append(processor_task)
        logger.debug("Started up %s resource download processors", len(self.processors))
        for task in self.processors:
            await task
        self.processors.clear()
        logger.debug("All resource download processors complete")

    async def process_queue(self, client: TelegramClient) -> None:
        while True:
            try:
                next_resource = self.dl_queue.get_nowait()  # TODO: ability to prioritise small downloads first
            except QueueEmpty:
                if not self.running:
                    return
                with resource_time_taken_waiting_for_queue.time():
                    await asyncio.sleep(0.3)
                continue
            with resource_time_taken_skipping_revisited_resource.time():
                if next_resource in self.completed_resources:
                    self.dl_queue.task_done()
                    resource_revisited.labels(resource_type=type(next_resource).__name__).inc()
                    continue  # TODO: have file size limits for download
            logger.info("Downloading resource: %s", next_resource)
            with resource_time_taken_downloading.time():
                try:
                    await next_resource.download(client, self.output)
                except Exception as e:
                    logger.critical("Failed to download resource %s, shutting down", next_resource, exc_info=e)
                    sys.exit(1)
            self.completed_resources.add(next_resource)
            self.dl_queue.task_done()
            resources_processed.labels(resource_type=type(next_resource).__name__).inc()
            logger.info(
                "Resource downloaded. Total downloaded: %s. Resources in queue: %s",
                len(self.completed_resources),
                self.dl_queue.qsize()
            )

    async def add_resource(self, resource: DLResource) -> None:
        if resource in self.completed_resources:
            return
        await self.dl_queue.put(resource)

    async def stop(self) -> None:
        self.running = False
        await self.dl_queue.join()
        logger.info("Stopped resource downloader")
