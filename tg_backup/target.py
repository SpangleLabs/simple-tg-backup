import asyncio
import datetime
import logging
import sys
from asyncio import Queue, QueueEmpty, Task
from typing import Set, List, Type

import telethon
from prometheus_client import Gauge, Counter, Summary
from telethon import TelegramClient

from tg_backup.config import TargetConfig, OutputConfig, StorableData
from tg_backup.encoding import encode_message
from tg_backup.dl_resource import DLResource
from tg_backup.tg_utils import get_chat_name


logger = logging.getLogger(__name__)

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
messages_processed = Counter(
    "tgbackup_backup_task_processed_messages_count",
    "Number of messages processed by the backup manager",
)
backup_time_taken = Summary(
    "tgbackup_backup_task_time_taken",
    "Amount of time taken (in seconds) for the backup task to do various tasks",
    labelnames=["task"],
)
time_taken_setup_chat = backup_time_taken.labels(task="setting up chat")
time_taken_fetching_messages = backup_time_taken.labels(task="fetching messages")
time_taken_encoding_message = backup_time_taken.labels(task="encoding message")
time_taken_checking_message = backup_time_taken.labels(task="checking message exists")
time_taken_saving_message = backup_time_taken.labels(task="saving message")
time_taken_waiting_for_resource_dl = backup_time_taken.labels(task="waiting for resource downloader to complete")


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


class BackupTask:
    def __init__(self, config: TargetConfig) -> None:
        self.config = config
        self.state = self.config.output.messages.load_state(self.config.chat_id)
        self.resource_downloader = ResourceDownloader(config.output)

    async def run(self, client: TelegramClient) -> None:
        chat_id = self.config.chat_id
        last_message_id = self.state.latest_msg_id
        self.state.latest_start_time = datetime.datetime.now(datetime.timezone.utc)
        # noinspection PyUnresolvedReferences
        self.state.scheme_layer = telethon.tl.alltlobjects.LAYER

        # Setup chat info
        with time_taken_setup_chat.time():
            entity = await client.get_entity(chat_id)
            chat_name = get_chat_name(entity)
            updated_latest = False
            logger.info("Backing up target chat: %s", chat_name)

        # Start resource downloader
        resource_dl_task = asyncio.get_event_loop().create_task(self.resource_downloader.run(client))

        # Process messages
        processed_count = 0
        # TODO: metric how long it takes to fetch messages
        async for message in client.iter_messages(entity):
            msg_id = message.id
            processed_count += 1
            # Update latest ID with the first message
            if not updated_latest:
                self.state.latest_msg_id = msg_id
                updated_latest = True
            # Check if we've caught up
            if last_message_id is not None and msg_id <= last_message_id:
                logger.info(f"- Caught up on %s", chat_name)
                break
            # Encode message
            with time_taken_encoding_message.time():
                encoded_msg = encode_message(message)
            # Save message if new
            new_msg = False
            # TODO: metric how long it takes to check message exists
            if not self.config.output.messages.message_exists(chat_id, msg_id):  # TODO: Maybe save history of messages?
                with time_taken_saving_message.time():
                    msg_metadata = StorableData(encoded_msg.raw_data)
                    self.config.output.messages.save_message(chat_id, msg_id, msg_metadata)
                    new_msg = True
            logger.info(
                "%s message ID %s, date: %s. %s processed. Resources in queue: %s",
                "Saved" if new_msg else "Skipped",
                msg_id,
                message.date,
                processed_count,
                self.resource_downloader.dl_queue.qsize(),
            )
            # Handle downloadable resources
            for resource in encoded_msg.downloadable_resources:
                await self.resource_downloader.add_resource(resource)
            # Metrics
            messages_processed.inc()

        # Finish up
        logger.info("Finished scraping messages")
        with time_taken_waiting_for_resource_dl.time():
            await self.resource_downloader.stop()
            await resource_dl_task
        logger.info("Finished downloading resources")
        self.state.latest_end_time = datetime.datetime.now(datetime.timezone.utc)
        self.config.output.messages.save_state(chat_id, self.state)
