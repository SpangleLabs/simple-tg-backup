import asyncio
import datetime
import logging
import sys
from asyncio import Queue, QueueEmpty
from typing import Set

import telethon
from telethon import TelegramClient

from tg_backup.config import TargetConfig, OutputConfig, StorableData
from tg_backup.encoding import encode_message
from tg_backup.dl_resource import DLResource
from tg_backup.tg_utils import get_chat_name


logger = logging.getLogger(__name__)


class ResourceDownloader:
    def __init__(self, output: OutputConfig) -> None:
        self.output = output
        self.running = False
        self.dl_queue: Queue[DLResource] = Queue()
        self.completed_resources: Set[DLResource] = set()

    async def run(self, client: TelegramClient) -> None:
        self.running = True
        while True:
            try:
                next_resource = await self.dl_queue.get()
            except QueueEmpty:
                if not self.running:
                    return
                await asyncio.sleep(0.3)
                continue
            if next_resource in self.completed_resources:
                continue
            logger.info("Downloading resource: %s", next_resource)
            try:
                await next_resource.download(client, self.output)
            except Exception:
                sys.exit(1)
            self.completed_resources.add(next_resource)
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
        entity = await client.get_entity(chat_id)
        chat_name = get_chat_name(entity)
        updated_latest = False
        logger.info("Backing up target chat: %s", chat_name)

        # Start resource downloader
        resource_dl_task = asyncio.get_event_loop().create_task(self.resource_downloader.run(client))

        # Process messages
        processed_count = 0
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
            encoded_msg = encode_message(message)
            # Save message if new
            new_msg = False
            if not self.config.output.messages.message_exists(chat_id, msg_id):  # TODO: Maybe save history of messages?
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

        # Finish up
        logger.info("Finished scraping messages")
        await self.resource_downloader.stop()
        await resource_dl_task
        logger.info("Finished downloading resources")
        self.state.latest_end_time = datetime.datetime.now(datetime.timezone.utc)
        self.config.output.messages.save_state(chat_id, self.state)
