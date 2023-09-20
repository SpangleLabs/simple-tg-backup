import asyncio
import datetime
import logging

import telethon
from prometheus_client import Counter, Summary
from telethon import TelegramClient

from tg_backup.config import TargetConfig, StorableData
from tg_backup.encoding import encode_message
from tg_backup.resource_downloader import ResourceDownloader
from tg_backup.tg_utils import get_chat_name


logger = logging.getLogger(__name__)

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
