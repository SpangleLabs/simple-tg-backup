import datetime
import logging
from typing import Dict, Set, Type

import telethon
from telethon import TelegramClient
from tqdm import tqdm

from tg_backup.config import TargetConfig
from tg_backup.encoding import encode_message, DLResource
from tg_backup.tg_utils import get_message_count, get_chat_name


logger = logging.getLogger(__name__)


class BackupTask:
    def __init__(self, config: TargetConfig) -> None:
        self.config = config
        self.state = self.config.output.metadata.load_state()

    async def run(self, client: TelegramClient) -> None:
        chat_id = self.config.chat_id
        last_message_id = self.state.latest_msg_id
        self.state.latest_start_time = datetime.datetime.now(datetime.timezone.utc)
        # noinspection PyUnresolvedReferences
        self.state.scheme_layer = telethon.tl.alltlobjects.LAYER

        entity = await client.get_entity(chat_id)
        count = await get_message_count(client, entity, last_message_id or 0)
        chat_name = get_chat_name(entity)
        updated_latest = False
        logger.info("Backing up target chat: %s", chat_name)
        print(f"- Updating {chat_name} logs")
        total_resources: Dict[Type, Set[DLResource]] = {}

        processed_count = 0
        with tqdm(total=count) as bar:
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
                # Handle message
                encoded_msg = encode_message(message)
                self.config.output.metadata.save_message(msg_id, encoded_msg.raw_data)
                logger.info("Saved message ID %s, date: %s. %s processed", msg_id, message.date, processed_count)
                # Handle downloadable resources  # TODO
                for resource in encoded_msg.downloadable_resources:
                    if type(resource) not in total_resources:
                        total_resources[type(resource)] = set()
                    total_resources[type(resource)].add(resource)
                total_resource_count = sum([len(x) for x in total_resources.values()])
                print(f"Gathered {total_resource_count} unique resources: " + ", ".join(f"{key.__name__}: {len(val)}" for key, val in total_resources.items()))
                bar.update(1)
        self.state.latest_end_time = datetime.datetime.now(datetime.timezone.utc)

        self.config.output.metadata.save_state(self.state)
