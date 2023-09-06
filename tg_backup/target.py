import json
import logging
from typing import Dict

from telethon import TelegramClient
from tqdm import tqdm

from tg_backup.encoding import encode_message
from tg_backup.tg_utils import get_message_count, get_chat_name


logger = logging.getLogger(__name__)


async def backup_target(client: TelegramClient, target: Dict) -> None:
    chat_id = target["chat_id"]
    last_message_id = 0  # TODO: Store for incremental backups

    entity = await client.get_entity(chat_id)
    count = await get_message_count(client, entity, last_message_id)
    chat_name = get_chat_name(entity)
    latest_id = None
    logger.info("Backing up target chat: %s", chat_name)
    print(f"- Updating {chat_name} logs")
    with tqdm(total=count) as bar:
        async for message in client.iter_messages(entity):
            if latest_id is None:
                latest_id = message.id
            if last_message_id is not None and message.id <= last_message_id:
                logger.info(f"- Caught up on %s", chat_name)
                break
            else:
                print(json.dumps(encode_message(message)))
            bar.update(1)

    last_message_id = latest_id
