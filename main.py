import asyncio
import logging

from telethon import TelegramClient

from tg_backup.config import load_config
from tg_backup.target import BackupTask

logger = logging.getLogger(__name__)


def main() -> None:
    conf = load_config()
    client = TelegramClient('simple_backup', conf.client.api_id, conf.client.api_hash)
    client.start()
    loop = asyncio.get_event_loop()
    tasks = [BackupTask(target_conf) for target_conf in conf.targets]
    for task in tasks:
        loop.run_until_complete(task.run(client))
    logger.info("All backups complete")


if __name__ == '__main__':
    main()
