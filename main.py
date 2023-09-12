import asyncio
import logging
import os
import sys
from logging.handlers import TimedRotatingFileHandler

from telethon import TelegramClient

from tg_backup.config import load_config
from tg_backup.target import BackupTask

logger = logging.getLogger(__name__)


def setup_logging() -> None:
    os.makedirs("logs", exist_ok=True)
    formatter = logging.Formatter("{asctime}:{levelname}:{name}:{message}", style="{")

    base_logger = logging.getLogger()
    base_logger.setLevel(logging.INFO)
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    base_logger.addHandler(console_handler)

    backup_logger = logging.getLogger("tg_backup")
    file_handler = TimedRotatingFileHandler("logs/tg_backup.log", when="midnight")
    file_handler.setFormatter(formatter)
    backup_logger.addHandler(file_handler)


def main() -> None:
    setup_logging()
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
