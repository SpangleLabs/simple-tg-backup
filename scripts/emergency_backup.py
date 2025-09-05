import asyncio
import json
import logging
import os
import sys
from logging.handlers import TimedRotatingFileHandler

import click

from scripts.archiver import Archiver

logger = logging.getLogger(__name__)


def setup_logging(log_level: str = "INFO") -> None:
    os.makedirs("logs", exist_ok=True)
    formatter = logging.Formatter("{asctime}:{levelname}:{name}:{message}", style="{")

    base_logger = logging.getLogger()
    base_logger.setLevel(log_level.upper())
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    base_logger.addHandler(console_handler)
    file_handler = TimedRotatingFileHandler("logs/backups.log", when="midnight")
    file_handler.setFormatter(formatter)
    base_logger.addHandler(file_handler)


@click.command()
@click.option("--log-level", type=str, help="Log level for the logger", default="INFO")
@click.option("--chat-id", type=int, help="ID of the telegram chat to emergency save deleted messages", required=True)
def main(log_level: str, chat_id: int) -> None:
    setup_logging(log_level)
    with open("config.json") as f:
        conf_data = json.load(f)
    archiver = Archiver(conf_data)
    asyncio.run(archiver.archive_chat(chat_id))


if __name__ == "__main__":
    main()
