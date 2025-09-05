import asyncio
import logging
import os
import sys
from logging.handlers import TimedRotatingFileHandler

import click

from scripts.archiver import Archiver
from scripts.config import load_config, BehaviourConfig

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
@click.option("--download-media/--no-media", default=None, help="Whether to download media or not")
@click.option("--check-admin-log/--no-admin-log", default=None, help="Whether to check the admin log for recent events, such as deleted messages")
@click.option("--follow-live/--no-follow-live", default=None, help="Whether to follow live messages in the chat")
@click.option("--archive-history/--no-archive-history", default=None, help="Whether to archive the history of the chat before this point")
def main(
        log_level: str,
        chat_id: int,
        download_media: bool,
        check_admin_log: bool,
        follow_live: bool,
        archive_history: bool,
) -> None:
    setup_logging(log_level)
    conf = load_config()
    archiver = Archiver(conf)
    chat_archive_behaviour = BehaviourConfig(
        download_media=download_media,
        check_admin_log=check_admin_log,
        follow_live=follow_live,
        archive_history=archive_history,
    )
    asyncio.run(archiver.archive_chat(chat_id, chat_archive_behaviour))


if __name__ == "__main__":
    main()
