import asyncio
import logging
import os
import sys
from logging.handlers import TimedRotatingFileHandler
from typing import Optional

import click
from prometheus_client import Gauge, start_http_server

from tg_backup.archiver import Archiver
from tg_backup.config import load_config, BehaviourConfig
from tg_backup.web_server import WebServer

logger = logging.getLogger(__name__)


start_time = Gauge("tgbackup_startup_unixtime", "Last time TG backup was started")


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
@click.option("--prom-port", type=int, help="Port to expose prometheus metrics on", default=8384)
@click.option("--chat-id", type=int, help="ID of the telegram chat to emergency save deleted messages")
@click.option("--download-media/--no-media", default=None, help="Whether to download media or not")
@click.option("--check-admin-log/--no-admin-log", default=None, help="Whether to check the admin log for recent events, such as deleted messages")
@click.option("--follow-live/--no-follow-live", default=None, help="Whether to follow live messages in the chat")
@click.option("--archive-history/--no-archive-history", default=None, help="Whether to archive the history of the chat before this point")
@click.option("--cleanup_duplicates/--no_cleanup_duplicates", default=None, help="Whether to clean up duplicate messages in the database")
@click.option("--msg_history_overlap", default=0, type=int, help="Number of days worth of overlapping non-modified messages to scrape before exiting re-archival early. (0 to archive entire history every time)")
def main(
        log_level: str,
        prom_port: int,
        chat_id: Optional[int],
        download_media: bool,
        check_admin_log: bool,
        follow_live: bool,
        archive_history: bool,
        cleanup_duplicates: bool,
        msg_history_overlap: int
) -> None:
    setup_logging(log_level)
    start_http_server(prom_port)
    conf = load_config()
    archiver = Archiver(conf)
    chat_archive_behaviour = BehaviourConfig(
        download_media=download_media,
        check_admin_log=check_admin_log,
        follow_live=follow_live,
        archive_history=archive_history,
        cleanup_duplicates=cleanup_duplicates,
        msg_history_overlap_days=msg_history_overlap,
    )
    if chat_id is None:
        web_server = WebServer(archiver)
        web_server.run()
    else:
        asyncio.run(archiver.archive_chat(chat_id, chat_archive_behaviour))


if __name__ == "__main__":
    main()
