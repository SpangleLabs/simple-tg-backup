import asyncio
import logging
import os
import sys
from logging.handlers import TimedRotatingFileHandler

from prometheus_client import start_http_server, Gauge

from tg_backup.config import load_config
from tg_backup.manager import Manager

start_time = Gauge("tgbackup_startup_unixtime", "Last time TG backup was started")


def setup_logging() -> None:
    os.makedirs("logs", exist_ok=True)
    formatter = logging.Formatter("{asctime}:{levelname}:{name}:{message}", style="{")

    base_logger = logging.getLogger()
    base_logger.setLevel(logging.DEBUG)
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    base_logger.addHandler(console_handler)

    backup_logger = logging.getLogger("tg_backup")
    file_handler = TimedRotatingFileHandler("logs/tg_backup.log", when="midnight")
    file_handler.setFormatter(formatter)
    backup_logger.addHandler(file_handler)


def main() -> None:

    setup_logging()
    start_http_server(7467)
    start_time.set_to_current_time()
    conf = load_config()
    manager = Manager(conf)
    loop = asyncio.get_event_loop()
    loop.run_until_complete(manager.run())


if __name__ == '__main__':
    main()
