import asyncio
import datetime
import logging
import os
import sys
from logging.handlers import TimedRotatingFileHandler
from typing import List

from prometheus_client import start_http_server, Gauge
from telethon import TelegramClient

from tg_backup.config import load_config
from tg_backup.target import BackupTask

logger = logging.getLogger(__name__)


start_time = Gauge("tgbackup_startup_unixtime", "Last time TG backup was started")
task_count = Gauge("tgbackup_task_count", "Number of backup tasks configured")
last_backup_started = Gauge("tgbackup_latest_task_started_unixtime", "Last time a backup task was started")
last_backup_ended = Gauge("tgbackup_latest_task_ended_unixtime", "Last time a backup task completed")


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
    logger.addHandler(file_handler)


async def run_tasks_once(client: TelegramClient, tasks: List[BackupTask]) -> None:
    # Run all tasks once which don't have schedules
    for task in tasks:
        if task.config.schedule.run_once:
            last_backup_started.set_to_current_time()
            await task.run(client)
            last_backup_ended.set_to_current_time()


async def run_tasks_schedule(client: TelegramClient, tasks: List[BackupTask]) -> None:
    if all([task.config.schedule.run_once for task in tasks]):
        logger.debug("No tasks have schedules set, skipping scheduler")
        return
    while True:
        for task in tasks:
            if task.config.schedule.run_once:
                continue
            last_run = task.state.latest_start_time
            if not last_run:
                last_backup_started.set_to_current_time()
                await task.run(client)
                last_backup_ended.set_to_current_time()
                continue
            next_run = task.config.schedule.next_run_time(last_run)
            now_time = datetime.datetime.now(datetime.timezone.utc)
            if now_time > next_run:
                logger.info("Triggering scheduled backup task. Scheduled time: %s. Actual time: %s", next_run, now_time)
                last_backup_started.set_to_current_time()
                await task.run(client)
                last_backup_ended.set_to_current_time()
                continue
        logger.debug("Waiting for next scheduled backup")
        await asyncio.sleep(60)


def main() -> None:

    setup_logging()
    start_http_server(7467)
    start_time.set_to_current_time()
    conf = load_config()
    client = TelegramClient('simple_backup', conf.client.api_id, conf.client.api_hash)
    client.start()
    loop = asyncio.get_event_loop()
    tasks = [BackupTask(target_conf) for target_conf in conf.targets]
    task_count.set(len(tasks))
    # TODO: run all tasks at once, and resource downloaders independently. (But then how to know when one is done)
    # Run all the run_once tasks once.
    loop.run_until_complete(run_tasks_once(client, tasks))
    # Go through scheduled tasks, and run on schedules
    loop.run_until_complete(run_tasks_schedule(client, tasks))
    logger.info("All backups complete")
    # TODO: build some formatting stuff too, processing logs into usable html files
    # TODO: Some gallery to view all the photos from a chat?


if __name__ == '__main__':
    main()
