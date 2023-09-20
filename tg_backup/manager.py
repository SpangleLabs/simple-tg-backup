import asyncio
import datetime
import logging

from prometheus_client import Gauge
from telethon import TelegramClient

from tg_backup.backup_target import BackupTarget
from tg_backup.config import BackupConfig

logger = logging.getLogger(__name__)
target_count = Gauge("tgbackup_target_count", "Number of backup targets configured")
last_backup_started = Gauge("tgbackup_latest_task_started_unixtime", "Last time a backup task was started")
last_backup_ended = Gauge("tgbackup_latest_task_ended_unixtime", "Last time a backup task completed")


class Manager:
    def __init__(self, config: BackupConfig) -> None:
        self.config = config
        self.targets = [BackupTarget(target_conf) for target_conf in config.targets]
        self.client = TelegramClient('simple_backup', self.config.client.api_id, self.config.client.api_hash)
        target_count.set(len(self.targets))

    async def run(self) -> None:
        await self.client.start()
        # TODO: run all tasks at once, and resource downloaders independently. (But then how to know when one is done)
        # Run all the run_once tasks once.
        await self.run_tasks_once()
        # Go through scheduled tasks, and run on schedules
        await self.run_tasks_schedule()
        logger.info("All backups complete")
        # TODO: build some formatting stuff too, processing logs into usable html files
        # TODO: Some gallery to view all the photos from a chat?

    async def run_backup(self, target: BackupTarget) -> None:
        last_backup_started.set_to_current_time()
        await target.run(self.client)
        last_backup_ended.set_to_current_time()

    async def run_tasks_once(self) -> None:
        # Run all tasks once which don't have schedules
        for target in self.targets:
            if target.config.schedule.run_once:
                await self.run_backup(target)

    async def run_tasks_schedule(self) -> None:
        if all([target.config.schedule.run_once for target in self.targets]):
            logger.debug("No backup targets have schedules set, skipping scheduler")
            return
        while True:
            for target in self.targets:
                if target.config.schedule.run_once:
                    continue
                last_run = target.state.latest_start_time
                if not last_run:
                    await self.run_backup(target)
                    continue
                next_run = target.config.schedule.next_run_time(last_run)
                now_time = datetime.datetime.now(datetime.timezone.utc)
                if now_time > next_run:
                    logger.info(
                        "Triggering scheduled backup of chat ID: %s. Scheduled time: %s. Actual time: %s",
                        target.config.chat_id,
                        next_run,
                        now_time,
                    )
                    await self.run_backup(target)
                    continue
            logger.debug("Waiting for next scheduled backup")
            await asyncio.sleep(60)
