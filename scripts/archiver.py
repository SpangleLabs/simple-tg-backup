import asyncio
import logging
from contextlib import asynccontextmanager
from typing import Optional, AsyncGenerator

from telethon import TelegramClient

from scripts.archive_target import ArchiveTarget
from scripts.config import Config, BehaviourConfig
from scripts.database.core_database import CoreDatabase
from scripts.media_downloader import MediaDownloader


logger = logging.getLogger(__name__)


class Archiver:
    def __init__(self, conf: Config) -> None:
        self.config = conf
        self.client = TelegramClient("simple_backup", conf.client.api_id, conf.client.api_hash)
        self.started = False
        self.media_dl = MediaDownloader(self.client)
        self.media_dl_task: Optional[asyncio.Task] = None
        self.core_db = CoreDatabase()

    async def start(self) -> None:
        logger.info("Starting Archiver core database")
        self.core_db.start()
        logger.info("Connecting to telegram")
        # noinspection PyUnresolvedReferences
        await self.client.start()
        logger.info("Starting media downloader")
        self.media_dl_task = asyncio.create_task(self.media_dl.run())
        self.started = True

    async def stop(self, fast: bool = False) -> None:
        # Shut down media downloader
        await self._stop_media_dl(fast=fast)
        # Disconnect from telegram
        logger.info("Disconnecting from telegram")
        await self.client.disconnect()
        # Disconnect database
        logger.info("Disconnecting from core database")
        self.core_db.stop()

    async def _stop_media_dl(self, fast: bool = False) -> None:
        # Shut down media downloader
        if not self.media_dl.running:
            return
        logging.info("Awaiting shutdown of media downloader")
        if fast:
            self.media_dl.abort()
        else:
            self.media_dl.mark_as_filled()
        if self.media_dl_task is not None:
            await self.media_dl_task

    @asynccontextmanager
    async def run(self) -> AsyncGenerator[None]:
        await self.start()
        # noinspection PyBroadException
        try:
            yield
        except Exception as e:
            logger.exception("Archiver has encountered exception, shutting down")
            await self.stop(fast=True)
        finally:
            await self.stop(fast=False)

    async def archive_chat(self, chat_id: int, archive_behaviour: BehaviourConfig) -> None:
        async with self.run():
            # Archive the target chat
            behaviour = BehaviourConfig.merge(archive_behaviour, self.config.default_behaviour)
            target = ArchiveTarget(chat_id, behaviour, self)
            await target.archive_chat()
