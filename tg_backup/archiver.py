3import logging
from contextlib import asynccontextmanager
from typing import AsyncGenerator

from telethon import TelegramClient

from tg_backup.archive_target import ArchiveTarget
from tg_backup.config import Config, BehaviourConfig
from tg_backup.database.core_database import CoreDatabase
from tg_backup.subsystems.media_downloader import MediaDownloader
from tg_backup.subsystems.user_data_fetcher import UserDataFetcher

logger = logging.getLogger(__name__)


class Archiver:
    def __init__(self, conf: Config) -> None:
        self.config = conf
        self.client = TelegramClient("simple_backup", conf.client.api_id, conf.client.api_hash)
        self.started = False
        self.core_db = CoreDatabase()
        self.media_dl = MediaDownloader(self.client)
        self.user_fetcher = UserDataFetcher(self.client, self.core_db)

    async def start(self) -> None:
        logger.info("Starting Archiver core database")
        self.core_db.start()
        logger.info("Connecting to telegram")
        # noinspection PyUnresolvedReferences
        await self.client.start()
        logger.info("Starting media downloader")
        self.media_dl.start()
        logger.info("Starting user fetcher")
        self.user_fetcher.start()
        self.started = True

    async def stop(self, fast: bool = False) -> None:
        # Shut down media downloader
        await self.media_dl.stop(fast=fast)
        # Shut down user data fetcher
        await self.user_fetcher.stop(fast=fast)
        # Disconnect from telegram
        logger.info("Disconnecting from telegram")
        await self.client.disconnect()
        # Disconnect database
        logger.info("Disconnecting from core database")
        self.core_db.stop()

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
