import asyncio
import logging
from typing import Optional

from telethon import TelegramClient

from scripts.archive_target import ArchiveTarget
from scripts.config import Config, BehaviourConfig
from scripts.media_downloader import MediaDownloader


logger = logging.getLogger(__name__)


class Archiver:
    def __init__(self, conf: Config) -> None:
        self.config = conf
        self.client = TelegramClient("simple_backup", conf.client.api_id, conf.client.api_hash)
        self.started = False
        self.media_dl = MediaDownloader(self.client)
        self.media_dl_task: Optional[asyncio.Task] = None

    async def start(self) -> None:
        await self.client.start()
        asyncio.create_task(self.media_dl.run())
        self.started = True

    async def stop(self) -> None:
        logging.info("Awaiting shutdown of media downloader")
        self.media_dl.mark_as_filled()
        await self.media_dl_task

    async def archive_chat(self, chat_id: int, archive_behaviour: BehaviourConfig) -> None:
        # Start up archiver
        await self.start()
        # Archive the target chat
        behaviour = BehaviourConfig.merge(archive_behaviour, self.config.default_behaviour)
        target = ArchiveTarget(chat_id, behaviour, self)
        await target.archive_chat()
        # Shutdown
        await self.stop()
