import logging
from contextlib import asynccontextmanager
from typing import AsyncGenerator

from telethon import TelegramClient

from tg_backup.archive_target import ArchiveTarget
from tg_backup.chat_settings_store import ChatSettingsStore
from tg_backup.config import Config, BehaviourConfig
from tg_backup.database.core_database import CoreDatabase
from tg_backup.models.dialog import Dialog
from tg_backup.subsystems.media_downloader import MediaDownloader
from tg_backup.subsystems.sticker_downloader import StickerDownloader
from tg_backup.subsystems.peer_data_fetcher import PeerDataFetcher

logger = logging.getLogger(__name__)


class Archiver:
    def __init__(self, conf: Config) -> None:
        self.config = conf
        self.client = TelegramClient("simple_backup", conf.client.api_id, conf.client.api_hash)
        self.running = False
        self.running_list_dialogs = False
        self.core_db = CoreDatabase()
        self.media_dl = MediaDownloader(self.client)
        self.peer_fetcher = PeerDataFetcher(self.client, self.core_db)
        self.sticker_downloader = StickerDownloader(self.client, self.core_db)
        self.chat_settings = ChatSettingsStore.load_from_file()

    async def start(self) -> None:
        if self.running:
            raise ValueError("Archiver is already running")
        logger.info("Starting Archiver core database")
        self.core_db.start()
        logger.info("Connecting to telegram")
        # noinspection PyUnresolvedReferences
        await self.client.start()
        # List all dialogs
        # dialogs = await self.client.get_dialogs()
        # logger.info("Your telegram account has %s open dialogs", len(dialogs))
        logger.info("Starting media downloader")
        self.media_dl.start()
        logger.info("Starting user fetcher")
        self.peer_fetcher.start()
        logger.info("Starting sticker downloader")
        self.sticker_downloader.start()
        self.running = True

    async def stop(self, fast: bool = False) -> None:
        self.running = False
        # Shut down media downloader
        await self.media_dl.stop(fast=fast)
        # Shut down user data fetcher
        await self.peer_fetcher.stop(fast=fast)
        # Shut down sticker downloader
        await self.sticker_downloader.stop(fast=fast)
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

    async def save_dialogs(self) -> list[Dialog]:
        if self.running_list_dialogs:
            raise ValueError("Save dialogs is already running")
        self.running_list_dialogs = True
        dialogs = []
        async with self.run():
            logger.info("Fetching dialog list from Telegram")
            raw_dialogs = await self.client.get_dialogs()
            logger.info("Found %s dialogs", len(raw_dialogs))
            for dialog in raw_dialogs:
                dialog_obj = Dialog.from_dialog(dialog)
                dialogs.append(dialog_obj)
                self.core_db.save_dialog(dialog_obj)
                peer = dialog.dialog.peer
                await self.peer_fetcher.queue_peer(None, None, peer)
        self.running_list_dialogs = False
        return dialogs

    async def archive_chat(self, chat_id: int, archive_behaviour: BehaviourConfig) -> None:
        async with self.run():
            # Find the dialog of the target chat
            dialogs = self.core_db.list_dialogs()
            matching_dialogs = [d for d in dialogs if d.resource_id == chat_id]
            if not matching_dialogs:
                logger.info("Cannot find dialog for specified chat ID, re-scanning dialog list")
                dialogs = await self.save_dialogs()
                matching_dialogs = [d for d in dialogs if d.resource_id == chat_id]
                if not matching_dialogs:
                    logger.error("Cannot find dialog matching the given chat ID, after re-scanning")
                    raise ValueError("Cannot find dialog matching the given chat ID, after re-scanning")
            # Archive the target chat
            behaviour = BehaviourConfig.merge(archive_behaviour, self.config.default_behaviour)
            target = ArchiveTarget(matching_dialogs[0], behaviour, self)
            self.current_targets = [target]
            await target.archive_chat()
