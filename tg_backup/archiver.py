import logging
from contextlib import asynccontextmanager
from typing import AsyncGenerator

from prometheus_client import Gauge
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


archiver_running = Gauge(
    "tgbackup_archiver_running",
    "Whether the TG backup archiver is currently running",
)
archiver_current_targets = Gauge(
    "tgbackup_archiver_current_targets_count",
    "Count of how many targets the archiver is currently archiving",
)
archiver_completed_targets = Gauge(
    "tgbackup_archiver_completed_targets_count",
    "Count of how many of the current archive targets the archiver has completed archiving",
)


class Archiver:
    def __init__(self, conf: Config) -> None:
        self.config = conf
        self.client = TelegramClient("simple_backup", conf.client.api_id, conf.client.api_hash)
        self.running = False
        self.running_list_dialogs = False
        self.current_targets: list[ArchiveTarget] = []
        self.core_db = CoreDatabase()
        self.media_dl = MediaDownloader(self.client)
        self.peer_fetcher = PeerDataFetcher(self.client, self.core_db)
        self.sticker_downloader = StickerDownloader(self.client, self.core_db)
        self.chat_settings = ChatSettingsStore.load_from_file()
        archiver_running.set_function(lambda: int(self.running))
        archiver_current_targets.set_function(lambda: len(self.current_targets))
        archiver_completed_targets.set_function(lambda: len([t for t in self.current_targets if t.run_record.archive_history_timer.has_ended()]))

    async def start(self) -> None:
        if self.running:
            raise ValueError("Archiver is already running")
        self.running = True
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

    async def stop(self, fast: bool = False) -> None:
        logger.info("Stopping archiver")
        # Shut down media downloader
        logger.info("Waiting for media downloader to complete")
        await self.media_dl.stop(fast=fast)
        # Shut down user data fetcher
        logger.info("Waiting for peer fetcher to complete")
        await self.peer_fetcher.stop(fast=fast)
        # Shut down sticker downloader
        logger.info("Waiting for sticker downloader to complete")
        await self.sticker_downloader.stop(fast=fast)
        # Disconnect from telegram
        logger.info("Disconnecting from telegram")
        await self.client.disconnect()
        # Disconnect database
        logger.info("Disconnecting from core database")
        self.core_db.stop()
        self.running = False
        logger.info("Archiver stopped")

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

    async def run_archive(self) -> None:
        """
        Runs the archiver with the ChatSettingsStore settings
        """
        async with self.run():
            dialogs = self.core_db.list_dialogs()
            fallback_behaviour = self.config.default_behaviour
            # Watch for new messages from applicable chats
            logger.info("Checking if any dialogs need following live")
            if self.chat_settings.any_follow_live(dialogs, fallback_behaviour):
                # TODO: Not sure how best to do this, fun
                raise NotImplementedError("Have not yet implemented follow live for multiple chats")
            # Archive the history of applicable chats
            logger.info("Checking which dialogs needs archiving")
            self.current_targets = []
            for dialog in dialogs:
                dialog_behaviour = self.chat_settings.behaviour_for_chat(dialog.resource_id, fallback_behaviour)
                do_archive_chat = self.chat_settings.should_archive_chat(dialog.resource_id)
                if not do_archive_chat:
                    continue
                archive_behaviour = BehaviourConfig.merge(BehaviourConfig(follow_live=False), dialog_behaviour)
                if archive_behaviour.needs_archive_run():
                    logger.info("Archiving dialog %s \"%s\"", dialog.resource_id, dialog.name)
                    archive_target = ArchiveTarget(dialog, archive_behaviour, self)
                    self.current_targets.append(archive_target)
            # Archiving targets
            logger.info("Archiving dialogs")
            for archive_target in self.current_targets:
                logger.info("Archiving chat: %s \"%s\"", dialog.resource_id, dialog.name)
                await archive_target.archive_chat()
            logger.info("Completed archiving all targets")

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
