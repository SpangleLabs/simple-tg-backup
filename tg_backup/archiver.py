import asyncio
import dataclasses
import datetime
import logging
from contextlib import asynccontextmanager
from typing import AsyncGenerator, Optional

from prometheus_client import Gauge
from telethon import TelegramClient

from tg_backup.archive_target import ArchiveTarget
from tg_backup.chat_settings_store import ChatSettingsStore
from tg_backup.config import Config, BehaviourConfig
from tg_backup.database.core_database import CoreDatabase
from tg_backup.models.dialog import Dialog
from tg_backup.multi_target_watcher import MultiTargetWatcher
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


@dataclasses.dataclass
class ArchiverActivity:
    name: str
    follow_targets: list[ArchiveTarget]
    archive_targets: list[ArchiveTarget]
    start_time: datetime.datetime = dataclasses.field(default_factory=lambda: datetime.datetime.now(datetime.timezone.utc))
    completed: bool = False

    @classmethod
    def from_targets(cls, name: str, targets: list[ArchiveTarget]) -> "ArchiverActivity":
        return cls(
            name=name,
            follow_targets=[t for t in targets if t.behaviour.follow_live == True],
            archive_targets=[t for t in targets if t.behaviour.follow_live == False]
        )

    def all_targets(self) -> list[ArchiveTarget]:
        return self.follow_targets + self.archive_targets

    def update_targets(self, targets: list[ArchiveTarget]) -> None:
        self.follow_targets = [t for t in targets if t.behaviour.follow_live == True]
        self.archive_targets = [t for t in targets if t.behaviour.follow_live == False]

    def completed_archive_targets(self) -> list[ArchiveTarget]:
        return [t for t in self.archive_targets if t.run_record.archive_history_timer.has_ended()]


class Archiver:
    def __init__(self, conf: Config) -> None:
        self.config = conf
        self.client = TelegramClient("simple_backup", conf.client.api_id, conf.client.api_hash)
        self.running = False
        self.running_list_dialogs = False
        self.current_activity: Optional[ArchiverActivity] = None
        self.core_db = CoreDatabase()
        self.media_dl = MediaDownloader(self.client)
        self.peer_fetcher = PeerDataFetcher(self.client, self.core_db)
        self.sticker_downloader = StickerDownloader(self.client, self.core_db)
        self.chat_settings = ChatSettingsStore.load_from_file()
        archiver_running.set_function(lambda: int(self.running))
        archiver_current_targets.set_function(lambda: len(self.current_activity.all_targets()) if self.current_activity else 0)
        archiver_completed_targets.set_function(lambda: len(self.current_activity.completed_archive_targets()) if self.current_activity else 0)

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

    def dialogs_to_archive_targets(
            self,
            dialogs: list[Dialog],
            override_behaviour: Optional[BehaviourConfig] = None,
    ) -> list[ArchiveTarget]:
        targets = []
        fallback_behaviour = self.config.default_behaviour
        behaviours = self.chat_settings.behaviour_for_dialogs(dialogs, fallback_behaviour, override_behaviour)
        for dialog in dialogs:
            behaviour = behaviours[dialog.resource_id]
            targets.append(ArchiveTarget(dialog, behaviour, self))
        return targets

    async def run_archive(self, dialogs: list[Dialog]) -> None:
        """
        Runs the archiver with the ChatSettingsStore settings
        """
        fallback_behaviour = self.config.default_behaviour
        follow_dialogs = self.chat_settings.list_follow_live(dialogs, fallback_behaviour)
        follow_targets = self.dialogs_to_archive_targets(follow_dialogs)
        archive_history_dialogs = self.chat_settings.list_needs_archive_run(dialogs, fallback_behaviour)
        override_follow = BehaviourConfig(follow_live=False)
        archive_history_targets = self.dialogs_to_archive_targets(archive_history_dialogs, override_follow)
        activity = ArchiverActivity("Running archive", follow_targets, archive_history_targets)
        self.current_activity = activity
        async with self.run():
            # Watch for new messages from applicable chats
            watch_task: Optional[asyncio.Task] = None
            if follow_targets:
                logger.info("Following %s dialogs live", len(follow_targets))
                multi_follow = await MultiTargetWatcher.from_targets(self.client, follow_targets)
                watch_task = asyncio.create_task(multi_follow.watch())
            # Archive the history of applicable chats
            logger.info("Archiving dialogs")
            for archive_target in archive_history_targets:
                dialog = archive_target.dialog
                logger.info("Archiving dialog %s \"%s\"", dialog.resource_id, dialog.name)
                await archive_target.archive_chat()
            logger.info("Completed archiving all targets")
            # Continue watching if relevant
            if watch_task:
                logger.info("Chat history archive complete, watching live updates")
                await watch_task
            activity.completed = True

    async def archive_chat(self, chat_id: int, archive_behaviour: BehaviourConfig) -> None:
        activity = ArchiverActivity(f"Archiving individual chat: {chat_id}", [], [])
        self.current_activity = activity
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
            activity.update_targets([target])
            await target.archive_chat()
