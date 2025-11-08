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
    target_watcher: Optional[MultiTargetWatcher]
    history_targets: list[ArchiveTarget]
    start_time: datetime.datetime = dataclasses.field(default_factory=lambda: datetime.datetime.now(datetime.timezone.utc))
    completed_history_targets = False
    history_targets_added = asyncio.Event()
    completed: bool = False
    watch_task: Optional[asyncio.Task] = None
    watcher: Optional[MultiTargetWatcher] = None

    def count_all_targets(self) -> int:
        return self.watcher.count_watched_targets() + len(self.history_targets)

    def has_history_targets(self) -> bool:
        return len(self.history_targets) > 0

    def add_history_target(self, target: ArchiveTarget) -> None:
        self.history_targets.append(target)
        self.history_targets_added.set()
        self.history_targets_added.clear()

    def update_history_targets(self, targets: list[ArchiveTarget]) -> None:
        self.history_targets = targets
        self.history_targets_added.set()
        self.history_targets_added.clear()

    def list_unfinished_history_targets(self) -> list[ArchiveTarget]:
        return [t for t in self.history_targets if not t.run_record.archive_history_timer.has_ended()]

    def next_unfinished_history_target(self) -> Optional[ArchiveTarget]:
        unfinished_targets = self.list_unfinished_history_targets()
        if unfinished_targets:
            return unfinished_targets[0]
        return None

    def has_unfinished_history_targets(self) -> bool:
        return len(self.list_unfinished_history_targets()) > 0

    def count_completed_history_targets(self) -> int:
        return len([t for t in self.history_targets if t.run_record.archive_history_timer.has_ended()])


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
        archiver_current_targets.set_function(lambda: self.current_activity.count_all_targets() if self.current_activity else 0)
        archiver_completed_targets.set_function(lambda: self.current_activity.count_completed_history_targets() if self.current_activity else 0)

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
                await self.peer_fetcher.queue_peer(None, None, None, peer)
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
        # Set up the multi target watcher
        target_watcher = MultiTargetWatcher.from_dialogs(self.client, self, self.chat_settings, dialogs)
        # List the targets to archive history for
        archive_history_dialogs = self.chat_settings.list_needs_archive_run(dialogs, fallback_behaviour)
        override_follow = BehaviourConfig(follow_live=False)
        archive_history_targets = self.dialogs_to_archive_targets(archive_history_dialogs, override_follow)
        # Set up the archiver activity
        activity = ArchiverActivity("Running archive", target_watcher, archive_history_targets)
        self.current_activity = activity
        async with self.run():
            # Watch for new messages from applicable chats
            if not target_watcher.watching_nothing():
                logger.info("Following %s dialogs live", target_watcher.count_watched_targets())
                activity.watch_task = asyncio.create_task(activity.watcher.watch())
            # Loop until we're complete
            while not activity.completed:
                # Archive the history of any applicable chats
                if activity.has_unfinished_history_targets():
                    logger.info("Archiving dialogs")
                    next_target = activity.next_unfinished_history_target()
                    while next_target is not None:
                        dialog = next_target.dialog
                        logger.info("Archiving dialog %s \"%s\"", dialog.resource_id, dialog.name)
                        await next_target.archive_chat()
                        next_target = activity.next_unfinished_history_target()
                    activity.completed_history_targets = True
                    logger.info("Completed archiving chat history of all targets")
                # If there's no watching to do, break the loop
                if activity.watch_task is None:
                    logger.info("History archival done, and not following any chats, shutting down archiver.")
                    activity.complete = True
                    break
                # Otherwise, watch the watcher until new history targets appear
                logger.info("Watching dialogs for live updates")
                done, pending = await asyncio.wait(
                    [
                        activity.watch_task,
                        activity.history_targets_added.wait(),
                    ],
                    return_when=asyncio.FIRST_COMPLETED,
                )
                if not activity.has_unfinished_history_targets() and activity.watch_task.done():
                    logger.info("Archive targets complete, and chat watcher is complete, shutting down archiver.")
                    activity.complete = True
                    break

    async def archive_chat(self, chat_id: int, archive_behaviour: BehaviourConfig) -> None:
        activity = ArchiverActivity(f"Archiving individual chat: {chat_id}", None, [])
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
            activity.update_history_targets([target])
            await target.archive_chat()

    def add_archive_history_target_while_running(self, history_target: ArchiveTarget) -> None:
        activity = self.current_activity
        if activity is None:
            raise ValueError("Not currently running, can't add a history target")
        if activity.completed:
            raise ValueError("Archiver has already finished, can't add a history target")
        # I don't know why I split this up with if statements, but it felt right, so, log lines I guess.
        if activity.has_history_targets():
            if activity.completed_history_targets:
                logger.info("History archival complete, adding a new target (which should resume history archival)")
                activity.add_history_target(history_target)
                return
            logger.info("History archival is active, added a new target to history archive list")
            activity.add_history_target(history_target)
            return
        logger.info("History archival was not previously needed, added a new target (which should trigger history archival)")
        activity.add_history_target(history_target)
        return
