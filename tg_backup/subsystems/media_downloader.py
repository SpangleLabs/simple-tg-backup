import asyncio
import dataclasses
import logging
import os
import pathlib
from typing import Optional, Union

import telethon
import typing_extensions
from prometheus_client import Counter, Summary, Histogram, Gauge
from telethon import TelegramClient
from telethon.errors import FileReferenceExpiredError
from telethon.tl.types import DocumentAttributeFilename, MessageMediaPhoto, MessageMediaDocument, MessageMediaWebPage, \
    MessageMediaGeo, MessageMediaGeoLive, MessageMediaPoll, MessageMediaDice, MessageMediaContact, MessageMediaToDo, \
    MessageMediaGiveaway, MessageMediaGiveawayResults, MessageMediaPaidMedia, MessageMediaStory, MessageMediaInvoice, \
    MessageMediaVenue, MessageMediaGame

from tg_backup.database.chat_database import ChatDatabase
from tg_backup.models.subsystem_queue_entry import MediaDownloaderQueueEntry
from tg_backup.models.web_page_media import WebPageMedia
from tg_backup.subsystems.abstract_subsystem import AbstractTargetQueuedSubsystem, ArchiveRunQueue
from tg_backup.subsystems.media_msg_refresh_cache import MessageRefreshCache, MessageMissingAfterRefresh

if typing_extensions.TYPE_CHECKING:
    from tg_backup.archiver import Archiver
    from tg_backup.archive_target import ArchiveTarget


logger = logging.getLogger(__name__)

media_processed_count = Counter(
    "tgbackup_mediadownloader_media_processed_count",
    "Total number of media files which have been picked from the queue by the MediaDownloader",
)
media_downloaded_count = Counter(
    "tgbackup_mediadownloader_media_downloaded_count",
    "Total number of media files which have been downloaded by the MediaDownloader",
)
file_reference_expired_count = Counter(
    "tgbackup_mediadownloader_file_reference_expired_count",
    "Total number of times media tried to download, but the file reference had expired",
)
time_waiting_for_refresh = Summary(
    "tgbackup_mediadownloader_time_waiting_for_message_refresh_seconds",
    "Total amount of time taken waiting for messages to be refreshed for expired file references",
)
refreshed_message_missing_media = Counter(
    "tgbackup_mediadownloader_refreshed_message_missing_media_count",
    "Total number of times where a message was refreshed, but then no longer contained the requested media",
)
media_download_failures = Counter(
    "tgbackup_mediadownloader_media_download_failure_count",
    "Number of exceptions raised when a media download fails to complete. (Excluding file reference expiration)",
)
total_media_download_attempts = Counter(
    "tgbackup_mediadownloader_total_media_download_attempts_count",
    "Total number of media download attempts in the MediaDownloader",
)
media_download_attempts_required = Histogram(
    "tgbackup_mediadownloader_download_attempts_required",
    "Number of attempts required to complete a media download",
    buckets=[1, 2, 3, 5, 10],
)
processed_media_id_cache_size = Gauge(
    "tgbackup_mediadownloader_processed_media_id_cache_size",
    "Number of media IDs in the MediaDownloader processed media cache",
)
processed_media_id_cache_rejections = Counter(
    "tgbackup_mediadownloader_processed_media_id_cache_rejections_count",
    "Number of media queue requests which were rejected due to already having been processed",
)
queued_media_id_cache_size = Gauge(
    "tgbackup_mediadownloader_queued_media_id_cache_size",
    "Number of media IDs in the MediaDownloader queued media cache",
)
queued_media_id_cache_rejections = Counter(
    "tgbackup_mediadownloader_queued_media_id_cache_rejections_count",
    "Number of media queue requests which were rejected due to already having been queued",
)
parsed_media_type_count = Counter(
    "tgbackup_mediadownloader_parsed_media_type_count",
    "Number of times each type of media has been parsed from observed messages",
    labelnames=["media_type"],
)
for media_type in ["no_media", "photo_expired", "photo", "document_expired", "document", "web_page_missing", "web_page", "data_only", "to_do", "ignored", "unknown"]:
    parsed_media_type_count.labels(media_type=media_type)
parsed_media_per_web_page = Histogram(
    "tgbackup_mediadownloader_parsed_media_per_web_page_count",
    "Number of media files found within a web page preview",
    buckets=[0, 1, 2, 5, 10, 20, 50],
)


class RefreshedMessageMissingMedia(Exception):
    def __init__(self, refreshed_message: telethon.types.Message):
        super().__init__()
        self.refreshed_message = refreshed_message


@dataclasses.dataclass
class MediaInfo:
    """
    Attributes:
        media_subfolder: Which subfolder within the chat's folder, to write the media file into.
        media_type: A vaguely-human-readable description of the media type, for logging and such.
        media_id: Unique ID of the media object, will be used as the filename.
        file_ext: Detected file extension of the media file.
        media_obj: The message or media object to pass to Telethon's download_media() method. Prefer passing the Message
        itself, if it's simple enough to be able to use that. In more niche types of media downloads, you may need to
        pass a raw Photo or Document.
    """
    media_subfolder: str
    media_type: str
    media_id: int
    file_ext: str
    media_obj: Union[telethon.types.Message, telethon.tl.types.Photo, telethon.tl.types.Document]
    web_page_media: Optional[WebPageMedia] = None


@dataclasses.dataclass
class MediaQueueInfo:
    chat_id: int
    chat_db: ChatDatabase
    archive_target: "ArchiveTarget"


@dataclasses.dataclass
class MediaQueueEntry:
    message: Optional[telethon.types.Message]
    media_info: Optional[MediaInfo]
    _storable_entry: Optional[MediaDownloaderQueueEntry] = None

    @property
    def message_id(self) -> int:
        if self.message is None:
            return self.storable_entry.message_id
        return self.message.id

    @property
    def media_id(self):
        if self.media_info is None:
            return self.storable_entry.media_id
        return self.media_info.media_id

    @property
    def storable_entry(self) -> MediaDownloaderQueueEntry:
        if self._storable_entry is None:
            self._storable_entry = MediaDownloaderQueueEntry(
                None,
                self.message.id,
                self.media_info.media_id,
            )
        return self._storable_entry


class MediaDownloader(AbstractTargetQueuedSubsystem[MediaQueueInfo, MediaQueueEntry]):
    MEDIA_NO_ACTION_NEEDED = [MessageMediaGeo, MessageMediaGeoLive, MessageMediaDice, MessageMediaToDo]
    MEDIA_TO_DO = [MessageMediaPoll, MessageMediaContact]
    MEDIA_IGNORE = [MessageMediaGiveaway, MessageMediaGiveawayResults, MessageMediaPaidMedia, MessageMediaStory, MessageMediaGame, MessageMediaInvoice, MessageMediaVenue]
    UNKNOWN_FILE_EXT = "unknown_filetype"
    MEDIA_FOLDER = "media"
    WEB_PAGE_MEDIA_FOLDER = "web_page_media"

    def __init__(self, archiver: "Archiver", client: TelegramClient, message_refresher: MessageRefreshCache) -> None:
        super().__init__(archiver, client)
        self.message_refresher = message_refresher
        self._chat_processed_media_id_cache: dict[int, set[int]] = {}
        self._chat_queued_media_id_cache: dict[int, set[int]] = {} # Cache of which media IDs have been queued for each chat
        processed_media_id_cache_size.set_function(lambda: sum(len(s) for s in self._chat_processed_media_id_cache.values()))
        queued_media_id_cache_size.set_function(lambda: sum(len(s) for s in self._chat_queued_media_id_cache.values()))

    def _processed_cache_has_media_id(self, chat_id: int, media_id: int) -> bool:
        return media_id in self._chat_processed_media_id_cache.get(chat_id, set())

    def _add_media_id_to_processed_cache(self, chat_id: int, media_id: int) -> None:
        if chat_id not in self._chat_processed_media_id_cache:
            self._chat_processed_media_id_cache[chat_id] = set()
        self._chat_processed_media_id_cache[chat_id].add(media_id)

    def _queued_cache_has_media_id(self, chat_id: int, media_id: int) -> bool:
        return media_id in self._chat_queued_media_id_cache.get(chat_id, set())

    def _add_media_id_to_queued_cache(self, chat_id: int, media_id: int) -> None:
        if chat_id not in self._chat_queued_media_id_cache:
            self._chat_queued_media_id_cache[chat_id] = set()
        self._chat_queued_media_id_cache[chat_id].add(media_id)

    def _parse_media_info(self, msg: telethon.types.Message, chat_id: int) -> list[MediaInfo]:
        # Skip if not media
        if not hasattr(msg, "media"):
            parsed_media_type_count.labels(media_type="no_media").inc()
            return []
        # Start checking media type
        media_ext = self.UNKNOWN_FILE_EXT
        media_type = type(msg.media)
        media_type_name = media_type.__name__
        if isinstance(msg.media, MessageMediaPhoto):
            if msg.media.photo is None:
                logger.info("This timed photo has expired, cannot archive")
                parsed_media_type_count.labels(media_type="photo_expired").inc()
                return []
            media_id = msg.media.photo.id
            media_ext = "jpg"
            parsed_media_type_count.labels(media_type="photo").inc()
            return [MediaInfo(self.MEDIA_FOLDER, media_type_name, media_id, media_ext, msg)]
        if isinstance(msg.media, MessageMediaDocument):
            if msg.media.document is None:
                logger.info("This timed document has expired, cannot archive")
                parsed_media_type_count.labels(media_type="document_expired").inc()
                return []
            media_id = msg.media.document.id
            media_ext = self._document_file_ext(msg.media.document) or media_ext
            parsed_media_type_count.labels(media_type="document").inc()
            return [MediaInfo(self.MEDIA_FOLDER, media_type_name, media_id, media_ext, msg)]
        if isinstance(msg.media, MessageMediaWebPage):
            if not isinstance(msg.media.webpage, telethon.tl.types.WebPage):
                logger.warning(
                    "This MessageMediaWebPage is missing the web page? chat_id %s, msg_id %s, date %s",
                    chat_id, getattr(msg, "id", None), getattr(msg, "date", None)
                )
                parsed_media_type_count.labels(media_type="web_page_missing").inc()
                return []
            parsed_media_type_count.labels(media_type="web_page").inc()
            return self._parse_media_from_web_page(msg.media.webpage)
        if media_type in self.MEDIA_NO_ACTION_NEEDED:
            logger.info("No action needed for data-only media type: %s", media_type_name)
            parsed_media_type_count.labels(media_type="data_only").inc()
            return []
        if media_type in self.MEDIA_TO_DO:
            logger.info(
                "Media type not yet implemented: %s, chat ID: %s, msg ID: %s, date %s",
                media_type_name, chat_id, getattr(msg, "id", None), getattr(msg, "date", None)
            ) # TODO: Implement these!
            parsed_media_type_count.labels(media_type="to_do").inc()
            return []
        if media_type in self.MEDIA_IGNORE:
            logger.info(
                "Media type ignored: %s, chat ID: %s, msg ID: %s, date %s",
                media_type_name, chat_id, getattr(msg, "id", None), getattr(msg, "date", None)
            )
            parsed_media_type_count.labels(media_type="ignored").inc()
            return []
        logger.warning(
            "Unknown media type! %s, chat ID: %s, msg ID: %s, date %s",
            media_type_name, chat_id, getattr(msg, "id", None), getattr(msg, "date", None)
        )
        parsed_media_type_count.labels(media_type="unknown").inc()
        return []

    @staticmethod
    def _document_file_ext(doc: telethon.tl.types.Document) -> Optional[str]:
        for attr in doc.attributes:
            if type(attr) == DocumentAttributeFilename:
                return attr.file_name.split(".")[-1]

    def _parse_media_from_web_page(self, web_page: telethon.tl.types.WebPage) -> list[MediaInfo]:
        web_page_id = web_page.id
        folder = self.WEB_PAGE_MEDIA_FOLDER
        photo_type = "MessageMediaWebPage.Photo"
        doc_type = "MessageMediaWebPage.Document"
        media_entries: list[MediaInfo] = []
        # Add the preview photo and document
        if web_page.photo is not None:
            media_id = web_page.photo.id
            file_ext = "jpg"
            web_page_entry = WebPageMedia.from_web_page(web_page_id, media_id, "$.photo")
            media_info = MediaInfo(folder, photo_type, media_id, file_ext, web_page.photo, web_page_entry)
            media_entries.append(media_info)
        if web_page.document is not None:
            media_id = web_page.document.id
            file_ext = self._document_file_ext(web_page.document) or self.UNKNOWN_FILE_EXT
            web_page_entry = WebPageMedia.from_web_page(web_page_id, media_id, "$.document")
            media_info = MediaInfo(folder, doc_type, media_id, file_ext, web_page.document, web_page_entry)
            media_entries.append(media_info)
        # Add in photos and documents inside instant view pages
        if web_page.cached_page is not None:
            for i, cached_photo in enumerate(web_page.cached_page.photos or []):
                media_id = cached_photo.id
                file_ext = "jpg"
                json_path = f"$.cached_page.photos[{i}]"
                web_media_entry = WebPageMedia.from_web_page(web_page_id, media_id, json_path)
                media_info = MediaInfo(folder, photo_type, media_id, file_ext, cached_photo, web_media_entry)
                media_entries.append(media_info)
            for i, cached_document in enumerate(web_page.cached_page.documents or []):
                media_id = cached_document.id
                file_ext = self._document_file_ext(cached_document) or self.UNKNOWN_FILE_EXT
                json_path = f"$.cached_page.documents[{i}]"
                web_media_entry = WebPageMedia.from_web_page(web_page_id, media_id, json_path)
                media_info = MediaInfo(folder, doc_type, media_id, file_ext, cached_document, web_media_entry)
                media_entries.append(media_info)
        parsed_media_per_web_page.observe(len(media_entries))
        return media_entries

    async def refresh_message_media(
            self,
            message: Optional[telethon.tl.types.Message],
            message_id: int,
            media_id: int,
            queue_info: MediaQueueInfo,
    ) -> tuple[telethon.tl.types.Message, MediaInfo]:
        logger.info("Refreshing message ID %s for media ID %s", message_id, media_id)
        with time_waiting_for_refresh.time():
            new_message = await self.message_refresher.get_message(queue_info.chat_id, message_id, message, queue_info.archive_target)
        logger.info("Fetched new message for message ID %s", message_id)
        media_info_entries = self._parse_media_info(new_message, queue_info.chat_id)
        media_info_matches = [m for m in media_info_entries if m.media_id == media_id]
        if media_info_matches:
            return new_message, media_info_matches[0]
        else:
            logger.warning("Could not find media after message refresh")
            refreshed_message_missing_media.inc()
            raise RefreshedMessageMissingMedia(new_message)

    async def _do_process(self) -> None:
        chat_queue, queue_entry = self._get_next_in_queue()
        chat_id = chat_queue.info.chat_id
        chat_db = chat_queue.info.chat_db
        archive_target = chat_queue.info.archive_target
        message = queue_entry.message
        media_info = queue_entry.media_info
        # If this queue entry came from storage, refresh the message it refers to
        storable_entry = queue_entry.storable_entry
        if message is None or media_info is None:
            if storable_entry.message_id is None or storable_entry.media_id is None:
                logger.info("Media got queued without having message ID or media ID")
                chat_db.delete_subsystem_queue_entry(storable_entry.queue_entry_id)
                chat_queue.queue.task_done()
                return
            logger.info("Processing stored queue entry. Refreshing message ID %s", storable_entry.message_id)
            try:
                message, media_info = await self.refresh_message_media(None, storable_entry.message_id, storable_entry.media_id, chat_queue.info)
            except RefreshedMessageMissingMedia as e:
                logger.warning("Refreshed message ID %s from storage is missing searched media, will queue all media in the refreshed message to be processed", storable_entry.message_id)
                new_message = e.refreshed_message
                media_info_entries = self._parse_media_info(new_message, chat_id)
                for media_info in media_info_entries:
                    queue_entry = MediaQueueEntry(new_message, media_info)
                    await self._add_media_queue_entry(chat_queue.queue_key, chat_queue.info, queue_entry)
                chat_db.delete_subsystem_queue_entry(storable_entry.queue_entry_id)
                chat_queue.queue.task_done()
                return
            except MessageMissingAfterRefresh as e:
                logger.warning("Message ID %s was not found after refresh run, marking media as complete", storable_entry.message_id)
                chat_db.delete_subsystem_queue_entry(storable_entry.queue_entry_id)
                chat_queue.queue.task_done()
                return
            queue_entry.message = message
            queue_entry.media_info = media_info
        media_processed_count.inc()
        # Process the media info
        try:
            await self._process_media(chat_id, chat_db, message, media_info, archive_target)
        except RefreshedMessageMissingMedia as e:
            logger.warning("Refreshed message ID %s is missing searched media, will process all media in refreshed message", message.id)
            # New message should be grabbed already, so this should return fast
            new_message = e.refreshed_message
            media_info_entries = self._parse_media_info(new_message, chat_id)
            for media_info in media_info_entries:
                await self._process_media(chat_id, chat_db, new_message, media_info, archive_target)
        # Mark task as done
        chat_db.delete_subsystem_queue_entry(queue_entry.storable_entry.queue_entry_id)
        chat_queue.queue.task_done()

    async def _process_media(
            self,
            chat_id: int,
            chat_db: ChatDatabase,
            message: telethon.types.Message,
            media_info: MediaInfo,
            archive_target: "ArchiveTarget"
    ) -> None:
        # Skip if already in cache
        if self._processed_cache_has_media_id(chat_id, media_info.media_id):
            return
        # Construct file path
        target_filename = f"{media_info.media_id}.{media_info.file_ext}"
        target_path = pathlib.Path("store") / "chats" / f"{chat_id}" / media_info.media_subfolder / target_filename
        os.makedirs(target_path.parent, exist_ok=True)
        if os.path.exists(target_path):
            logger.info("Skipping download of pre-existing file")
            return
        # Download the media
        attempt_count = 0
        download_success = False
        while attempt_count <= 10:
            attempt_count += 1
            logger.info("Downloading media, type: %s, ID: %s", media_info.media_type, media_info.media_id)
            total_media_download_attempts.inc()
            try:
                await self.client.download_media(media_info.media_obj, str(target_path))
            except FileReferenceExpiredError as e:
                logger.warning("File reference expired for message ID %s, will refresh message", message.id)
                file_reference_expired_count.inc()
                try:
                    with time_waiting_for_refresh.time():
                        message = await self.message_refresher.get_message(chat_id, message.id, message, archive_target)
                    logger.info("Fetched new message for message ID %s", message.id)
                except MessageMissingAfterRefresh:
                    logger.warning("Message ID %s was not found after message refresh, could not download media", message.id)
                    return
                media_info_entries = self._parse_media_info(message, chat_id)
                media_info_matches = [m for m in media_info_entries if m.media_id == media_info.media_id]
                if media_info_matches:
                    media_info = media_info_matches[0]
                else:
                    logger.warning("Could not find media after message refresh")
                    refreshed_message_missing_media.inc()
                    raise RefreshedMessageMissingMedia(message)
            except Exception as e:
                logger.error("Failed to download media from message ID %s (chat ID %s, date %s), (will retry) error:", message.id, chat_id, getattr(message, "date", None), exc_info=e)
                media_download_failures.inc()
                await asyncio.sleep(60)
            else:
                download_success = True
                break
        if not download_success:
            os.unlink(target_path)
            logger.error("Could not download file after 10 attempts. Skipping. Media ID %s, Message ID %s, chat ID %s, date %s", media_info.media_id, message.id, chat_id, getattr(message, "date", None))
            return
        media_downloaded_count.inc()
        media_download_attempts_required.observe(attempt_count)
        logger.info("Media download complete, type: %s, ID: %s", media_info.media_type, media_info.media_id)
        # Mark media in cache
        self._add_media_id_to_processed_cache(chat_id, media_info.media_id)
        # Save web page media to DB, if appropriate
        web_page_media = media_info.web_page_media
        if web_page_media is not None and chat_db is not None:
            chat_db.save_web_page_media(web_page_media)

    async def wait_until_queue_empty(self, queue_key: Optional[str]) -> None:
        return await self._wait_for_queue_and_message_refresher(queue_key, self.message_refresher)

    async def queue_media(
            self,
            queue_key: str,
            chat_id: int,
            chat_db: ChatDatabase,
            message: telethon.types.Message,
            archive_target: "ArchiveTarget",
    ) -> None:
        info = MediaQueueInfo(chat_id, chat_db, archive_target)
        # Determine media info entries
        media_info_entries = self._parse_media_info(message, chat_id)
        if not media_info_entries:
            return
        logger.info(
            "Found %s media entries in message ID %s chat ID %s",
            len(media_info_entries), message.id, chat_id
        )
        # Queue up each of the media info entries
        for media_info in media_info_entries:
            queue_entry = MediaQueueEntry(message, media_info)
            await self._add_media_queue_entry(queue_key, info, queue_entry)

    async def _add_media_queue_entry(self, queue_key: str, info: MediaQueueInfo, queue_entry: MediaQueueEntry) -> None:
        if queue_entry.message_id is None or queue_entry.media_id is None:
            logger.debug("Refusing to queue media download request for None")
            return
        if self._processed_cache_has_media_id(info.chat_id, queue_entry.media_id):
            logger.debug("Media ID %s has already been downloaded", queue_entry.media_id)
            processed_media_id_cache_rejections.inc()
            return
        if self._queued_cache_has_media_id(info.chat_id, queue_entry.media_id):
            logger.debug("Media ID %s has already been queued", queue_entry.media_id)
            queued_media_id_cache_rejections.inc()
            return
        await self._add_queue_entry(queue_key, info, queue_entry)
        info.chat_db.save_subsystem_queue_entry(queue_entry.storable_entry)
        self._add_media_id_to_queued_cache(info.chat_id, queue_entry.media_id)

    async def initialise_new_queue(self, new_queue: ArchiveRunQueue[MediaQueueInfo, MediaQueueEntry]) -> None:
        stored_queue_entries = new_queue.info.chat_db.list_subsystem_queue_entries(MediaDownloaderQueueEntry.SUBSYSTEM_NAME)
        logger.info("Loading %s MediaDownloader queue entries from chat database for chat ID %s", len(stored_queue_entries), new_queue.info.chat_id)
        for stored_entry in stored_queue_entries:
            stored_media_entry = MediaDownloaderQueueEntry.from_generic(stored_entry)
            queue_entry = MediaQueueEntry(None, None, stored_media_entry)
            await self._add_media_queue_entry(new_queue.queue_key, new_queue.info, queue_entry)
