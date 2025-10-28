import dataclasses
import logging
import os
import pathlib
from typing import Optional, Union

import telethon
from prometheus_client import Counter
from telethon import TelegramClient
from telethon.tl.types import DocumentAttributeFilename, MessageMediaPhoto, MessageMediaDocument, MessageMediaWebPage, \
    MessageMediaGeo, MessageMediaGeoLive, MessageMediaPoll, MessageMediaDice, MessageMediaContact, MessageMediaToDo, \
    MessageMediaGiveaway, MessageMediaGiveawayResults, MessageMediaPaidMedia, MessageMediaStory, MessageMediaInvoice, \
    MessageMediaVenue, MessageMediaGame

from tg_backup.database.chat_database import ChatDatabase
from tg_backup.models.web_page_media import WebPageMedia
from tg_backup.subsystems.abstract_subsystem import AbstractTargetQueuedSubsystem

logger = logging.getLogger(__name__)

media_processed_count = Counter(
    "tgbackup_mediadownloader_media_processed_count",
    "Total number of media-containing messages which have been picked from the queue by the MediaDownloader",
)
media_downloaded_count = Counter(
    "tgbackup_mediadownloader_media_downloaded_count",
    "Total number of media files which have been downloaded by the MediaDownloader",
)


@dataclasses.dataclass
class MediaQueueEntry:
    message: telethon.types.Message


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


class MediaDownloader(AbstractTargetQueuedSubsystem[MediaQueueEntry]):
    MEDIA_NO_ACTION_NEEDED = [MessageMediaGeo, MessageMediaGeoLive, MessageMediaDice, MessageMediaToDo]
    MEDIA_TO_DO = [MessageMediaPoll, MessageMediaContact]
    MEDIA_IGNORE = [MessageMediaGiveaway, MessageMediaGiveawayResults, MessageMediaPaidMedia, MessageMediaStory, MessageMediaGame, MessageMediaInvoice, MessageMediaVenue]
    UNKNOWN_FILE_EXT = "unknown_filetype"
    MEDIA_FOLDER = "media"
    WEB_PAGE_MEDIA_FOLDER = "web_page_media"

    def __init__(self, client: TelegramClient) -> None:
        super().__init__(client)
        self.chat_seen_web_page_ids: dict[int, set[int]] = {}


    def _parse_media_info(self, msg: telethon.types.Message, chat_id: int) -> list[MediaInfo]:
        # Skip if not media
        if not hasattr(msg, "media"):
            return []
        # Start checking media type
        media_ext = self.UNKNOWN_FILE_EXT
        if not hasattr(msg, "media"):
            return []
        media_type = type(msg.media)
        media_type_name = media_type.__name__
        if isinstance(msg.media, MessageMediaPhoto):
            if msg.media.photo is None:
                logger.info("This timed photo has expired, cannot archive")
                return []
            media_id = msg.media.photo.id
            media_ext = "jpg"
            return [MediaInfo(self.MEDIA_FOLDER, media_type_name, media_id, media_ext, msg)]
        if isinstance(msg.media, MessageMediaDocument):
            if msg.media.document is None:
                logger.info("This timed document has expired, cannot archive")
                return []
            media_id = msg.media.document.id
            media_ext = self._document_file_ext(msg.media.document) or media_ext
            return [MediaInfo(self.MEDIA_FOLDER, media_type_name, media_id, media_ext, msg)]
        if isinstance(msg.media, MessageMediaWebPage):
            if not isinstance(msg.media.webpage, telethon.tl.types.WebPage):
                logger.warning(
                    "This MessageMediaWebPage is missing the web page? chat_id %s, msg_id %s, date %s",
                    chat_id, getattr(msg, "id", None), getattr(msg, "date", None)
                )
                return []
            return self._parse_media_from_web_page(msg.media.webpage)
        if media_type in self.MEDIA_NO_ACTION_NEEDED:
            logger.info("No action needed for data-only media type: %s", media_type_name)
            return []
        if media_type in self.MEDIA_TO_DO:
            logger.info(
                "Media type not yet implemented: %s, chat ID: %s, msg ID: %s, date %s",
                media_type_name, chat_id, getattr(msg, "id", None), getattr(msg, "date", None)
            ) # TODO: Implement these!
            return []
        if media_type in self.MEDIA_IGNORE:
            logger.info(
                "Media type ignored: %s, chat ID: %s, msg ID: %s, date %s",
                media_type_name, chat_id, getattr(msg, "id", None), getattr(msg, "date", None)
            )
            return []
        logger.warning(
            "Unknown media type! %s, chat ID: %s, msg ID: %s, date %s",
            media_type_name, chat_id, getattr(msg, "id", None), getattr(msg, "date", None)
        )
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
        return media_entries

    async def _do_process(self) -> None:
        chat_queue, queue_entry = self._get_next_in_queue()
        chat_id = chat_queue.chat_id
        media_processed_count.inc()
        # Determine media info
        media_info_entries = self._parse_media_info(queue_entry.message, chat_id)
        if not media_info_entries:
            return
        logger.info(
            "Found %s media entries in message ID % chat ID %s",
            len(media_info_entries), queue_entry.message.id, chat_id
        )
        for media_info in media_info_entries:
            # Construct file path
            target_filename = f"{media_info.media_id}.{media_info.file_ext}"
            target_path = pathlib.Path("store") / "chats" / f"{chat_id}" / media_info.media_subfolder / target_filename
            os.makedirs(target_path.parent, exist_ok=True)
            if os.path.exists(target_path):
                logger.info("Skipping download of pre-existing file")
                return
            # Download the media
            logger.info("Downloading media, type: %s, ID: %s", media_info.media_type, media_info.media_id)
            await self.client.download_media(media_info.media_obj, str(target_path))
            media_downloaded_count.inc()
            logger.info("Media download complete, type: %s, ID: %s", media_info.media_type, media_info.media_id)
            # Save web page media to DB, if appropriate
            chat_db = chat_queue.chat_db
            web_page_media = media_info.web_page_media
            if web_page_media is not None and chat_db is not None:
                chat_db.save_web_page_media(web_page_media)

    async def queue_media(
            self,
            queue_key: str,
            chat_id: int,
            chat_db: ChatDatabase,
            message: telethon.types.Message,
    ) -> None:
        entry = MediaQueueEntry(message)
        await self._add_queue_entry(queue_key, chat_id, chat_db, entry)
