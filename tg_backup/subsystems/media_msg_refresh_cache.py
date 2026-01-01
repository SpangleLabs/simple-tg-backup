import asyncio
import dataclasses
import logging
import typing
from asyncio import QueueEmpty
from typing import Optional

import telethon
from prometheus_client import Counter, Gauge
from telethon import TelegramClient

if typing.TYPE_CHECKING:
    from tg_backup.archive_target import ArchiveTarget

logger = logging.getLogger(__name__)


count_refresh_requests = Counter(
    "tgbackup_messagerefresh_refresh_request_count",
    "Count of the number of message refresh requests that the MessageRefreshCache handles",
)
count_already_refreshed_msgs = Counter(
    "tgbackup_messagerefresh_refresh_already_exists_count",
    "Count of how many times a message requested for refresh, already exists refreshed in the cache",
)
count_refreshed_messages = Counter(
    "tgbackup_messagerefresh_messages_refreshed_count",
    "Count of the number of messages which were refreshed by the MessageRefreshCache",
)
size_message_refresh_cache = Gauge(
    "tgbackup_messagerefresh_total_cache_size",
    "Total number of messages in the message refresh cache",
)
refresh_task_active = Gauge(
    "tgbackup_messagerefresh_refresh_task_active",
    "Whether or not the message refresh task is active",
)
message_refresh_request_queue_size = Gauge(
    "tgbackup_messagerefresh_refresh_queue_size",
    "Number of refresh requests in the refresh request queue",
)
message_refresh_request_queue_empty = Gauge(
    "tgbackup_messagerefresh_refresh_queue_empty",
    "Whether the message refresh cache queue is considered to be empty",
)


@dataclasses.dataclass
class RefreshRequest:
    chat_id: int
    msg_id: int
    old_msg: telethon.types.Message
    archive_target: "ArchiveTarget"


class ChatMessageRefreshCache:
    def __init__(self, msg_cache: "MessageRefreshCache", chat_id: int) -> None:
        self.msg_cache = msg_cache
        self.chat_id = chat_id
        self._message_cache: dict[int, telethon.types.Message] = {}
        self._waiting_for_msg: dict[int, asyncio.Event] = {}

    def get_message_only(self, message_id: int) -> Optional[telethon.types.Message]:
        return self._message_cache.get(message_id, None)

    def add_message_to_cache(self, msg: telethon.types.Message) -> None:
        # Add message to cache
        self._message_cache[msg.id] = msg
        # Trigger any events for anything waiting on this message
        self.signal_message_updated(msg.id)

    def signal_message_updated(self, message_id: int) -> None:
        wait_event = self._waiting_for_msg.get(message_id, None)
        if wait_event is not None:
            logger.info("Signalling that message ID %s has been refreshed", message_id)
            wait_event.set()
            wait_event.clear()

    def wait_event_for_msg(self, message_id: int) -> asyncio.Event:
        wait_event = self._waiting_for_msg.get(message_id, None)
        if wait_event is not None:
            logger.info("Returning existing wait event for message ID %s", message_id)
            return wait_event
        logger.info("Creating new wait event for message ID %s", message_id)
        wait_event = asyncio.Event()
        self._waiting_for_msg[message_id] = wait_event
        return wait_event

    def size(self) -> int:
        return len(self._message_cache)


class MessageRefreshCache:
    MSG_REFRESH_BATCH_SIZE = 3000

    """
    If the media queue has gotten quite long, file references may go out of date before the media is downloaded.
    This refresh cache allows us to re-fetch all the messages of a chat (from a given message ID), and then return those refreshed message objects for future media downloads in that chat.
    """
    def __init__(self, client: TelegramClient) -> None:
        self.client = client
        self.chat_caches: dict[int, ChatMessageRefreshCache] = {}
        self._refresh_task_lock = asyncio.Lock() # This lock regulates whether the refresh task is running
        self._refresh_task: Optional[asyncio.Task] = None
        self._refresh_queue: asyncio.Queue[RefreshRequest] = asyncio.Queue()
        self._refresh_queue_empty: asyncio.Event = asyncio.Event()
        self._refresh_queue_empty.set()
        size_message_refresh_cache.set_function(lambda: sum(c.size() for c in self.chat_caches.values()))
        refresh_task_active.set_function(lambda: self._refresh_task is not None and not self._refresh_task.done())
        message_refresh_request_queue_size.set_function(lambda: self._refresh_queue.qsize())
        message_refresh_request_queue_empty.set_function(lambda: self._refresh_queue_empty.is_set())

    def _get_chat_cache(self, chat_id: int) -> ChatMessageRefreshCache:
        if chat_id not in self.chat_caches:
            self.chat_caches[chat_id] = ChatMessageRefreshCache(self, chat_id)
        return self.chat_caches[chat_id]

    async def get_message(
            self,
            chat_id: int,
            message_id: int,
            old_msg: telethon.types.Message,
            archive_target: "ArchiveTarget",
    ) -> Optional[telethon.types.Message]:
        # First, check if a new version already exists in cache
        chat_cache = self._get_chat_cache(chat_id)
        new_msg = chat_cache.get_message_only(message_id)
        if new_msg is not None and new_msg != old_msg:
            count_already_refreshed_msgs.inc()
            return new_msg
        # Fetch a wait event, and request the message be refreshed
        wait_event = chat_cache.wait_event_for_msg(message_id)
        logger.info("Requesting message ID %s be refreshed", message_id)
        await self._queue_refresh(chat_id, message_id, old_msg, archive_target)
        # Wait for the request to populate the new message
        logger.info("Waiting for message ID %s to be refreshed", message_id)
        await wait_event.wait()
        logger.info("Wait for message ID %s is over, getting and checking message", message_id)
        # Check the new message is actually different
        new_msg = chat_cache.get_message_only(message_id)
        if new_msg is None:
            logger.warning("Message ID %s was requested for refresh, but no message was found in cache after refresh", message_id)
            raise ValueError(f"Attempted to update message ID {message_id}, (chat ID {chat_id}), but the new message was not found in cache afterwards")
        if new_msg == old_msg:
            logger.warning("Message ID %s was requested for refresh, but new version was not different", message_id)
            msg_date = getattr(old_msg, "date", None)
            raise ValueError(f"Attempted to update message ID {message_id}, (chat ID {chat_id}, message date {msg_date}) but the new message is not an update")
        # Otherwise, return the new message
        return new_msg

    async def _queue_refresh(
            self,
            chat_id: int,
            message_id: int,
            old_msg: telethon.types.Message,
            archive_target: "ArchiveTarget",
    ) -> None:
        count_refresh_requests.inc()
        # Acquire the lock before modifying queue and task
        async with self._refresh_task_lock:
            # Queue up the request
            refresh_request = RefreshRequest(chat_id, message_id, old_msg, archive_target)
            self._refresh_queue_empty.clear()
            await self._refresh_queue.put(refresh_request)
            # Check if task is running, and start it if not
            if self._refresh_task is None or self._refresh_task.done():
                logger.info("Starting up message refresher task")
                self._refresh_task = asyncio.create_task(self._run_refreshes())

    async def _run_refreshes(self) -> None:
        """
        This method runs in task, one at once. It empties out the queue of message refresh requests and exits
        """
        logger.info("Starting up message refresher")
        while True:
            async with self._refresh_task_lock:
                try:
                    refresh_request = self._refresh_queue.get_nowait()
                except QueueEmpty:
                    logger.info("Message refresh request queue is empty")
                    break
            chat_id = refresh_request.chat_id
            msg_id = refresh_request.msg_id
            archive_target = refresh_request.archive_target
            chat_cache = self._get_chat_cache(chat_id)
            # If the cache already has an updated version, skip this request
            cached_msg = chat_cache.get_message_only(msg_id)
            if cached_msg is not None and cached_msg != refresh_request.old_msg:
                chat_cache.signal_message_updated(msg_id)
                logger.info("Discarding expired refresh request for chat ID %s, message ID %s", chat_id, msg_id)
                continue
            # Refresh messages
            logger.info("Refreshing message ID %s from chat ID %s", msg_id, chat_id)
            await self._refresh_from_msg(chat_id, msg_id, archive_target)
        # Left the loop, must have emptied queue
        logger.info("Shutting down message refresher, as the request queue is clear")
        self._refresh_queue_empty.set()

    async def _refresh_from_msg(self, chat_id: int, max_message_id: int, archive_target: "ArchiveTarget") -> None:
        chat_cache = self._get_chat_cache(chat_id)
        logger.info("Fetching refreshed message objects for chat %s", chat_id)
        num_msgs = 0
        async for msg in self.client.iter_messages(chat_id, max_id=max_message_id+1):
            count_refreshed_messages.inc()
            # Include all messages, including without media, because they might have previously had media when requested
            # Add them to the message cache
            chat_cache.add_message_to_cache(msg)
            # Signal that the message is updated
            chat_cache.signal_message_updated(msg.id)
            # Send the message back to the archive target for saving
            await archive_target.process_message(msg)
            # Increment counter
            num_msgs += 1
            # If there's more than a thousand messages, quit here. We don't want to get the whole chat history
            if num_msgs >= self.MSG_REFRESH_BATCH_SIZE:
                break
        logger.info(
            "Finished refreshing messages for chat %s. Added %s messages to cache (cache is now %s messages)",
            chat_id, num_msgs, chat_cache.size()
        )

    def refresh_queue_size(self) -> int:
        return self._refresh_queue.qsize()

    async def wait_until_target_done(self, archive_target: "ArchiveTarget") -> None:
        logger.info("Waiting until MessageRefreshCache queue is done with target with chat ID %s", archive_target.chat_id)
        # This could be optimised to check for the specific target, but, it is not likely this will wait long for the entire queue to empty
        await self._refresh_queue_empty.wait()
