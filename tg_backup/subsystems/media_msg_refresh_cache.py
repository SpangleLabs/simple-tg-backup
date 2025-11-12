import asyncio
import dataclasses
import logging
from asyncio import QueueEmpty
from typing import Optional

import telethon
from telethon import TelegramClient

logger = logging.getLogger(__name__)


@dataclasses.dataclass
class RefreshRequest:
    chat_id: int
    msg_id: int
    old_msg: telethon.types.Message


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
            wait_event.set()
            wait_event.clear()

    def wait_event_for_msg(self, message_id: int) -> asyncio.Event:
        wait_event = self._waiting_for_msg.get(message_id, None)
        if wait_event is not None:
            return wait_event
        wait_event = asyncio.Event()
        self._waiting_for_msg[message_id] = wait_event
        return wait_event

    def size(self) -> int:
        return len(self._message_cache)


class MessageRefreshCache:
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

    def _get_chat_cache(self, chat_id: int) -> ChatMessageRefreshCache:
        if chat_id not in self.chat_caches:
            self.chat_caches[chat_id] = ChatMessageRefreshCache(self, chat_id)
        return self.chat_caches[chat_id]

    async def get_message(
            self,
            chat_id: int,
            message_id: int,
            old_msg: telethon.types.Message,
    ) -> Optional[telethon.types.Message]:
        # First, check if a new version already exists in cache
        chat_cache = self._get_chat_cache(chat_id)
        new_msg = chat_cache.get_message_only(message_id)
        if new_msg is not None and new_msg != old_msg:
            return new_msg
        # Fetch a wait event, and request the message be refreshed
        wait_event = chat_cache.wait_event_for_msg(message_id)
        logger.debug("Requesting message ID %s be refreshed", message_id)
        await self._queue_refresh(chat_id, message_id, old_msg)
        # Wait for the request to populate the new message
        logger.debug("Waiting for message ID %s to be refreshed", message_id)
        await wait_event.wait()
        logger.debug("Wait for message ID %s is over, getting and checking message", message_id)
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

    async def _queue_refresh(self, chat_id: int, message_id: int, old_msg: telethon.types.Message) -> None:
        # Acquire the lock before modifying queue and task
        async with self._refresh_task_lock:
            # Queue up the request
            refresh_request = RefreshRequest(chat_id, message_id, old_msg)
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
            chat_cache = self._get_chat_cache(chat_id)
            # If the cache already has an updated version, skip this request
            cached_msg = chat_cache.get_message_only(msg_id)
            if cached_msg is not None and cached_msg != refresh_request.old_msg:
                chat_cache.signal_message_updated(msg_id)
                logger.info("Discarding expired refresh request for chat ID %s, message ID %s", chat_id, msg_id)
                continue
            # Refresh messages
            logger.info("Refreshing message ID %s from chat ID %s", msg_id, chat_id)
            await self._refresh_from_msg(chat_id, msg_id)
        # Left the loop, must have emptied queue
        logger.info("Shutting down message refresher, as the request queue is clear")

    async def _refresh_from_msg(self, chat_id: int, max_message_id: int) -> None:
        chat_cache = self._get_chat_cache(chat_id)
        logger.info("Fetching refreshed message objects for chat %s", chat_id)
        num_msgs = 0
        async for msg in self.client.iter_messages(chat_id, max_id=max_message_id):
            # Skip any messages without media
            if getattr(msg, "media", None) is None:
                continue
            # Add them to the message cache
            chat_cache.add_message_to_cache(msg)
            # Increment counter
            num_msgs += 1
            # If there's more than a thousand messages, quit here. We don't want to get the whole chat history
            if num_msgs > 1000:
                break
        logger.info(
            "Finished refreshing messages for chat %s. Added %s messages to cache (cache is now %s messages)",
            chat_id, num_msgs, chat_cache.size()
        )
