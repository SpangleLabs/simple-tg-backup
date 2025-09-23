import asyncio
import dataclasses

from prometheus_client import Counter
from telethon import TelegramClient
from telethon.tl.functions.users import GetFullUserRequest
from telethon.tl.types import PeerUser

from tg_backup.database.chat_database import ChatDatabase
from tg_backup.database.core_database import CoreDatabase
from tg_backup.models.user import User
from tg_backup.subsystems.abstract_subsystem import AbstractSubsystem


users_processed = Counter(
    "tgbackup_userdatadownloader_users_processed_count",
    "Total number of users which have been picked from the queue by the UserDataFetcher",
)


@dataclasses.dataclass
class UserQueueEntry:
    user: PeerUser


@dataclasses.dataclass
class UserDataChatQueue:
    chat_id: int
    chat_db: ChatDatabase
    queue: asyncio.Queue[UserQueueEntry]
    stop_when_empty: bool = False


class UserDataFetcher(AbstractSubsystem):
    def __init__(self, client: TelegramClient, core_db: CoreDatabase) -> None:
        super().__init__(client)
        self.core_db = core_db
        self.chat_queues: dict[int, UserDataChatQueue] = {}
        self.core_seen_user_ids: set[int] = set()
        self.chat_seen_user_ids: dict[int, set[int]] = {}

    def user_id_seen_core(self, user_id: int) -> bool:
        return user_id in self.core_seen_user_ids

    def user_id_seen_in_chat(self, user_id: int, chat_id: int) -> bool:
        return user_id in self.chat_seen_user_ids.get(chat_id, set())

    def _get_next_in_queue(self) -> tuple[UserDataChatQueue, UserQueueEntry]:
        for chat_queue in self.chat_queues.values():
            try:
                return chat_queue, chat_queue.queue.get_nowait()
            except asyncio.QueueEmpty:
                continue
        raise asyncio.QueueEmpty()

    async def _do_process(self) -> None:
        # Fetch item from queue
        chat_queue, queue_entry = self._get_next_in_queue()
        users_processed.inc()
        # Check whether cache wants update
        chat_id = chat_queue.chat_id
        user_id = queue_entry.user.user_id
        if self.user_id_seen_core(user_id) and self.user_id_seen_in_chat(user_id, chat_id):
            return
        # Get full user info
        # noinspection PyTypeChecker
        full = await self.client(GetFullUserRequest(queue_entry.user))
        # Convert user to storable object
        user_obj = User.from_full_user(full)
        # Save to chat DB and core DB
        if not self.user_id_seen_core(user_id):
            self.core_db.save_user(user_obj)
        if not self.user_id_seen_in_chat(user_id, chat_id):
            chat_queue.chat_db.save_user(user_obj)
        # Save to cache
        self.core_seen_user_ids.add(queue_entry.user.user_id)
        if chat_id not in self.chat_seen_user_ids:
            self.chat_seen_user_ids[chat_id] = set()
        self.chat_seen_user_ids[chat_id].add(user_id)
        # Mark done in queue
        chat_queue.queue.task_done()

    def queue_size(self) -> int:
        return sum(chat_queue.queue.qsize() for chat_queue in self.chat_queues.values())

    async def queue_user(self, chat_id: int, chat_db: ChatDatabase, user: PeerUser) -> None:
        # Ensure a user is given
        if user is None:
            return
        # Ensure user is of the correct type
        if not isinstance(user, PeerUser):
            if isinstance(user, int):
                user = PeerUser(user)
            else:
                raise ValueError(f"Unrecognised user type: {user}")
        # Set up chat queue
        if chat_id not in self.chat_queues:
            self.chat_queues[chat_id] = UserDataChatQueue(chat_id, chat_db, asyncio.Queue())
        # Ensure chat queue isn't being emptied
        if self.chat_queues[chat_id].stop_when_empty:
            raise ValueError("UserDataFetcher has been told to stop for that chat when empty, cannot queue more users for it")
        # Add to chat queue
        await self.chat_queues[chat_id].queue.put(UserQueueEntry(user))

    async def wait_until_chat_empty(self, chat_id: int) -> None:
        if chat_id not in self.chat_queues:
            return
        self.chat_queues[chat_id].stop_when_empty = True
        await self.chat_queues[chat_id].queue.join()
