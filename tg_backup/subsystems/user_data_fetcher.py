import asyncio
import dataclasses

from telethon import TelegramClient
from telethon.tl.functions.users import GetFullUserRequest
from telethon.tl.types import PeerUser

from tg_backup.database.chat_database import ChatDatabase
from tg_backup.database.core_database import CoreDatabase
from tg_backup.models.user import User
from tg_backup.subsystems.abstract_subsystem import AbstractSubsystem


@dataclasses.dataclass
class UserQueueEntry:
    chat_id: int
    chat_db: ChatDatabase
    user: PeerUser


class UserDataFetcher(AbstractSubsystem):
    def __init__(self, client: TelegramClient, core_db: CoreDatabase) -> None:
        super().__init__(client)
        self.core_db = core_db
        self.queue: asyncio.Queue[UserQueueEntry] = asyncio.Queue()
        self.core_seen_user_ids: set[int] = set()
        self.chat_seen_user_ids: dict[int, set[int]] = {}

    def user_id_seen_core(self, user_id: int) -> bool:
        return user_id in self.core_seen_user_ids

    def user_id_seen_in_chat(self, user_id: int, chat_id: int) -> bool:
        return user_id in self.chat_seen_user_ids.get(chat_id, set())

    async def _do_process(self) -> None:
        # Fetch item from queue
        queue_entry = self.queue.get_nowait()
        # Check whether cache wants update
        chat_id = queue_entry.chat_id
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
            queue_entry.chat_db.save_user(user_obj)
        # Save to cache
        self.core_seen_user_ids.add(queue_entry.user.user_id)
        if chat_id not in self.chat_seen_user_ids:
            self.chat_seen_user_ids[chat_id] = set()
        self.chat_seen_user_ids[chat_id].add(user_id)

    def queue_size(self) -> int:
        return self.queue.qsize()

    async def queue_user(self, chat_id: int, chat_db: ChatDatabase, user: PeerUser) -> None:
        if user is None:
            return
        if not isinstance(user, PeerUser):
            raise ValueError(f"Unrecognised user type: {user}")
        await self.queue.put(UserQueueEntry(chat_id, chat_db, user))
