import asyncio
import dataclasses
from typing import Union, NewType

from prometheus_client import Counter
from telethon import TelegramClient
from telethon.tl.functions.channels import GetFullChannelRequest
from telethon.tl.functions.messages import GetFullChatRequest
from telethon.tl.functions.users import GetFullUserRequest
from telethon.tl.types import PeerUser, PeerChat, PeerChannel

from tg_backup.database.chat_database import ChatDatabase
from tg_backup.database.core_database import CoreDatabase
from tg_backup.models.chat import Chat
from tg_backup.models.user import User
from tg_backup.subsystems.abstract_subsystem import AbstractSubsystem


peers_processed = Counter(
    "tgbackup_peerdatafetcher_peers_processed_count",
    "Total number of peers which have been picked from the queue by the PeerDataFetcher",
)

Peer = Union[PeerUser, PeerChat, PeerChannel]
PeerCacheID = NewType("PeerCacheID", tuple[str, int])


def peer_cache_key(peer: Peer) -> PeerCacheID:
    return {
        PeerUser: lambda p: PeerCacheID(("PeerUser", p.user_id)),
        PeerChat: lambda p: PeerCacheID(("PeerChat", p.chat_id)),
        PeerChannel: lambda p: PeerCacheID(("PeerChannel", p.channel_id)),
    }[peer.__class__](peer)


@dataclasses.dataclass
class ChatQueueEntry:
    peer: Peer
    raw: object


@dataclasses.dataclass
class ChatQueue:
    chat_id: int
    chat_db: ChatDatabase
    queue: asyncio.Queue[ChatQueueEntry]
    stop_when_empty: bool = False


class PeerDataFetcher(AbstractSubsystem):
    def __init__(self, client: TelegramClient, core_db: CoreDatabase) -> None:
        super().__init__(client)
        self.core_db = core_db
        self.chat_queues: dict[int, ChatQueue] = {}
        self.core_seen_peer_ids: set[PeerCacheID] = set()
        self.chat_seen_peer_ids: dict[int, set[PeerCacheID]] = {}

    def peer_id_seen_core(self, peer: Peer) -> bool:
        return peer_cache_key(peer) in self.core_seen_peer_ids

    def peer_id_seen_in_chat(self, peer: Peer, chat_id: int) -> bool:
        return peer_cache_key(peer) in self.chat_seen_peer_ids.get(chat_id, set())

    def record_peer_id_seen_core(self, peer: Peer) -> None:
        self.core_seen_peer_ids.add(peer_cache_key(peer))

    def record_peer_id_seen_in_chat(self, peer: Peer, chat_id: int) -> None:
        if chat_id not in self.chat_seen_peer_ids:
            self.chat_seen_peer_ids[chat_id] = set()
        self.chat_seen_peer_ids[chat_id].add(peer_cache_key(peer))

    def _get_next_in_queue(self) -> tuple[ChatQueue, ChatQueueEntry]:
        for chat_queue in self.chat_queues.values():
            try:
                return chat_queue, chat_queue.queue.get_nowait()
            except asyncio.QueueEmpty:
                continue
        raise asyncio.QueueEmpty()

    async def _do_process(self) -> None:
        # Fetch item from queue
        chat_queue, queue_entry = self._get_next_in_queue()
        peers_processed.inc()
        # Check whether cache wants update
        if self.peer_id_seen_core(queue_entry.peer) and self.peer_id_seen_in_chat(queue_entry.peer, chat_queue.chat_id):
            return
        if isinstance(queue_entry.peer, PeerUser):
            return await self._process_user(chat_queue, queue_entry.peer)
        if isinstance(queue_entry.peer, PeerChat):
            return await self._process_chat(chat_queue, queue_entry.peer)
        if isinstance(queue_entry.peer, PeerChannel):
            return await self._process_channel(chat_queue, queue_entry.peer)

    async def _process_user(self, chat_queue: ChatQueue, user: PeerUser) -> None:
        chat_id = chat_queue.chat_id
        # Get full user info
        # noinspection PyTypeChecker
        full = await self.client(GetFullUserRequest(user))
        # Convert user to storable object
        user_obj = User.from_full_user(full)
        # Save to chat DB and core DB
        if not self.peer_id_seen_core(user):
            self.core_db.save_user(user_obj)
        if not self.peer_id_seen_in_chat(user, chat_id):
            chat_queue.chat_db.save_user(user_obj)
        # Save to cache
        self.record_peer_id_seen_core(user)
        self.record_peer_id_seen_in_chat(user, chat_id)
        # Mark done in queue
        chat_queue.queue.task_done()

    async def _process_chat(self, chat_queue: ChatQueue, chat: PeerChat) -> None:
        chat_id = chat_queue.chat_id
        # Get chat entity
        entity = await self.client.get_entity(chat)
        # Get full chat info
        # noinspection PyTypeChecker
        full = await self.client(GetFullChatRequest(entity))
        # Convert chat to storable object
        chat_obj = Chat.from_full_chat(full)
        # Queue up any linked chats
        if chat_obj.migrated_to_chat_id is not None:
            await self.queue_channel(chat_id, chat_queue.chat_db, chat_obj.migrated_to_chat_id, force_add=True)
        # Save to chat DB and core DB
        await self._save_chat(chat, chat_id, chat_queue, chat_obj)

    async def _process_channel(self, chat_queue: ChatQueue, channel: PeerChannel) -> None:
        chat_id = chat_queue.chat_id
        # Get full channel info
        # noinspection PyTypeChecker
        full = await self.client(GetFullChannelRequest(channel))
        # Convert channel to storable object
        chat_obj = Chat.from_full_chat(full)
        # Queue up any linked chats
        if chat_obj.linked_chat_id is not None:
            await self.queue_channel(chat_id, chat_queue.chat_db, chat_obj.linked_chat_id, force_add=True)
        if chat_obj.migrated_from_chat_id is not None:
            await self.queue_chat(chat_id, chat_queue.chat_db, chat_obj.migrated_from_chat_id, force_add=True)
        # Save to chat DB and core DB
        await self._save_chat(channel, chat_id, chat_queue, chat_obj)

    async def _save_chat(self, peer: Peer, chat_id: int, chat_queue: ChatQueue, chat_obj: Chat) -> None:
        if not self.peer_id_seen_core(peer):
            self.core_db.save_chat(chat_obj)
        if not self.peer_id_seen_in_chat(peer, chat_id):
            chat_queue.chat_db.save_chat(chat_obj)
        # Save to cache
        self.record_peer_id_seen_core(peer)
        self.record_peer_id_seen_in_chat(peer, chat_id)
        # Mark done in queue
        chat_queue.queue.task_done()

    def queue_size(self) -> int:
        return sum(chat_queue.queue.qsize() for chat_queue in self.chat_queues.values())

    async def queue_user(
            self,
            chat_id: int,
            chat_db: ChatDatabase,
            user: Union[PeerUser, int],
            force_add: bool = False,
    ) -> None:
        if user is None:
            return
        if isinstance(user, int):
            user = PeerUser(user)
        await self.queue_peer(chat_id, chat_db, user, force_add=force_add)

    async def queue_chat(
            self,
            chat_id: int,
            chat_db: ChatDatabase,
            chat: Union[PeerChat, int],
            force_add: bool = False,
    ) -> None:
        if chat is None:
            return
        if isinstance(chat, int):
            chat = PeerChat(chat)
        await self.queue_peer(chat_id, chat_db, chat, force_add=force_add)

    async def queue_channel(
            self,
            chat_id: int,
            chat_db: ChatDatabase,
            channel: Union[PeerChannel, int],
            force_add: bool = False,
    ) -> None:
        if channel is None:
            return
        if isinstance(channel, int):
            channel = PeerChannel(channel)
        await self.queue_peer(chat_id, chat_db, channel, force_add=force_add)

    async def queue_peer(self, chat_id: int, chat_db: ChatDatabase, peer: Peer, force_add: bool = False) -> None:
        # Ensure a peer is given
        if peer is None:
            return
        raw = peer
        # Ensure peer is of a valid type
        if not isinstance(peer, PeerUser) and not isinstance(peer, PeerChat) and not isinstance(peer, PeerChannel):
            raise ValueError(f"Unrecognised peer type: {peer}")
        # Check if already in cache
        if self.peer_id_seen_core(peer) and self.peer_id_seen_in_chat(peer, chat_id):
            return
        # Set up chat queue
        if chat_id not in self.chat_queues:
            self.chat_queues[chat_id] = ChatQueue(chat_id, chat_db, asyncio.Queue())
        # Ensure chat queue isn't being emptied
        if not force_add and self.chat_queues[chat_id].stop_when_empty:
            raise ValueError("PeerDataFetcher has been told to stop for that chat when empty, cannot queue more peers for it")
        # Add to chat queue
        await self.chat_queues[chat_id].queue.put(ChatQueueEntry(peer, raw))

    async def wait_until_chat_empty(self, chat_id: int) -> None:
        if chat_id not in self.chat_queues:
            return
        self.chat_queues[chat_id].stop_when_empty = True
        await self.chat_queues[chat_id].queue.join()
