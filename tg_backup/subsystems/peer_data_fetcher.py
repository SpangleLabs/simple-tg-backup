import dataclasses
import datetime
import logging
from typing import Union, NewType, Optional

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
from tg_backup.subsystems.abstract_subsystem import ArchiveRunQueue, AbstractTargetQueuedSubsystem, TimedCache

peers_processed = Counter(
    "tgbackup_peerdatafetcher_peers_processed_count",
    "Total number of peers which have been picked from the queue by the PeerDataFetcher",
)

logger = logging.getLogger(__name__)


Peer = Union[PeerUser, PeerChat, PeerChannel]
PeerCacheID = NewType("PeerCacheID", tuple[str, int])


def peer_cache_key(peer: Peer) -> PeerCacheID:
    return {
        PeerUser: lambda p: PeerCacheID(("PeerUser", p.user_id)),
        PeerChat: lambda p: PeerCacheID(("PeerChat", p.chat_id)),
        PeerChannel: lambda p: PeerCacheID(("PeerChannel", p.channel_id)),
    }[peer.__class__](peer)


@dataclasses.dataclass
class PeerQueueEntry:
    peer: Peer


class PeerDataFetcher(AbstractTargetQueuedSubsystem[PeerQueueEntry]):
    CACHE_EXPIRY = datetime.timedelta(days=1)

    def __init__(self, client: TelegramClient, core_db: CoreDatabase) -> None:
        super().__init__(client)
        self.core_db = core_db
        self._core_seen_peer_ids = TimedCache[PeerCacheID](self.CACHE_EXPIRY)
        self.chat_seen_peer_ids: dict[int, set[PeerCacheID]] = {} # Not timed, because we just want at least one entry for the peer in each chat DB

    def peer_id_seen_core(self, peer: Peer) -> bool:
        return self._core_seen_peer_ids.is_resource_id_cached(peer_cache_key(peer))

    def peer_id_seen_in_chat(self, peer: Peer, chat_id: Optional[int]) -> bool:
        if chat_id is None:
            return True
        return peer_cache_key(peer) in self.chat_seen_peer_ids.get(chat_id, set())

    def record_peer_id_seen_core(self, peer: Peer) -> None:
        self._core_seen_peer_ids.cache_resource_id(peer_cache_key(peer))

    def record_peer_id_seen_in_chat(self, peer: Peer, chat_id: Optional[int]) -> None:
        if chat_id is None:
            return
        if chat_id not in self.chat_seen_peer_ids:
            self.chat_seen_peer_ids[chat_id] = set()
        self.chat_seen_peer_ids[chat_id].add(peer_cache_key(peer))

    async def _do_process(self) -> None:
        # Fetch item from queue
        chat_queue, queue_entry = self._get_next_in_queue()
        peers_processed.inc()
        logger.info("Processing peer to fetch")
        # Check whether cache wants update
        if self.peer_id_seen_core(queue_entry.peer) and self.peer_id_seen_in_chat(queue_entry.peer, chat_queue.chat_id):
            chat_queue.queue.task_done()
            return
        if isinstance(queue_entry.peer, PeerUser):
            return await self._process_user(chat_queue, queue_entry.peer)
        if isinstance(queue_entry.peer, PeerChat):
            return await self._process_chat(chat_queue, queue_entry.peer)
        if isinstance(queue_entry.peer, PeerChannel):
            return await self._process_channel(chat_queue, queue_entry.peer)

    async def _process_user(self, chat_queue: ArchiveRunQueue[PeerQueueEntry], user: PeerUser) -> None:
        chat_id = chat_queue.chat_id
        logger.info("Fetching full user info from telegram")
        # Get full user info
        # noinspection PyTypeChecker
        full = await self.client(GetFullUserRequest(user))
        # Convert user to storable object
        user_obj = User.from_full_user(full)
        # Save to chat DB and core DB
        if not self.peer_id_seen_core(user):
            self.core_db.save_user(user_obj)
        if not self.peer_id_seen_in_chat(user, chat_id) and chat_queue.chat_db is not None:
            chat_queue.chat_db.save_user(user_obj)
        # Save to cache
        self.record_peer_id_seen_core(user)
        self.record_peer_id_seen_in_chat(user, chat_id)
        # Mark done in queue
        chat_queue.queue.task_done()

    async def _process_chat(self, chat_queue: ArchiveRunQueue[PeerQueueEntry], chat: PeerChat) -> None:
        queue_key = chat_queue.queue_key
        chat_id = chat_queue.chat_id
        chat_db = chat_queue.chat_db
        logger.info("Fetching full chat data from telegram")
        # Get full chat info
        # noinspection PyTypeChecker
        full = await self.client(GetFullChatRequest(chat.chat_id))
        # Convert chat to storable object
        chat_obj = Chat.from_full_chat(full)
        # Queue up any linked chats
        if chat_obj.migrated_to_chat_id is not None:
            await self.queue_channel(queue_key, chat_id, chat_db, chat_obj.migrated_to_chat_id, force_add=True)
        # Save to chat DB and core DB
        await self._save_chat(chat, chat_id, chat_queue, chat_obj)

    async def _process_channel(self, chat_queue: ArchiveRunQueue[PeerQueueEntry], channel: PeerChannel) -> None:
        queue_key = chat_queue.queue_key
        chat_id = chat_queue.chat_id
        chat_db = chat_queue.chat_db
        logger.info("Fetching full channel data from telegram")
        # Get full channel info
        # noinspection PyTypeChecker
        full = await self.client(GetFullChannelRequest(channel))
        # Convert channel to storable object
        chat_obj = Chat.from_full_chat(full)
        # Queue up any linked chats
        if chat_obj.linked_chat_id is not None:
            await self.queue_channel(queue_key, chat_id, chat_db, chat_obj.linked_chat_id, force_add=True)
        if chat_obj.migrated_from_chat_id is not None:
            await self.queue_chat(queue_key, chat_id, chat_db, chat_obj.migrated_from_chat_id, force_add=True)
        # Save to chat DB and core DB
        await self._save_chat(channel, chat_id, chat_queue, chat_obj)

    async def _save_chat(self, peer: Peer, chat_id: Optional[int], chat_queue: ArchiveRunQueue[PeerQueueEntry], chat_obj: Chat) -> None:
        if not self.peer_id_seen_core(peer):
            self.core_db.save_chat(chat_obj)
        if not self.peer_id_seen_in_chat(peer, chat_id) and chat_queue.chat_db is not None:
            chat_queue.chat_db.save_chat(chat_obj)
        # Save to cache
        self.record_peer_id_seen_core(peer)
        self.record_peer_id_seen_in_chat(peer, chat_id)
        # Mark done in queue
        chat_queue.queue.task_done()

    async def queue_user(
            self,
            queue_key: Optional[str],
            chat_id: Optional[int],
            chat_db: Optional[ChatDatabase],
            user: Union[PeerUser, int],
            force_add: bool = False,
    ) -> None:
        if user is None:
            return
        if isinstance(user, int):
            user = PeerUser(user)
        await self.queue_peer(queue_key, chat_id, chat_db, user, force_add=force_add)

    async def queue_chat(
            self,
            queue_key: Optional[str],
            chat_id: Optional[int],
            chat_db: Optional[ChatDatabase],
            chat: Union[PeerChat, int],
            force_add: bool = False,
    ) -> None:
        if chat is None:
            return
        if isinstance(chat, int):
            chat = PeerChat(chat)
        await self.queue_peer(queue_key, chat_id, chat_db, chat, force_add=force_add)

    async def queue_channel(
            self,
            queue_key: Optional[str],
            chat_id: Optional[int],
            chat_db: Optional[ChatDatabase],
            channel: Union[PeerChannel, int],
            force_add: bool = False,
    ) -> None:
        if channel is None:
            return
        if isinstance(channel, int):
            channel = PeerChannel(channel)
        await self.queue_peer(queue_key, chat_id, chat_db, channel, force_add=force_add)

    async def queue_peer(
            self,
            queue_key: Optional[str],
            chat_id: Optional[int],
            chat_db: Optional[ChatDatabase],
            peer: Peer,
            force_add: bool = False,
    ) -> None:
        # Ensure a peer is given
        if peer is None:
            return
        # Ensure peer is of a valid type
        if not isinstance(peer, PeerUser) and not isinstance(peer, PeerChat) and not isinstance(peer, PeerChannel):
            raise ValueError(f"Unrecognised peer type: {peer}")
        # Check if already in cache
        if self.peer_id_seen_core(peer) and self.peer_id_seen_in_chat(peer, chat_id):
            return
        logger.info("Adding peer %s to peer queue", peer_cache_key(peer))
        await self._add_queue_entry(queue_key, chat_id, chat_db, PeerQueueEntry(peer), force_add)
