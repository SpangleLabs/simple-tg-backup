import asyncio
import logging
from abc import ABC, abstractmethod

from telethon import TelegramClient


logger = logging.getLogger(__name__)


class AbstractSubsystem(ABC):
    def __init__(self, client: TelegramClient):
        self.client = client
        self.running = False
        self.stop_when_empty = False

    async def run(self) -> None:
        self.running = True
        while self.running:
            try:
                await self._do_process()
            except asyncio.QueueEmpty:
                if self.stop_when_empty:
                    logger.info("Queue is empty, shutting down %s", type(self).__name__)
                    self.running = False
                    return
                await asyncio.sleep(1)
                continue

    @abstractmethod
    async def _do_process(self) -> None:
        raise NotImplementedError()

    def abort(self) -> None:
        self.running = False

    def mark_as_filled(self) -> None:
        self.stop_when_empty = True
