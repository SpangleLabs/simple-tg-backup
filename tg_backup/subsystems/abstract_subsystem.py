import asyncio
import logging
from abc import ABC, abstractmethod
from typing import Optional

from telethon import TelegramClient


logger = logging.getLogger(__name__)


class AbstractSubsystem(ABC):
    def __init__(self, client: TelegramClient):
        self.client = client
        self.running = False
        self.stop_when_empty = False
        self.task: Optional[asyncio.Task] = None

    def name(self) -> str:
        return type(self).__name__

    def start(self) -> None:
        self.task = asyncio.create_task(self.run())

    async def stop(self, fast: bool = False) -> None:
        # Shut down subsystem
        if not self.running:
            return
        logging.info("Awaiting shutdown of %s", self.name())
        if fast:
            self.abort()
        else:
            self.mark_as_filled()
        if self.task is not None:
            await self.task

    async def run(self) -> None:
        self.running = True
        while self.running:
            try:
                await self._do_process()
            except asyncio.QueueEmpty:
                if self.stop_when_empty:
                    logger.info("Queue is empty, shutting down %s", self.name())
                    self.running = False
                    return
                await asyncio.sleep(1)
                continue
            logger.info("There are %s remaining items in the %s queue", self.queue_size(), self.name())

    @abstractmethod
    async def _do_process(self) -> None:
        raise NotImplementedError()

    @abstractmethod
    def queue_size(self) -> int:
        raise NotImplementedError()

    def abort(self) -> None:
        self.running = False

    def mark_as_filled(self) -> None:
        self.stop_when_empty = True
