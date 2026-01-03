from typing import Optional


class SubsystemQueueEntry:
    def __init__(
            self,
            queue_entry_id: Optional[int],
            subsystem_name: str,
            message_id: Optional[int],
            extra_data: Optional[dict],
    ) -> None:
        self.queue_entry_id = queue_entry_id
        self.subsystem_name = subsystem_name
        self.message_id = message_id
        self.extra_data = extra_data


class MediaDownloaderQueueEntry(SubsystemQueueEntry):
    SUBSYSTEM_NAME = "MediaDownloader"

    def __init__(self, queue_entry_id: Optional[int], message_id: int, media_id: int) -> None:
        super().__init__(
            queue_entry_id,
            self.SUBSYSTEM_NAME,
            message_id,
            {
                "media_id": media_id,
            },
        )

    @property
    def media_id(self) -> int:
        if self.extra_data is None:
            raise ValueError("This queue entry does not contain the right data to be a MediaDownloaderQueueEntry")
        return self.extra_data["media_id"]

    @classmethod
    def from_generic(cls, queue_entry: SubsystemQueueEntry) -> "MediaDownloaderQueueEntry":
        if queue_entry.subsystem_name != cls.SUBSYSTEM_NAME:
            raise ValueError(f"This queue entry is for a different subsystem ({queue_entry.subsystem_name}) than {cls.SUBSYSTEM_NAME}")
        if queue_entry.message_id is None or queue_entry.extra_data is None or queue_entry.extra_data.get("media_id") is None:
            raise ValueError("This queue entry does not contain the right data to be a MediaDownloaderQueueEntry")
        return cls(
            queue_entry.queue_entry_id,
            queue_entry.message_id,
            queue_entry.extra_data["media_id"],
        )


class StickerDownloaderQueueEntry(SubsystemQueueEntry):
    SUBSYSTEM_NAME = "StickerDownloader"

    def __init__(
            self,
            queue_entry_id: Optional[int],
            message_id: int,
            sticker_id: int,
            direct_from_msg: bool,
    ) -> None:
        super().__init__(
            queue_entry_id,
            self.SUBSYSTEM_NAME,
            message_id,
            {
                "sticker_id": sticker_id,
                "direct_from_msg": direct_from_msg,
            },
        )

    @property
    def direct_from_msg(self) -> bool:
        if self.extra_data is None:
            raise ValueError("This queue entry does not contain the right data to be a StickerDownloaderQueueEntry")
        return self.extra_data["direct_from_msg"]

    @property
    def sticker_id(self) -> int:
        if self.extra_data is None:
            raise ValueError("This queue entry does not contain the right data to be a StickerDownloaderQueueEntry")
        return self.extra_data["sticker_id"]

    @classmethod
    def from_generic(cls, queue_entry: SubsystemQueueEntry) -> "StickerDownloaderQueueEntry":
        if queue_entry.subsystem_name != cls.SUBSYSTEM_NAME:
            raise ValueError(f"This queue entry is for a different subsystem ({queue_entry.subsystem_name}) than {cls.SUBSYSTEM_NAME}")
        if queue_entry.message_id is None or queue_entry.extra_data is None or queue_entry.extra_data.get("direct_from_msg") is None or queue_entry.extra_data.get("sticker_id") is None:
            raise ValueError("This queue entry does not contain the right data to be a StickerDownloaderQueueEntry")
        return cls(
            queue_entry.queue_entry_id,
            queue_entry.message_id,
            queue_entry.extra_data["direct_from_msg"],
            queue_entry.extra_data["sticker_id"]
        )
