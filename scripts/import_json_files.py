import asyncio
import datetime
import glob
import json
import logging
import os.path
import pathlib
import shutil

from main import setup_logging
from tg_backup.archiver import Archiver
from tg_backup.config import load_config
from tg_backup.database.chat_database import ChatDatabase
from tg_backup.models.admin_event import AdminEvent
from tg_backup.models.message import Message
from tg_backup.utils.parse_str_repr import StrReprObj, str_repr_parser

logger = logging.getLogger(__name__)


class JSONFileImporter:
    def __init__(self, archiver: Archiver, file_path: str):
        self.archiver = archiver
        self.file_path = pathlib.Path(file_path)
        self.chat_id = int(self.file_path.name.split(".")[0])
        self.chat_db = ChatDatabase(self.chat_id)
        self.file_datetime = datetime.datetime.fromtimestamp(os.path.getctime(self.file_path), tz=datetime.timezone.utc)
        self.seen_user_ids: set[int] = set()
        self.str_parser = str_repr_parser()
        self.known_admin_event_ids: set[int] = set()
        self.known_message_ids: set[int] = set()

    async def _process_admin_event(
            self,
            admin_event: dict,
    ) -> None:
        logger.info("Processing admin event ID: %s", admin_event["id"])
        if admin_event["id"] in self.known_admin_event_ids:
            logger.info("Already archived that one")
            return
        evt_str = admin_event["str"]
        evt_str_obj: StrReprObj = self.str_parser.parse_string(evt_str)[0]
        if evt_str_obj.to_str() != evt_str:
            raise ValueError("AAAAAA: Couldn't parse and re-encode event object without changes")
        evt_obj = AdminEvent.from_str_repr_obj(self.file_datetime, evt_str_obj)
        self.chat_db.save_admin_event(evt_obj)
        if evt_str_obj.values_dict["action"].class_name == "ChannelAdminLogEventActionDeleteMessage":
            msg_str_obj = evt_str_obj.values_dict["action"].values_dict["message"]
            msg_obj = Message.from_str_repr_obj(self.file_datetime, msg_str_obj, deleted=True)
            await self._save_message(msg_obj)
        if evt_str_obj.values_dict["action"].class_name == "ChannelAdminLogEventActionEditMessage":
            prev_msg_str_obj = evt_str_obj.values_dict["action"].values_dict["prev_message"]
            new_msg_str_obj = evt_str_obj.values_dict["action"].values_dict["new_message"]
            prev_msg_obj = Message.from_str_repr_obj(self.file_datetime, prev_msg_str_obj)
            new_msg_obj = Message.from_str_repr_obj(self.file_datetime, new_msg_str_obj)
            await self._save_message(prev_msg_obj)
            await self._save_message(new_msg_obj)
        self.known_admin_event_ids.add(evt_obj.resource_id)

    async def _process_message(
            self,
            message: dict,
    ) -> None:
        logger.info("Processing message ID: %s", message["id"])
        if message["id"] in self.known_message_ids:
            logger.info("Already archived that one")
            return
        # Parse the message from a string
        msg_str = message["str"]
        msg_str_obj = StrReprObj.parse_str_repr(msg_str)
        if msg_str_obj.to_str() != msg_str:
            raise ValueError("AAAAAA")
        msg_obj = Message.from_str_repr_obj(self.file_datetime, msg_str_obj)
        await self._save_message(msg_obj)
        self.known_message_ids.add(msg_obj.resource_id)

    async def _save_message(self, msg_obj: Message) -> None:
        self.chat_db.save_message(msg_obj)
        # Save users
        if msg_obj.user_id not in self.seen_user_ids:
            await self.archiver.peer_fetcher.queue_user(None, self.chat_id, self.chat_db, msg_obj.user_id)
        # Check stickers and cry
        if msg_obj.sticker_id is not None and msg_obj.sticker_set_id is not None:
            matches = glob.glob(f"store/stickers/{msg_obj.sticker_set_id}/{msg_obj.sticker_id}.*")
            if not matches:
                raise ValueError("AAAAA Missing sticker for message ID: %s", msg_obj.resource_id)
            else:
                logger.info("Sticker for message ID: %s already downloaded, phew", msg_obj.resource_id)
            return
        # Copy media over if you can
        if msg_obj.media_id is not None:
            # Check if file exists in media folder for chat
            matches = glob.glob(f"store/chats/{self.chat_id}/media/{msg_obj.media_id}.*")
            if matches:
                logger.info("File ID %s already exists for message id: %s", msg_obj.media_id, msg_obj.resource_id)
            else:
                matches = glob.glob(f"store/media/{msg_obj.media_id}.*")
                if matches:
                    file_ext = matches[0].split(".")[-1]
                    logger.info("Moving media file ID %s into chat directory for message ID: %s", msg_obj.media_id, msg_obj.resource_id)
                    shutil.copyfile(matches[0], f"store/chats/{self.chat_id}/media/{msg_obj.media_id}.{file_ext}")
                    logger.info("Moved media file")
                else:
                    logger.error("Missing media file ID: %s for message ID: %s", msg_obj.media_id, msg_obj.resource_id)
                    # raise ValueError("AAAA Missing media file ID: %s for message ID: %s", msg_obj.media_id, msg_obj.resource_id)

    async def run(self) -> None:
        self.known_admin_event_ids = self.chat_db.list_admin_event_ids_by_archive_datetime(self.file_datetime)
        self.known_message_ids = self.chat_db.list_message_ids_by_archive_datetime(self.file_datetime)
        with open(self.file_path, "r") as f:
            json_data = json.load(f)
        for admin_event in json_data["admin_events"]:
            await self._process_admin_event(admin_event)
        for message in json_data.get("messages", []):
            await self._process_message(message)


def main() -> None:
    setup_logging("INFO")
    conf = load_config()
    archiver = Archiver(conf)
    for json_path in glob.glob("store/*.json"):
        logger.info("Importing file: %s", json_path)
        importer = JSONFileImporter(archiver, json_path)
        importer.chat_db.start()
        asyncio.run(importer.run())
        importer.chat_db.stop()


if __name__ == "__main__":
    main()
