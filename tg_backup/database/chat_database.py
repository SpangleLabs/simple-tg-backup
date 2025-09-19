import datetime
import json
from contextlib import closing

from tg_backup.database.abstract_database import AbstractDatabase, storable_date, parsable_date
from tg_backup.database.chat_db_migrations import InitialChatDatabase
from tg_backup.database.migration import DBMigration
from tg_backup.models.admin_event import AdminEvent
from tg_backup.models.message import Message
from tg_backup.utils.json_encoder import encode_json_extra


class ChatDatabase(AbstractDatabase):

    def __init__(self, chat_id: int):
        super().__init__()
        self.chat_id = chat_id

    def file_path(self) -> str:
        return f"store/chats/{self.chat_id}/chat_db.sqlite"

    def list_migrations(self) -> list[DBMigration]:
        return [InitialChatDatabase()]

    def save_admin_event(self, admin_event: AdminEvent) -> None:
        with closing(self.conn.cursor()) as cursor:
            cursor.execute(
                "INSERT INTO admin_events (archive_datetime, archive_tl_scheme_layer, id, type, str_repr, dict_repr, datetime, message_id)"
                " VALUES (:archive_datetime, :archive_tl_scheme_layer, :id, :type, :str_repr, :dict_repr, :datetime, :message_id)",
                {
                    "archive_datetime": storable_date(admin_event.archive_datetime),
                    "archive_tl_scheme_layer": admin_event.archive_tl_schema_layer,
                    "id": admin_event.resource_id,
                    "type": admin_event.resource_type,
                    "str_repr": admin_event.str_repr,
                    "dict_repr": json.dumps(admin_event.dict_repr, default=encode_json_extra),
                    "datetime": storable_date(admin_event.datetime),
                    "message_id": admin_event.message_id
                }
            )
            self.conn.commit()

    def list_admin_event_ids_by_archive_datetime(self, archive_datetime: datetime.datetime) -> set[int]:
        evt_ids = set()
        with closing(self.conn.cursor()) as cursor:
            resp = cursor.execute(
                "SELECT DISTINCT id FROM admin_events WHERE archive_datetime = :archive_datetime",
                {
                    "archive_datetime": archive_datetime.isoformat(),
                }
            )
            for row in resp.fetchall():
                evt_ids.add(row["id"])
        return evt_ids

    def save_message(self, message: Message) -> None:
        with closing(self.conn.cursor()) as cursor:
            cursor.execute(
                "INSERT INTO messages (archive_datetime, archive_tl_scheme_layer, id, type, str_repr, dict_repr, datetime, text, media_id, user_id, sticker_id, sticker_set_id, deleted, edit_datetime)"
                " VALUES (:archive_datetime, :archive_tl_scheme_layer, :id, :type, :str_repr, :dict_repr, :datetime, :text, :media_id, :user_id, :sticker_id, :sticker_set_id, :deleted, :edit_datetime)",
                {
                    "archive_datetime": storable_date(message.archive_datetime),
                    "archive_tl_scheme_layer": message.archive_tl_schema_layer,
                    "id": message.resource_id,
                    "type": message.resource_type,
                    "str_repr": message.str_repr,
                    "dict_repr": json.dumps(message.dict_repr, default=encode_json_extra),
                    "datetime": storable_date(message.datetime),
                    "text": message.text,
                    "media_id": message.media_id,
                    "user_id": message.user_id,
                    "sticker_id": message.sticker_id,
                    "sticker_set_id": message.sticker_set_id,
                    "deleted": message.deleted,
                    "edit_datetime": storable_date(message.edit_datetime),
                }
            )
            self.conn.commit()

    def get_messages(self, msg_id: int) -> list[Message]:
        msgs = []
        with closing(self.conn.cursor()) as cursor:
            resp = cursor.execute(
                "SELECT  archive_datetime, archive_tl_scheme_layer, id, type, str_repr, dict_repr, datetime, text, media_id, user_id, sticker_id, sticker_set_id, deleted, edit_datetime"
                " FROM messages "
                " WHERE id = :msg_id",
                {
                    "msg_id": msg_id,
                }
            )
            for row in resp.fetchall():
                msg = Message(
                    archive_datetime=datetime.datetime.fromisoformat(row["archive_datetime"]),
                    archive_tl_schema_layer=row["archive_tl_scheme_layer"],
                    resource_id=row["id"],
                    resource_type=row["type"],
                    str_repr=row["str_repr"],
                    dict_repr=json.loads(row["dict_repr"]),
                )
                msg.datetime = parsable_date(row["datetime"])
                msg.text = row["text"]
                msg.media_id = row["media_id"]
                msg.user_id = row["user_id"]
                msg.sticker_id = row["sticker_id"]
                msg.sticker_set_id = row["sticker_set_id"]
                msg.deleted = bool(row["deleted"])
                msg.edit_datetime = parsable_date(row["edit_datetime"])
                msgs.append(msg)
        return msgs

    def list_message_ids(self) -> set[int]:
        msg_ids = set()
        with closing(self.conn.cursor()) as cursor:
            resp = cursor.execute("SELECT DISTINCT id FROM messages")
            for row in resp.fetchall():
                msg_ids.add(row["id"])
        return msg_ids

    def list_message_ids_by_archive_datetime(self, archive_datetime: datetime.datetime) -> set[int]:
        msg_ids = set()
        with closing(self.conn.cursor()) as cursor:
            resp = cursor.execute(
                "SELECT DISTINCT id FROM messages WHERE archive_datetime = :archive_datetime",
                {
                    "archive_datetime": archive_datetime.isoformat(),
                }
            )
            for row in resp.fetchall():
                msg_ids.add(row["id"])
        return msg_ids

    def delete_messages(self, msg_id: int) -> None:
        with closing(self.conn.cursor()) as cursor:
            cursor.execute(
                "DELETE FROM messages WHERE id = :msg_id",
                {
                    "msg_id": msg_id,
                }
            )
            self.conn.commit()
