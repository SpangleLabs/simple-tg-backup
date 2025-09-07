import json
from contextlib import closing

from tg_backup.database.abstract_database import AbstractDatabase, storable_date
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

    def save_message(self, message: Message) -> None:
        with closing(self.conn.cursor()) as cursor:
            cursor.execute(
                "INSERT INTO messages (archive_datetime, archive_tl_scheme_layer, id, type, str_repr, dict_repr, datetime, text, media_id, user_id, deleted)"
                " VALUES (:archive_datetime, :archive_tl_scheme_layer, :id, :type, :str_repr, :dict_repr, :datetime, :text, :media_id, :user_id, :deleted)",
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
                    "deleted": message.deleted,
                }
            )
            self.conn.commit()
