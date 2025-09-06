import json
from contextlib import closing

from scripts.database.abstract_database import AbstractDatabase, storable_date
from scripts.database.chat_db_migrations import InitialChatDatabase
from scripts.database.migration import DBMigration
from scripts.models.admin_event import AdminEvent
from scripts.utils.json_encoder import encode_json_extra


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
