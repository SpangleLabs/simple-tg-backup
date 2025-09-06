import datetime
import json
import os
import pathlib
import sqlite3
from abc import ABC, abstractmethod
from contextlib import closing
from typing import TYPE_CHECKING, Optional

from scripts.models.chat import Chat
from scripts.utils.json_encoder import encode_json_extra

if TYPE_CHECKING:
    from scripts.database.migration import DBMigration


def storable_date(date_val: Optional[datetime.date]) -> Optional[str]:
    if date_val is None:
        return None
    return date_val.isoformat()


class AbstractDatabase(ABC):
    def __init__(self) -> None:
        self.conn: Optional[sqlite3.Connection] = None

    @abstractmethod
    def file_path(self) -> str:
        raise NotImplementedError()

    def start(self) -> None:
        file_path = self.file_path()
        os.makedirs(pathlib.Path(file_path).parent, exist_ok=True)
        self.conn = sqlite3.connect(file_path)
        self.conn.row_factory = sqlite3.Row
        self.apply_migrations()

    def stop(self) -> None:
        self.conn.close()

    @abstractmethod
    def list_migrations(self) -> list["DBMigration"]:
        raise NotImplementedError()

    def get_migration_data(self, migration_id: int) -> Optional[sqlite3.Row]:
        try:
            with closing(self.conn.cursor()) as cursor:
                resp = cursor.execute("SELECT * FROM db_migrations where migration_id = ?", (migration_id,))
                return resp.fetchone()
        except sqlite3.OperationalError as e:
            if "no such table: db_migrations" in str(e):
                return None
            raise e

    def save_migration_data(self, migration_id: int, migration_name: str, start_time: datetime.datetime, end_time: Optional[datetime.datetime]) -> None:
        with closing(self.conn.cursor()) as cursor:
            cursor.execute(
                "INSERT INTO db_migrations (migration_id, migration_name, start_time, end_time)"
                " VALUES (:migration_id, :migration_name, :start_time, :end_time)"
                " ON CONFLICT (migration_id)"
                " DO UPDATE SET migration_name = :migration_name, start_time = :start_time, end_time = :end_time",
                {
                    "migration_id": migration_id,
                    "migration_name": migration_name,
                    "start_time": storable_date(start_time),
                    "end_time": storable_date(end_time),
                }
            )
            self.conn.commit()

    def apply_migrations(self) -> None:
        migrations = sorted(self.list_migrations(), key=lambda m: m.migration_id)
        for migration in migrations:
            migration_row = self.get_migration_data(migration.migration_id)
            if migration_row is None or migration_row["start_time"] is None:
                start_time = datetime.datetime.now(datetime.timezone.utc)
                if not migration.is_initial_setup:
                    self.save_migration_data(migration.migration_id, migration.migration_name, start_time, None)
                migration.execute(self.conn)
                end_time = datetime.datetime.now(datetime.timezone.utc)
                self.save_migration_data(migration.migration_id, migration.migration_name, start_time, end_time)
                continue
            if migration_row["end_time"] is None:
                raise ValueError(f"Migration {migration.migration_id} ({migration.migration_name}) was started, but not completed, for database: {self.file_path()}")

    def save_chat(self, chat: Chat) -> None:
        with closing(self.conn.cursor()) as cursor:
            cursor.execute(
                "INSERT INTO chats (archive_datetime, archive_tl_scheme_layer, id, type, str_repr, dict_repr, title)"
                " VALUES (:archive_datetime, :archive_tl_scheme_layer, :id, :type, :str_repr, :dict_repr, :title)",
                {
                    "archive_datetime": storable_date(chat.archive_datetime),
                    "archive_tl_scheme_layer": chat.archive_tl_schema_layer,
                    "id": chat.resource_id,
                    "type": chat.resource_type,
                    "str_repr": chat.str_repr,
                    "dict_repr": json.dumps(chat.dict_repr, default=encode_json_extra),
                    "title": chat.title,
                }
            )
            self.conn.commit()
