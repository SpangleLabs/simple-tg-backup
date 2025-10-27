import pathlib
import sqlite3
from contextlib import closing

from tg_backup.database.migration import DBMigration


class InitialChatDatabase(DBMigration):
    @property
    def migration_id(self) -> int:
        return 1

    @property
    def migration_name(self) -> str:
        return "initial_setup"

    @property
    def is_initial_setup(self) -> bool:
        return True

    def execute(self, conn: sqlite3.Connection) -> None:
        with open(pathlib.Path(__file__).parent / "chat_schema.sql") as f:
            schema_str = f.read()
        with closing(conn.cursor()) as cursor:
            cursor.executescript(schema_str)

class AddWebPageMediaTable(DBMigration):
    @property
    def migration_id(self) -> int:
        return 5

    @property
    def migration_name(self) -> str:
        return "add_web_page_media_table"

    def execute(self, conn: sqlite3.Connection) -> None:
        with open(pathlib.Path(__file__).parent / "chat_migration_005_web_page_media.sql") as f:
            schema_str = f.read()
        with closing(conn.cursor()) as cursor:
            cursor.executescript(schema_str)
