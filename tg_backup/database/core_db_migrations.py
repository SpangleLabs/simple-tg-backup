import pathlib
import sqlite3
from contextlib import closing

from tg_backup.database.migration import DBMigration


class InitialCoreDatabase(DBMigration):
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
        with open(pathlib.Path(__file__).parent / "core_schema.sql") as f:
            schema_str = f.read()
        with closing(conn.cursor()) as cursor:
            cursor.executescript(schema_str)


class ExtraChatColumns(DBMigration):
    @property
    def migration_id(self) -> int:
        return 2

    @property
    def migration_name(self) -> str:
        return "extra_chat_columns"

    def execute(self, conn: sqlite3.Connection) -> None:
        with open(pathlib.Path(__file__).parent / "core_migration_002_chat_columns.sql") as f:
            schema_str = f.read()
        with closing(conn.cursor()) as cursor:
            cursor.executescript(schema_str)