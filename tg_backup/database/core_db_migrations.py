import sqlite3

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
        self._execute_script(conn, "core_schema.sql")


class ExtraChatColumns(DBMigration):
    @property
    def migration_id(self) -> int:
        return 2

    @property
    def migration_name(self) -> str:
        return "extra_chat_columns"

    def execute(self, conn: sqlite3.Connection) -> None:
        self._execute_script(conn, "core_migration_002_chat_columns.sql")


class ArchiveRecordTable(DBMigration):
    @property
    def migration_id(self) -> int:
        return 3

    @property
    def migration_name(self) -> str:
        return "archive_record_table"

    def execute(self, conn: sqlite3.Connection) -> None:
        self._execute_script(conn, "core_migration_003_archive_runs_table.sql")


class DialogsTable(DBMigration):
    @property
    def migration_id(self) -> int:
        return 4

    @property
    def migration_name(self) -> str:
        return "dialog_objects_table"

    def execute(self, conn: sqlite3.Connection) -> None:
        self._execute_script(conn, "core_migration_004_dialogs_table.sql")


class DialogsTakeoutColumns(DBMigration):
    @property
    def migration_id(self) -> int:
        return 6

    @property
    def migration_name(self) -> str:
        return "dialog_takeout_columns"

    def execute(self, conn: sqlite3.Connection) -> None:
        self._execute_script(conn, "core_migration_006_dialogs_takeout_columns.sql")


class ArchiveRunMergeTimers(DBMigration):
    @property
    def migration_id(self) -> int:
        return 7

    @property
    def migration_name(self) -> str:
        return "archive_run_merge_timers"

    def execute(self, conn: sqlite3.Connection) -> None:
        self._execute_script(conn, "core_migration_007_archive_run_merge_timers.sql")
