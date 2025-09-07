from tg_backup.database.abstract_database import AbstractDatabase
from tg_backup.database.core_db_migrations import InitialCoreDatabase
from tg_backup.database.migration import DBMigration


class CoreDatabase(AbstractDatabase):

    def file_path(self) -> str:
        return "store/core_db.sqlite"

    def list_migrations(self) -> list[DBMigration]:
        return [InitialCoreDatabase()]
