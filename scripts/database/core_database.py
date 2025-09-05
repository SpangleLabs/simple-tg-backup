from contextlib import closing

from scripts.database.abstract_database import AbstractDatabase, storable_date
from scripts.database.core_db_migrations import InitialCoreDatabase
from scripts.database.migration import DBMigration
from scripts.models.chat import Chat


class CoreDatabase(AbstractDatabase):

    def file_path(self) -> str:
        return "store/core_db.sqlite"

    def list_migrations(self) -> list[DBMigration]:
        return [InitialCoreDatabase()]
