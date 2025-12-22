import pathlib
import sqlite3
from abc import ABC, abstractmethod
from contextlib import closing


class DBMigration(ABC):

    @property
    @abstractmethod
    def migration_id(self) -> int:
        raise NotImplementedError()

    @property
    @abstractmethod
    def migration_name(self) -> str:
        raise NotImplementedError()

    @property
    def is_initial_setup(self) -> bool:
        return False

    @abstractmethod
    def execute(self, conn: sqlite3.Connection) -> None:
        raise NotImplementedError()

    def _execute_script(self, conn: sqlite3.Connection, script_filename: str) -> None:
        with open(pathlib.Path(__file__).parent / script_filename) as f:
            schema_str = f.read()
        with closing(conn.cursor()) as cursor:
            cursor.executescript(schema_str)

