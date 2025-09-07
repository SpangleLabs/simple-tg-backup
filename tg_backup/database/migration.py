import sqlite3
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING


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

