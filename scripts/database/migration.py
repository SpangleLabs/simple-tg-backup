import sqlite3
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from scripts.database.abstract_database import AbstractDatabase


class DBMigration(ABC):

    @property
    @abstractmethod
    def migration_id(self) -> int:
        raise NotImplementedError()

    @property
    @abstractmethod
    def migration_name(self) -> str:
        raise NotImplementedError()

    @abstractmethod
    def execute(self, conn: sqlite3.Connection) -> None:
        raise NotImplementedError()

