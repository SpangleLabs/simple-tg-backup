from scripts.database.abstract_database import AbstractDatabase
from scripts.database.chat_db_migrations import InitialChatDatabase
from scripts.database.migration import DBMigration


class ChatDatabase(AbstractDatabase):

    def __init__(self, chat_id: int):
        super().__init__()
        self.chat_id = chat_id

    def file_path(self) -> str:
        return f"store/chats/{self.chat_id}/chat_db.sqlite"

    def list_migrations(self) -> list[DBMigration]:
        return [InitialChatDatabase()]
