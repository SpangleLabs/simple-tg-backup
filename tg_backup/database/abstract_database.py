import datetime
import logging
import os
import pathlib
import sqlite3
from abc import ABC, abstractmethod
from contextlib import closing
from typing import TYPE_CHECKING, Optional

from tg_backup.models.chat import Chat
from tg_backup.models.user import User
from tg_backup.utils.json_encoder import encode_json, encode_optional_json, decode_optional_json, decode_json_dict

if TYPE_CHECKING:
    from tg_backup.database.migration import DBMigration


logger = logging.getLogger(__name__)


def storable_date(date_val: Optional[datetime.date]) -> Optional[str]:
    if date_val is None:
        return None
    return date_val.isoformat()


def parsable_date(date_val: Optional[str]) -> Optional[datetime.date]:
    if date_val is None:
        return None
    return datetime.datetime.fromisoformat(date_val)


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
        self.conn = None

    def is_connected(self) -> bool:
        return self.conn is not None

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
                logger.info("Running database migration %s on %s", migration.migration_name, type(self).__name__)
                try:
                    migration.execute(self.conn)
                except Exception as e:
                    logger.error(f"Failed to apply migration {migration.migration_name} on database {self.file_path()}")
                    raise e
                end_time = datetime.datetime.now(datetime.timezone.utc)
                self.save_migration_data(migration.migration_id, migration.migration_name, start_time, end_time)
                continue
            if migration_row["end_time"] is None:
                raise ValueError(f"Migration {migration.migration_id} ({migration.migration_name}) was started, but not completed, for database: {self.file_path()}")

    def save_chat(self, chat: Chat) -> None:
        with closing(self.conn.cursor()) as cursor:
            cursor.execute(
                "INSERT INTO chats (archive_datetime, archive_tl_scheme_layer, id, type, str_repr, dict_repr, title, creation_date, is_creator, have_left, is_broadcast_channel, participants_count, about, username, other_usernames, migrated_to_chat_id, migrated_from_chat_id, linked_chat_id)"
                " VALUES (:archive_datetime, :archive_tl_scheme_layer, :id, :type, :str_repr, :dict_repr, :title, :creation_date, :is_creator, :have_left, :is_broadcast_channel, :participants_count, :about, :username, :other_usernames, :migrated_to_chat_id, :migrated_from_chat_id, :linked_chat_id)",
                {
                    "archive_datetime": storable_date(chat.archive_datetime),
                    "archive_tl_scheme_layer": chat.archive_tl_schema_layer,
                    "id": chat.resource_id,
                    "type": chat.resource_type,
                    "str_repr": chat.str_repr,
                    "dict_repr": encode_json(chat.dict_repr),
                    "title": chat.title,
                    "creation_date": storable_date(chat.creation_date),
                    "is_creator": chat.is_creator,
                    "have_left": chat.have_left,
                    "is_broadcast_channel": chat.broadcast_channel,
                    "participants_count": chat.participants_count,
                    "about": chat.about,
                    "username": chat.username,
                    "other_usernames": encode_optional_json(chat.other_usernames),
                    "migrated_to_chat_id": chat.migrated_to_chat_id,
                    "migrated_from_chat_id": chat.migrated_from_chat_id,
                    "linked_chat_id": chat.linked_chat_id,
                }
            )
            self.conn.commit()

    def list_chats(self) -> list[Chat]:
        chats: list[Chat] = []
        with closing(self.conn.cursor()) as cursor:
            resp = cursor.execute(
                "SELECT archive_datetime, archive_tl_scheme_layer, id, type, str_repr, dict_repr, title, creation_date, is_creator, have_left, is_broadcast_channel, participants_count, about, username, other_usernames, migrated_to_chat_id, migrated_from_chat_id, linked_chat_id FROM chats",
            )
            for row in resp.fetchall():
                chat = Chat(
                    archive_datetime=datetime.datetime.fromisoformat(row["archive_datetime"]),
                    archive_tl_schema_layer=row["archive_tl_scheme_layer"],
                    resource_id=row["id"],
                    resource_type=row["type"],
                    str_repr=row["str_repr"],
                    dict_repr=decode_json_dict(row["dict_repr"]),
                )
                chat.title = row["title"]
                chat.creation_date = parsable_date(row["creation_date"])
                chat.is_creator = row["is_creator"]
                chat.have_left = row["have_left"]
                chat.broadcast_channel = row["is_broadcast_channel"]
                chat.participants_count = row["participants_count"]
                chat.about = row["about"]
                chat.username = row["username"]
                chat.other_usernames = decode_optional_json(row["other_usernames"])
                chat.migrated_to_chat_id = row["migrated_to_chat_id"]
                chat.migrated_from_chat_id = row["migrated_from_chat_id"]
                chat.linked_chat_id = row["linked_chat_id"]
                chats.append(chat)
        return chats

    def save_user(self, user: User) -> None:
        with closing(self.conn.cursor()) as cursor:
            cursor.execute(
                "INSERT INTO users (archive_datetime, archive_tl_scheme_layer, id, type, str_repr, dict_repr, bio, birthday, is_bot, is_deleted, first_name, last_name, phone_number, has_premium, username, other_usernames)"
                " VALUES (:archive_datetime, :archive_tl_scheme_layer, :id, :type, :str_repr, :dict_repr, :bio, :birthday, :is_bot, :is_deleted, :first_name, :last_name, :phone_number, :has_premium, :username, :other_usernames)",
                {
                    "archive_datetime": storable_date(user.archive_datetime),
                    "archive_tl_scheme_layer": user.archive_tl_schema_layer,
                    "id": user.resource_id,
                    "type": user.resource_type,
                    "str_repr": user.str_repr,
                    "dict_repr": encode_json(user.dict_repr),
                    "bio": user.bio,
                    "birthday": storable_date(user.birthday),
                    "is_bot": user.is_bot,
                    "is_deleted": user.is_deleted,
                    "first_name": user.first_name,
                    "last_name": user.last_name,
                    "phone_number": user.phone_number,
                    "has_premium": user.has_premium,
                    "username": user.username,
                    "other_usernames": encode_optional_json(user.other_usernames),
                }
            )
            self.conn.commit()

    def list_users(self) -> list[User]:
        users: list[User] = []
        with closing(self.conn.cursor()) as cursor:
            resp = cursor.execute(
                "SELECT archive_datetime, archive_tl_scheme_layer, id, type, str_repr, dict_repr, bio, birthday, is_bot, is_deleted, first_name, last_name, phone_number, has_premium, username, other_usernames FROM users",
            )
            for row in resp.fetchall():
                user = User(
                    archive_datetime=datetime.datetime.fromisoformat(row["archive_datetime"]),
                    archive_tl_schema_layer=row["archive_tl_scheme_layer"],
                    resource_id=row["id"],
                    resource_type=row["type"],
                    str_repr=row["str_repr"],
                    dict_repr=decode_json_dict(row["dict_repr"]),
                )
                user.bio = row["bio"]
                user.birthday = parsable_date(row["birthday"])
                user.is_bot = row["is_bot"]
                user.is_deleted = row["is_deleted"]
                user.first_name = row["first_name"]
                user.last_name = row["last_name"]
                user.phone_number = row["phone_number"]
                user.has_premium = row["has_premium"]
                user.username = row["username"]
                user.other_usernames = decode_optional_json(row["other_usernames"])
                users.append(user)
        return users