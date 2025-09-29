import datetime
import json
from contextlib import closing

from tg_backup.database.abstract_database import AbstractDatabase, storable_date
from tg_backup.database.core_db_migrations import InitialCoreDatabase
from tg_backup.database.migration import DBMigration
from tg_backup.models.sticker import Sticker
from tg_backup.models.sticker_set import StickerSet
from tg_backup.utils.json_encoder import encode_json_extra


class CoreDatabase(AbstractDatabase):

    def file_path(self) -> str:
        return "store/core_db.sqlite"

    def list_migrations(self) -> list[DBMigration]:
        return [InitialCoreDatabase()]

    def save_sticker(self, sticker: Sticker) -> None:
        with closing(self.conn.cursor()) as cursor:
            cursor.execute(
                "INSERT INTO stickers (archive_datetime, archive_tl_scheme_layer, id, type, str_repr, dict_repr, sticker_set_id, emoji, file_name, sticker_upload_date)"
                " VALUES (:archive_datetime, :archive_tl_scheme_layer, :id, :type, :str_repr, :dict_repr, :sticker_set_id, :emoji, :file_name, :sticker_upload_date)",
                {
                    "archive_datetime": storable_date(sticker.archive_datetime),
                    "archive_tl_scheme_layer": sticker.archive_tl_schema_layer,
                    "id": sticker.resource_id,
                    "type": sticker.resource_type,
                    "str_repr": sticker.str_repr,
                    "dict_repr": json.dumps(sticker.dict_repr, default=encode_json_extra),
                    "sticker_set_id": sticker.sticker_set_id,
                    "emoji": sticker.emoji,
                    "file_name": sticker.file_name,
                    "sticker_upload_date": storable_date(sticker.sticker_upload_date),
                }
            )
            self.conn.commit()

    def save_sticker_set(self, sticker_set: StickerSet) -> None:
        with closing(self.conn.cursor()) as cursor:
            cursor.execute(
                "INSERT INTO sticker_sets (archive_datetime, archive_tl_scheme_layer, id, type, str_repr, dict_repr, handle, title, sticker_count)"
                " VALUES (:archive_datetime, :archive_tl_scheme_layer, :id, :type, :str_repr, :dict_repr, :handle, :title, :sticker_count)",
                {
                    "archive_datetime": storable_date(sticker_set.archive_datetime),
                    "archive_tl_scheme_layer": sticker_set.archive_tl_schema_layer,
                    "id": sticker_set.resource_id,
                    "type": sticker_set.resource_type,
                    "str_repr": sticker_set.str_repr,
                    "dict_repr": json.dumps(sticker_set.dict_repr, default=encode_json_extra),
                    "handle": sticker_set.handle,
                    "title": sticker_set.title,
                    "sticker_count": sticker_set.sticker_count,
                }
            )
            self.conn.commit()

    def list_sticker_sets(self) -> list[StickerSet]:
        sets: list[StickerSet] = []
        with closing(self.conn.cursor()) as cursor:
            resp = cursor.execute(
                "SELECT archive_datetime, archive_tl_scheme_layer, id, type, str_repr, dict_repr, handle, title, sticker_count FROM sticker_sets",
            )
            for row in resp.fetchall():
                sticker_set = StickerSet(
                    archive_datetime=datetime.datetime.fromisoformat(row["archive_datetime"]),
                    archive_tl_schema_layer=row["archive_tl_scheme_layer"],
                    resource_id=row["id"],
                    resource_type=row["type"],
                    str_repr=row["str_repr"],
                    dict_repr=json.loads(row["dict_repr"]),
                )
                sticker_set.handle = row["handle"]
                sticker_set.title = row["title"]
                sticker_set.sticker_count = row["sticker_count"]
                sets.append(sticker_set)
        return sets
