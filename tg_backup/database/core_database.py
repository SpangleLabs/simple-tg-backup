import datetime
import json
from contextlib import closing

from prometheus_client import Gauge

from tg_backup.config import BehaviourConfig
from tg_backup.database.abstract_database import AbstractDatabase, storable_date, parsable_date
from tg_backup.database.core_db_migrations import InitialCoreDatabase, ExtraChatColumns, ArchiveRecordTable, \
    DialogsTable
from tg_backup.database.migration import DBMigration
from tg_backup.models.archive_run_record import ArchiveRunRecord, ArchiveRunStats
from tg_backup.utils.dialog_type import DialogType
from tg_backup.models.dialog import Dialog
from tg_backup.models.sticker import Sticker
from tg_backup.models.sticker_set import StickerSet
from tg_backup.utils.json_encoder import encode_json_extra


count_archive_runs = Gauge(
    "tgbackup_coredb_archive_runs_count",
    "Total number of archive runs which are stored in the database, as of last database check",
)
count_dialogs = Gauge(
    "tgbackup_coredb_dialogs_count",
    "Total number of dialogs which are stored in the database, as of last database check",
)


class CoreDatabase(AbstractDatabase):

    def start(self) -> None:
        super().start()
        # Initialise metrics
        self.count_dialogs()
        self.count_archive_runs()

    def file_path(self) -> str:
        return "store/core_db.sqlite"

    def list_migrations(self) -> list[DBMigration]:
        return [
            InitialCoreDatabase(),
            ExtraChatColumns(),
            ArchiveRecordTable(),
            DialogsTable(),
        ]

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

    def save_archive_run(self, archive_run: ArchiveRunRecord) -> None:
        with closing(self.conn.cursor()) as cursor:
            cursor.execute(
                "INSERT INTO archive_runs "
                " (archive_run_id, target_type, target_id, time_queued, history_time_start, history_time_latest, history_time_end, follow_time_start, follow_time_latest, follow_time_end, behaviour_config, completed, failure_reason, archive_stats)"
                " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) "
                " ON CONFLICT (archive_run_id) DO UPDATE SET"
                " target_type=excluded.target_type, target_id=excluded.target_id, time_queued=excluded.time_queued,"
                " history_time_start=excluded.history_time_start, history_time_latest=excluded.history_time_latest, history_time_end=excluded.history_time_end,"
                " follow_time_start=excluded.follow_time_start, follow_time_latest=excluded.follow_time_latest, follow_time_end=excluded.follow_time_end,"
                " behaviour_config=excluded.behaviour_config, completed=excluded.completed, failure_reason=excluded.failure_reason, archive_stats=excluded.archive_stats",
                (
                    archive_run.archive_run_id,
                    archive_run.target_type.value,
                    archive_run.target_id,
                    archive_run.time_queued.isoformat(),
                    storable_date(archive_run.archive_history_timer.start_time),
                    storable_date(archive_run.archive_history_timer.latest_msg_time),
                    storable_date(archive_run.archive_history_timer.end_time),
                    storable_date(archive_run.follow_live_timer.start_time),
                    storable_date(archive_run.follow_live_timer.latest_msg_time),
                    storable_date(archive_run.follow_live_timer.end_time),
                    json.dumps(archive_run.behaviour_config.to_dict(), default=encode_json_extra),
                    archive_run.completed,
                    archive_run.failure_reason,
                    json.dumps(archive_run.archive_stats.to_dict(), default=encode_json_extra),
                )
            )
            self.conn.commit()
        self.count_archive_runs()

    def count_archive_runs(self) -> int:
        with closing(self.conn.cursor()) as cursor:
            resp = cursor.execute("SELECT COUNT(*) FROM archive_runs")
            count = int(resp.fetchone()[0])
            count_archive_runs.set(count)
            return count

    def list_archive_runs(self) -> list[ArchiveRunRecord]:
        records = []
        with closing(self.conn.cursor()) as cursor:
            resp = cursor.execute(
                "SELECT archive_run_id, target_type, target_id, time_queued, history_time_start, history_time_latest, history_time_end, follow_time_start, follow_time_latest, follow_time_end, behaviour_config, completed, failure_reason, archive_stats "
                " FROM archive_runs"
            )
            for row in resp.fetchall():
                record = ArchiveRunRecord(
                    target_type=DialogType.from_str(row["target_type"]),
                    target_id=row["target_id"],
                    core_db=self,
                    time_queued=parsable_date(row["time_queued"]),
                    history_time_start=parsable_date(row["history_time_start"]),
                    history_time_latest=parsable_date(row["history_time_latest"]),
                    history_time_end=parsable_date(row["history_time_end"]),
                    follow_time_start=parsable_date(row["follow_time_start"]),
                    follow_time_latest=parsable_date(row["follow_time_latest"]),
                    follow_time_end=parsable_date(row["follow_time_end"]),
                    behaviour_config=BehaviourConfig.from_dict(json.loads(row["behaviour_config"])),
                    completed=row["completed"] == 1,
                    failure_reason=row["failure_reason"],
                    archive_run_id=row["archive_run_id"],
                )
                record.archive_stats = ArchiveRunStats.from_dict(record, json.loads(row["archive_stats"]))
                records.append(record)
        count_archive_runs.set(len(records))
        return records

    def save_dialog(self, dialog: Dialog) -> None:
        with closing(self.conn.cursor()) as cursor:
            cursor.execute(
                # Note that we don't update first_seen on conflict
                "INSERT INTO dialogs (archive_datetime, archive_tl_scheme_layer, id, type, str_repr, dict_repr, chat_type, name, pinned, archived_chat, last_msg_date, first_seen, last_seen) "
                " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) "
                " ON CONFLICT(id) DO UPDATE SET "
                " archive_datetime=excluded.archive_datetime, archive_tl_scheme_layer=excluded.archive_tl_scheme_layer, "
                " type=excluded.type, str_repr=excluded.str_repr, dict_repr=excluded.dict_repr, chat_type=excluded.chat_type, "
                " name=excluded.name, pinned=excluded.pinned, archived_chat=excluded.archived_chat, "
                " last_msg_date=excluded.last_msg_date, last_seen=excluded.last_seen",
                (
                    storable_date(dialog.archive_datetime),
                    dialog.archive_tl_schema_layer,
                    dialog.resource_id,
                    dialog.resource_type,
                    dialog.str_repr,
                    json.dumps(dialog.dict_repr, default=encode_json_extra),
                    dialog.chat_type.value,
                    dialog.name,
                    dialog.pinned,
                    dialog.archived_chat,
                    storable_date(dialog.last_msg_date),
                    storable_date(dialog.first_seen),
                    storable_date(dialog.last_seen)
                )
            )
            self.conn.commit()
        self.count_dialogs()

    def count_dialogs(self) -> int:
        with closing(self.conn.cursor()) as cursor:
            resp = cursor.execute("SELECT COUNT(*) FROM dialogs")
            count = int(resp.fetchone()[0])
            count_dialogs.set(count)
            return count

    def list_dialogs(self) -> list[Dialog]:
        dialogs = []
        with closing(self.conn.cursor()) as cursor:
            resp = cursor.execute(
                "SELECT archive_datetime, archive_tl_scheme_layer, id, type, str_repr, dict_repr, chat_type, name, pinned, archived_chat, last_msg_date, first_seen, last_seen "
                " FROM dialogs ORDER BY pinned DESC, archived_chat, last_msg_date DESC",
            )
            for row in resp.fetchall():
                dialog = Dialog(
                    archive_datetime=datetime.datetime.fromisoformat(row["archive_datetime"]),
                    archive_tl_schema_layer=row["archive_tl_scheme_layer"],
                    resource_id=row["id"],
                    resource_type=row["type"],
                    str_repr=row["str_repr"],
                    dict_repr=json.loads(row["dict_repr"]),
                )
                dialog.chat_type = DialogType.from_str(row["chat_type"])
                dialog.name = row["name"]
                dialog.pinned = row["pinned"] == 1
                dialog.archived_chat = row["archived_chat"] == 1
                dialog.last_msg_date = parsable_date(row["last_msg_date"])
                dialog.first_seen = parsable_date(row["first_seen"])
                dialog.last_seen = parsable_date(row["last_seen"])
                dialogs.append(dialog)
        count_dialogs.set(len(dialogs))
        return dialogs
