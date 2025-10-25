import asyncio
import datetime
import pathlib
from typing import Optional

import aiohttp_jinja2
import jinja2
from aiohttp import web
from prometheus_client import Gauge

from tg_backup.archiver import Archiver
from tg_backup.chat_settings_store import NewChatsFilter
from tg_backup.config import BehaviourConfig
from tg_backup.database.core_database import CoreDatabase
from tg_backup.models.abstract_resource import group_by_id
from tg_backup.utils.chat_matcher import matcher_parser

JINJA_TEMPLATE_DIR = pathlib.Path(__file__).parent / 'web_templates'

webserver_running = Gauge(
    "tgbackup_webserver_running",
    "Whether the TG backup webserver is currently running",
)

class WebServer:
    def __init__(self, archiver: Archiver) -> None:
        self.archiver = archiver
        self.core_db = CoreDatabase()
        self.counter = 0
        self.app = web.Application()
        self.jinja_env = aiohttp_jinja2.setup(self.app, loader=jinja2.FileSystemLoader(JINJA_TEMPLATE_DIR))

    async def home_page(self, req: web.Request) -> web.Response:
        return aiohttp_jinja2.render_template("home.html.jinja2", req, {})

    async def archive_info(self, req: web.Request) -> web.Response:
        chats_by_id = group_by_id(self.core_db.list_chats())
        users_by_id = group_by_id(self.core_db.list_users())
        sticker_sets_by_id = group_by_id(self.core_db.list_sticker_sets())
        archive_records = self.core_db.list_archive_runs()
        return aiohttp_jinja2.render_template(
            "archive_info.html.jinja2",
            req,
            {
                "running": self.archiver.running,
                "archive_records": archive_records,
                "chats_by_id": chats_by_id,
                "users_by_id": users_by_id,
                "sticker_sets_by_id": sticker_sets_by_id,
            }
        )

    async def archiver_status(self, req: web.Request) -> web.Response:
        return aiohttp_jinja2.render_template(
            "archiver_state.html.jinja2",
            req,
            {
                "running": self.archiver.running,
                "current_activity": self.archiver.current_activity,
                "archiver": self.archiver,
            }
        )

    async def archiver_status_post(self, req: web.Request) -> web.Response:
        data = await req.post()
        if data.get("action") == "run_archiver":
            dialogs = self.core_db.list_dialogs()
            asyncio.create_task(self.archiver.run_archive(dialogs))
            while not self.archiver.running:
                await asyncio.sleep(0.1)
            return await self.archiver_status(req)
        if data.get("action") == "stop_watcher":
            activity = self.archiver.current_activity
            if activity is None:
                return web.Response(status=400, text="Archiver is not currently active")
            watcher = activity.watcher
            if watcher is None:
                return web.Response(status=400, text="Archiver is not currently watching any targets")
            watcher.shutdown()
            while watcher.running:
                await asyncio.sleep(0.1)
            return await self.archiver_status(req)
        return web.Response(status=404, text="Unrecognised action")


    async def settings_behaviour(self, req: web.Request) -> web.Response:
        return aiohttp_jinja2.render_template(
            "settings_behaviour.html.jinja2",
            req,
            {
                "settings": self.archiver.chat_settings
            }
        )

    async def settings_behaviour_save(self, req: web.Request) -> web.Response:
        data = await req.post()
        settings = self.archiver.chat_settings
        settings.default_archive = data.get("default_archive") == "on"
        behaviour = BehaviourConfig(
            download_media=data.get("download_media") == "on",
            check_admin_log=data.get("check_admin_log") == "on",
            follow_live=data.get("follow_live") == "on",
            archive_history=data.get("archive_history") == "on",
            cleanup_duplicates=data.get("cleanup_duplicates") == "on",
            msg_history_overlap_days=int(data.get("msg_history_overlap_days")),
        )
        settings.default_behaviour = behaviour
        self.archiver.chat_settings = settings
        self.archiver.chat_settings.save_to_file()
        return await self.settings_behaviour(req)

    async def settings_known_dialogs(self, req: web.Request) -> web.Response:
        dialogs = self.core_db.list_dialogs()
        newest_dialog_date: Optional[datetime.datetime] = None
        for d in dialogs:
            if newest_dialog_date is None or d.last_seen > newest_dialog_date:
                newest_dialog_date = d.last_seen
        return aiohttp_jinja2.render_template(
            "settings_known_dialogs.html.jinja2",
            req,
            {
                "settings": self.archiver.chat_settings,
                "dialogs": self.core_db.list_dialogs(),
                "running_list_dialogs": self.archiver.running_list_dialogs,
                "newest_dialog_date": newest_dialog_date,
            }
        )

    async def settings_known_dialogs_post(self, req: web.Request) -> web.Response:
        data = await req.post()
        if data.get("action") == "update_known_dialogs":
            dialogs = self.core_db.list_dialogs()
            for data_key, data_val in data.items():
                if data_key.startswith("archive_dialog_"):
                    dialog_id = int(data_key[len("archive_dialog_"):])
                    dialog = [d for d in dialogs if d.resource_id == dialog_id][0]
                    parsed_val = {
                        "default": None,
                        "archive": True,
                        "no_archive": False,
                    }[data_val]
                    self.archiver.chat_settings.set_chat_archive(dialog_id, dialog, parsed_val)
            self.archiver.chat_settings.save_to_file()
            return await self.settings_known_dialogs(req)
        if data.get("action") == "list_dialogs":
            if self.archiver.running_list_dialogs:
                return web.Response(status=403, text="List dialogs request is already running")
            asyncio.create_task(self.archiver.save_dialogs())
            return await self.settings_known_dialogs(req)
        return web.Response(status=404, text="Unrecognised action")

    async def settings_new_dialogs(self, req: web.Request) -> web.Response:
        return aiohttp_jinja2.render_template(
            "settings_new_dialogs.html.jinja2",
            req,
            {
                "settings": self.archiver.chat_settings,
            }
        )

    async def settings_new_dialogs_post(self, req: web.Request) -> web.Response:
        data = await req.post()
        if data.get("action") == "update_new_dialogs":
            for data_key, data_val in data.items():
                if data_key.startswith("archive_filter_"):
                    filter_id = int(data_key[len("archive_filter_"):])
                    chat_filter = self.archiver.chat_settings.new_chat_filters[filter_id]
                    parsed_val = {
                        "default": None,
                        "archive": True,
                        "no_archive": False,
                    }[data_val]
                    chat_filter.archive = parsed_val
            new_filter_str = data.get("new_filter_filter").strip()
            if new_filter_str != "":
                filter_parser = matcher_parser()
                try:
                    new_filter_filter = filter_parser.parse_string(new_filter_str)
                except Exception as e:
                    return web.Response(status=400, text=f"Could not parse new chat filter: {str(e)}")
                parsed_archive_val = {
                    "default": None,
                    "archive": True,
                    "no_archive": False,
                }[data.get("new_filter_archive")]
                new_filter = NewChatsFilter(new_filter_str, parsed_archive_val, None)
                self.archiver.chat_settings.new_chat_filters.append(new_filter)
            self.archiver.chat_settings.save_to_file()
            return await self.settings_new_dialogs(req)
        if data.get("action").startswith("delete_filter_"):
            filter_id = int(data.get("action")[len("delete_filter_"):])
            del self.archiver.chat_settings.new_chat_filters[filter_id]
            self.archiver.chat_settings.save_to_file()
            return await self.settings_new_dialogs(req)
        if data.get("action").startswith("move_up_"):
            filter_id = int(data.get("action")[len("move_up_"):])
            if filter_id == 0:
                return web.Response(status=400, text="Can't move top up, silly")
            filters = self.archiver.chat_settings.new_chat_filters
            filters[filter_id-1], filters[filter_id] = filters[filter_id], filters[filter_id-1]
            self.archiver.chat_settings.new_chat_filters = filters
            self.archiver.chat_settings.save_to_file()
            return await self.settings_new_dialogs(req)
        if data.get("action").startswith("move_down_"):
            filter_id = int(data.get("action")[len("move_down_"):])
            filters = self.archiver.chat_settings.new_chat_filters
            if filter_id == len(filters)-1:
                return web.Response(status=400, text="Can't move bottom down, silly")
            filters[filter_id], filters[filter_id+1] = filters[filter_id+1], filters[filter_id]
            self.archiver.chat_settings.new_chat_filters = filters
            self.archiver.chat_settings.save_to_file()
            return await self.settings_new_dialogs(req)
        return web.Response(status=404, text="Unrecognised action")

    def _setup_routes(self) -> None:
        self.app.add_routes([
            web.static("/static", str(JINJA_TEMPLATE_DIR / "static")),
            web.get("/", self.home_page),
            web.get("/archive/", self.archive_info),
            web.get("/archiver/", self.archiver_status),
            web.post("/archiver/", self.archiver_status_post),
            web.get("/settings/behaviour", self.settings_behaviour),
            web.post("/settings/behaviour", self.settings_behaviour_save),
            web.get("/settings/known_dialogs", self.settings_known_dialogs),
            web.post("/settings/known_dialogs", self.settings_known_dialogs_post),
            web.get("/settings/new_dialogs", self.settings_new_dialogs),
            web.post("/settings/new_dialogs", self.settings_new_dialogs_post),
        ])

    def run(self) -> None:
        try:
            webserver_running.set(1)
            self.core_db.start()
            self._setup_routes()
            web.run_app(self.app, host='127.0.0.1', port=2000)
        finally:
            self.core_db.stop()
            webserver_running.set(0)
