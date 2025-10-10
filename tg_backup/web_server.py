import asyncio
import datetime
import pathlib
from typing import Optional

import aiohttp_jinja2
import jinja2
from aiohttp import web

from tg_backup.archiver import Archiver
from tg_backup.config import BehaviourConfig
from tg_backup.database.core_database import CoreDatabase
from tg_backup.models.abstract_resource import group_by_id

JINJA_TEMPLATE_DIR = pathlib.Path(__file__).parent / 'web_templates'

class WebServer:
    def __init__(self, archiver: Archiver) -> None:
        self.archiver = archiver
        self.core_db = CoreDatabase()
        self.counter = 0
        self.app = web.Application()
        self.jinja_env = aiohttp_jinja2.setup(self.app, loader=jinja2.FileSystemLoader(JINJA_TEMPLATE_DIR))

    async def home_page(self, req: web.Request) -> web.Response:
        return aiohttp_jinja2.render_template("home.html.jinja2", req, {})

    async def archiver_state(self, req: web.Request) -> web.Response:
        chats_by_id = group_by_id(self.core_db.list_chats())
        users_by_id = group_by_id(self.core_db.list_users())
        sticker_sets_by_id = group_by_id(self.core_db.list_sticker_sets())
        archive_records = self.core_db.list_archive_runs()
        return aiohttp_jinja2.render_template(
            "archive_state.html.jinja2",
            req,
            {
                "running": self.archiver.running,
                "archive_records": archive_records,
                "chats_by_id": chats_by_id,
                "users_by_id": users_by_id,
                "sticker_sets_by_id": sticker_sets_by_id,
            }
        )

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

    async def settings_known_dialog_behaviour(self, req: web.Request) -> web.Response:
        dialog_id = req.match_info["dialog_id"]
        # TODO: a form to configure behaviour settings for a chat

    def _setup_routes(self) -> None:
        self.app.add_routes([
            web.static("/static", str(JINJA_TEMPLATE_DIR / "static")),
            web.get("/", self.home_page),
            web.get("/archive/", self.archiver_state),
            web.get("/settings/behaviour", self.settings_behaviour),
            web.post("/settings/behaviour", self.settings_behaviour_save),
            web.get("/settings/known_dialogs", self.settings_known_dialogs),
            web.post("/settings/known_dialogs", self.settings_known_dialogs_post),
        ])

    def run(self) -> None:
        try:
            self.core_db.start()
            self._setup_routes()
            web.run_app(self.app, host='127.0.0.1', port=2000)
        finally:
            self.core_db.stop()
