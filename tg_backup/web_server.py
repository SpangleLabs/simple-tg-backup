import pathlib

import aiohttp_jinja2
import jinja2
from aiohttp import web

from tg_backup.archiver import Archiver
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
        return aiohttp_jinja2.render_template(
            "archive_state.jinja2",
            req,
            {
                "running": self.archiver.running,
                "chats_by_id": chats_by_id,
                "users_by_id": users_by_id,
                "sticker_sets_by_id": sticker_sets_by_id,
            }
        )

    def _setup_routes(self) -> None:
        self.app.add_routes([
            web.get("/", self.home_page),
            web.get("/archive/", self.archiver_state),
        ])

    def run(self) -> None:
        try:
            self.core_db.start()
            self._setup_routes()
            web.run_app(self.app, host='127.0.0.1', port=2000)
        finally:
            self.core_db.stop()
