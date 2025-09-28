import pathlib

import aiohttp_jinja2
import jinja2
from aiohttp import web

from tg_backup.archiver import Archiver

JINJA_TEMPLATE_DIR = pathlib.Path(__file__).parent / 'web_templates'

class WebServer:
    def __init__(self, archiver: Archiver) -> None:
        self.archiver = archiver
        self.counter = 0
        self.app = web.Application()
        self.jinja_env = aiohttp_jinja2.setup(self.app, loader=jinja2.FileSystemLoader(JINJA_TEMPLATE_DIR))

    def setup_routes(self) -> None:
        self.app.add_routes([web.get('/', self.home_page)])

    async def home_page(self, req):
        return aiohttp_jinja2.render_template("home.html.jinja2", req, {})

    def run(self) -> None:
        web.run_app(self.app, host='127.0.0.1', port=2000)
