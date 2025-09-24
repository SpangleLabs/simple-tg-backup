import curses
import logging
from typing import Optional

from tg_backup.archiver import Archiver
from tg_backup.config import BehaviourConfig
from tg_backup.menus.astract_menu import AbstractMenu
from tg_backup.menus.main_menu import MainMenu

logger = logging.getLogger(__name__)

class CLI:
    def __init__(self, archiver: Archiver, default_behaviour: BehaviourConfig) -> None:
        self.archiver = archiver
        self.default_behaviour = default_behaviour
        self.current_menu: Optional[AbstractMenu] = None
        self.running = False

    async def run(self) -> None:
        self.running = True
        try:
            curses.wrapper(self.render)
        except curses.error as e:
            logger.warning("Could not start curses display", exc_info=e)

    def render(self, window: curses.window) -> None:
        if self.current_menu is None:
            self.current_menu = MainMenu()
        while self.running:
            try:
                window.clear()
                self.current_menu.render(window)
                window.refresh()
            except curses.error:
                window.clear()
                window.addstr("Terminal may be too small to render menu :(")
                window.refresh()
            curses.halfdelay(10)
            try:
                char = window.getkey()
            except curses.error:
                pass
            else:
                self.current_menu.handle_keypress(char)



