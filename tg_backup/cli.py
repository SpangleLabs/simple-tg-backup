import curses
import logging
from typing import Optional

from tg_backup.archiver import Archiver
from tg_backup.cli_window import CLIWindow
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
        cli_window = CLIWindow(window)
        while self.running:
            with cli_window.prepare_for_render():
                self.current_menu.render(cli_window)
            curses.halfdelay(10)
            try:
                char = window.getkey()
            except curses.error:
                pass
            else:
                self.current_menu.handle_keypress(char)



