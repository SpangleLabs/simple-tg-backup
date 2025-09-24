import curses
from typing import Optional

from tg_backup.menus.astract_menu import AbstractMenu


class MainMenu(AbstractMenu):
    def __init__(self) -> None:
        super().__init__()
        self.last_key: Optional[str] = None
        self.typing = ""

    def render(self, window: curses.window) -> None:
        rows, cols = window.getmaxyx()
        window.addstr("Hello world, this is the simple telegram archiver main menu.\n")
        window.addstr(f"The window size is {rows} rows and {cols} columns.\n")
        if self.last_key is None:
            window.addstr("Try pressing a button I guess\n")
        else:
            window.addstr(f"Ah, you pressed {self.last_key}\n")
        if self.typing:
            window.addstr(f"In total you have typed: {self.typing}\n")

    def handle_keypress(self, key: str) -> None:
        self.last_key = key
        if len(key) == 1:
            self.typing += key
