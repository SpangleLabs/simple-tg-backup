from typing import Optional, TYPE_CHECKING

from tg_backup.cli_window import CLIWindow
from tg_backup.menus.astract_menu import AbstractMenu

if TYPE_CHECKING:
    from tg_backup.cli import CLI


class MainMenu(AbstractMenu):
    def __init__(self) -> None:
        super().__init__()
        self.last_key: Optional[str] = None
        self.typing = ""
        self.selected_option = 0
        self.menu_options = [
            "Specify a chat ID or username to download",
            "Modify the default archive behaviour settings",
            "Modify archive settings for known chats",
            "Modify archive settings for new chats",
        ]

    def render(self, window: CLIWindow) -> None:
        window.write_title("Telegram Archiver")
        window.write_line("Hello there, this is the simple telegram archiver main menu.")
        window.write_line("What would you like to do?")
        window.write_options(self.menu_options, self.selected_option)
        window.write_final_line("Press [q] to quit")

    def handle_keypress(self, key: str) -> None:
        self.last_key = key
        if len(key) == 1:
            self.typing += key
