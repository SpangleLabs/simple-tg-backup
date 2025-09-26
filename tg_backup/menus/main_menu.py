import dataclasses
from typing import Optional, TYPE_CHECKING, Callable

from tg_backup.cli_window import CLIWindow
from tg_backup.menus.astract_menu import AbstractMenu

if TYPE_CHECKING:
    from tg_backup.cli import CLI


@dataclasses.dataclass
class MenuOption:
    name: str
    function: Callable[["CLI"], None]


class MainMenu(AbstractMenu):
    def __init__(self) -> None:
        super().__init__()
        self.last_key: Optional[str] = None
        self.typing = ""
        self.selected_option = 0
        self.menu_options = [
            MenuOption(
                "Run the archiver with current settings",
                self.option_run_archiver,
            ),
            MenuOption(
                "Specify a chat ID or username to download",
                self.option_specify_chat_id,
            ),
            MenuOption(
                "Modify the default archive behaviour settings",
                self.option_modify_default_behaviour,
            ),
            MenuOption(
                "Modify archive settings for known chats",
                self.option_modify_known_chats,
            ),
            MenuOption(
                "Modify archive settings for new chats",
                self.option_modify_new_chat_filters,
            )
        ]

    def render(self, window: CLIWindow) -> None:
        window.write_title("Telegram Archiver")
        window.write_line("Hello there, this is the simple telegram archiver main menu.")
        window.write_line("What would you like to do?")
        window.write_options([m.name for m in self.menu_options], self.selected_option)
        window.write_final_line("Press [enter] to select an option, press [q] to quit")

    def handle_keypress(self, cli: "CLI", key: str) -> None:
        if key == "q":
            cli.running = False
        if key == "KEY_UP" and self.selected_option > 0:
            self.selected_option -= 1
        if key == "KEY_DOWN":
            self.selected_option = min(self.selected_option + 1, len(self.menu_options) - 1)
        if key == "\n":
            option = self.menu_options[self.selected_option]
            option.function(cli)

    def option_run_archiver(self, cli: "CLI") -> None:
        raise NotImplementedError()

    def option_specify_chat_id(self, cli: "CLI") -> None:
        raise NotImplementedError()

    def option_modify_default_behaviour(self, cli: "CLI") -> None:
        raise NotImplementedError()

    def option_modify_known_chats(self, cli: "CLI") -> None:
        raise NotImplementedError()

    def option_modify_new_chat_filters(self, cli: "CLI") -> None:
        raise NotImplementedError()