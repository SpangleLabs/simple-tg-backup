from abc import ABC, abstractmethod
from typing import TYPE_CHECKING

from tg_backup.cli_window import CLIWindow

if TYPE_CHECKING:
    from tg_backup.cli import CLI


class AbstractMenu(ABC):

    @abstractmethod
    def render(self, window: CLIWindow) -> None:
        raise NotImplementedError()

    @abstractmethod
    def handle_keypress(self, cli: "CLI", key: str) -> None:
        raise NotImplementedError()
