import curses
from abc import ABC, abstractmethod


class AbstractMenu(ABC):

    @abstractmethod
    def render(self, window: curses.window) -> None:
        raise NotImplementedError()

    @abstractmethod
    def handle_keypress(self, key: str) -> None:
        raise NotImplementedError()
