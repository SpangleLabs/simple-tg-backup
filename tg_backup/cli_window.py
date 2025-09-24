import contextlib
import curses
from typing import Iterator


class CLIWindowException(Exception):
    pass


class CLIWindow:
    def __init__(self, window: curses.window) -> None:
        self.window = window
        self.cols = 0
        self.rows = 0

    @contextlib.contextmanager
    def prepare_for_render(self) -> Iterator[None]:
        try:
            self.window.clear()
            self.rows, self.cols = self.window.getmaxyx()
            yield
            self.window.refresh()
        except CLIWindowException as e:
            self.window.clear()
            self.window.addstr(f"Rendering hit unexpected error:\n{e}")
            self.window.refresh()
        except curses.error:
            self.window.clear()
            self.window.addstr("Terminal may be too small to render menu :(")
            self.window.refresh()

    def write_title(self, text: str) -> None:
        if len(text) > self.cols:
            raise CLIWindowException("Title too wide to fit in display")
        format_str = f"{{:^{self.cols}}}"
        self.window.addstr(format_str.format(text), curses.A_REVERSE)

    def write_line(self, text: str) -> None:
        if len(text) > self.cols:
            raise CLIWindowException("Line too wide to fit in display")
        self.window.addstr(text+"\n")

    def write_final_line(self, text: str) -> None:
        if len(text) > self.cols:
            raise CLIWindowException("Final line too wide to fit in display")
        self.window.move(self.rows - 1, 0)
        self.window.addstr(text)

    def write_options(self, options: list[str], selected: int) -> None:
        if selected < 0 or selected >= len(options):
            raise CLIWindowException("Selected option out of range of options")
        if len(options) > self.rows:
            raise CLIWindowException("Too many options to fit on display")
        for n, option in enumerate(options):
            radio_box = "[*] " if n == selected else "[ ] "
            self.window.addstr(radio_box + option + "\n")
