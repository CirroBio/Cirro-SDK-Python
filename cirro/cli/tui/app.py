"""
Cirro Textual App.

The whole entry point: one App, one HomeScreen (menu). Every menu row is
a :class:`MenuItem` from ``actions.py``; the row's ``run`` callable is
invoked with the app instance and is responsible for pushing whatever
screen the operation needs (usually a :class:`LogScreen`).
"""
from __future__ import annotations

from textual import on
from textual.app import App, ComposeResult
from textual.binding import Binding
from textual.containers import Vertical
from textual.screen import Screen
from textual.widgets import Footer, Header, Label, OptionList
from textual.widgets.option_list import Option

from cirro.cli.tui.actions import MENU


class HomeScreen(Screen):
    """Top-level menu."""

    DEFAULT_CSS = """
    HomeScreen { align: center middle; }
    #home-box {
        width: 60;
        max-width: 90%;
        height: auto;
        padding: 1 2;
        border: round $accent;
        background: $surface;
    }
    #home-title {
        text-style: bold;
        color: $accent;
        padding-bottom: 1;
        content-align: center middle;
    }
    #home-help {
        color: $text-muted;
        padding-top: 1;
        content-align: center middle;
    }
    OptionList { height: auto; }
    """

    BINDINGS = [
        Binding("q", "app.quit", "Quit"),
        Binding("ctrl+c", "app.quit", "Quit", show=False),
    ]

    def compose(self) -> ComposeResult:
        yield Header(show_clock=False)
        with Vertical(id="home-box"):
            yield Label("Cirro", id="home-title")
            options = [Option(item.label, id=str(i)) for i, item in enumerate(MENU)]
            yield OptionList(*options, id="home-menu")
            yield Label("↑/↓ move   Enter=select   q=quit", id="home-help")
        yield Footer()

    def on_mount(self) -> None:
        self.query_one("#home-menu", OptionList).focus()

    @on(OptionList.OptionSelected, "#home-menu")
    def _on_selected(self, event: OptionList.OptionSelected) -> None:
        idx = event.option_index
        if idx is None or idx < 0 or idx >= len(MENU):
            return
        MENU[idx].run(self.app)


class CirroTUI(App):
    """The Cirro TUI application."""

    TITLE = "Cirro TUI"
    SUB_TITLE = "Data transfer & operations"

    def on_mount(self) -> None:
        self.push_screen(HomeScreen())


def launch() -> None:
    CirroTUI().run()
