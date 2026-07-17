"""
Screen for running a controller function (upload / download / …) inside
the TUI. The callable runs in a Textual worker thread; stdout, stderr, and
the ``CLI`` logger are all piped into a :class:`RichLog`. When the worker
finishes the footer flips to ``Done – press any key``.
"""
from __future__ import annotations

import contextlib
import logging
import sys
import threading
from typing import Any, Callable, Optional

from textual import on
from textual.app import ComposeResult
from textual.binding import Binding
from textual.containers import Vertical
from textual.screen import Screen
from textual.widgets import Footer, Header, Label, RichLog

from cirro.cli.tui.bridge import TUIBridge, set_current_bridge, reset_current_bridge


class LogScreen(Screen):
    """Run a target callable in a worker; stream its output into a log."""

    DEFAULT_CSS = """
    LogScreen { align: center middle; }
    #log-box { width: 100%; height: 100%; padding: 1 2; }
    #log-title { text-style: bold; color: $accent; padding-bottom: 1; }
    #log-status { color: $text-muted; padding-top: 1; }
    RichLog { height: 1fr; border: round $panel; }
    """

    BINDINGS = [
        Binding("escape", "close", "Close"),
        Binding("q", "close", "Close"),
    ]

    def __init__(self, title: str, target: Callable[..., Any], *args, **kwargs):
        super().__init__()
        self._title = title
        self._target = target
        self._args = args
        self._kwargs = kwargs
        self._finished = threading.Event()
        self._result: Any = None
        self._error: Optional[BaseException] = None

    def compose(self) -> ComposeResult:
        yield Header(show_clock=False)
        with Vertical(id="log-box"):
            yield Label(self._title, id="log-title")
            yield RichLog(id="log-view", markup=False, highlight=False, wrap=False)
            yield Label("Running…", id="log-status")
        yield Footer()

    def on_mount(self) -> None:
        self.query_one("#log-view", RichLog).focus()
        self.run_worker(self._run, thread=True, exclusive=False, name=self._title)

    # --- Worker ------------------------------------------------------------

    def _run(self) -> None:
        app = self.app
        log = self.query_one("#log-view", RichLog)
        status = self.query_one("#log-status", Label)

        def write_line(line: str) -> None:
            log.write(line)

        stream = _AppLogStream(app, write_line)
        handler = _RichLogHandler(app, write_line)
        handler.setFormatter(logging.Formatter("%(levelname)s %(message)s"))
        cli_logger = logging.getLogger("CLI")
        cli_logger.addHandler(handler)

        bridge = TUIBridge(request_prompt=lambda spec, br: _push_prompt_from_worker(app, spec, br))
        token = set_current_bridge(bridge)
        try:
            with contextlib.redirect_stdout(stream), contextlib.redirect_stderr(stream):
                try:
                    self._result = self._target(*self._args, **self._kwargs)
                except KeyboardInterrupt:
                    self._error = KeyboardInterrupt()
                    app.call_from_thread(write_line, "Cancelled by user.")
                except BaseException as e:  # noqa: BLE001
                    self._error = e
                    app.call_from_thread(write_line, f"ERROR: {type(e).__name__}: {e}")
        finally:
            reset_current_bridge(token)
            cli_logger.removeHandler(handler)
            stream.flush()
            self._finished.set()
            done_msg = "Done — press Esc or q to close" if self._error is None else "Failed — press Esc or q to close"
            app.call_from_thread(status.update, done_msg)

    def action_close(self) -> None:
        # Only allow close after worker signals done.
        if self._finished.is_set():
            self.app.pop_screen()


# --- Helpers ---------------------------------------------------------------


def _push_prompt_from_worker(app, spec: dict, bridge: TUIBridge) -> None:
    """Called from worker via ``TUIBridge``; hop back to UI thread to push
    the PromptScreen, then send the answer back through the bridge."""

    def on_dismiss(value: Any) -> None:
        if value is None:
            bridge.cancel()
        else:
            bridge.resolve(value)

    def push() -> None:
        # Local import to avoid a package-level cycle at import time.
        from cirro.cli.tui.prompt_screen import PromptScreen

        app.push_screen(PromptScreen(spec), on_dismiss)

    app.call_from_thread(push)


class _AppLogStream:
    """File-like sink that pushes each line into a RichLog from any thread."""

    def __init__(self, app, write_line: Callable[[str], None]):
        self._app = app
        self._write_line = write_line
        self._buf = ""

    def write(self, s: str) -> int:
        if not s:
            return 0
        # tqdm uses \r to overwrite; treat as line break so progress at least scrolls.
        text = s.replace("\r", "\n")
        self._buf += text
        while "\n" in self._buf:
            line, _, self._buf = self._buf.partition("\n")
            if line:
                self._app.call_from_thread(self._write_line, line)
        return len(s)

    def flush(self) -> None:
        if self._buf:
            self._app.call_from_thread(self._write_line, self._buf)
            self._buf = ""

    def isatty(self) -> bool:  # tqdm checks this
        return False


class _RichLogHandler(logging.Handler):
    def __init__(self, app, write_line: Callable[[str], None]):
        super().__init__()
        self._app = app
        self._write_line = write_line

    def emit(self, record: logging.LogRecord) -> None:
        try:
            msg = self.format(record)
        except Exception:  # pragma: no cover - defensive
            return
        self._app.call_from_thread(self._write_line, msg)
