"""
Sync-in-async bridge.

Every ``gather_*`` function in ``cirro/cli/interactive`` is synchronous and
blocks on ``questionary``. To reuse them inside a Textual App we run them in
a worker thread; each ``ask()``/``prompt_wrapper()`` call posts a spec to
the App on the UI thread and blocks on a queue until the App pushes the
answer back.

Nothing here knows about individual prompt types — the App decides what to
render from ``spec['type']``. That keeps this bridge tiny and stable.
"""
from __future__ import annotations

import contextvars
import queue
from dataclasses import dataclass
from typing import Any, Callable, Optional


_CURRENT: contextvars.ContextVar[Optional["TUIBridge"]] = contextvars.ContextVar(
    "cirro_tui_bridge", default=None
)


@dataclass
class _Cancelled:
    """Sentinel meaning the user hit Esc / closed the prompt."""


class TUIBridge:
    """Bridge between a background worker thread and the Textual App.

    The worker thread calls :meth:`ask` (synchronously). The bridge posts a
    request onto the app's UI thread via ``request_prompt``, which is
    expected to eventually call :meth:`resolve` with the user's answer (or
    :meth:`cancel` if the user aborted).
    """

    def __init__(self, request_prompt: Callable[[dict, "TUIBridge"], None]):
        self._request_prompt = request_prompt
        self._answers: "queue.Queue[Any]" = queue.Queue()

    def ask(self, spec: dict) -> Any:
        """Called from a worker thread. Blocks until the app answers."""
        self._request_prompt(spec, self)
        result = self._answers.get()
        if isinstance(result, _Cancelled):
            raise KeyboardInterrupt()
        return result

    def resolve(self, value: Any) -> None:
        self._answers.put(value)

    def cancel(self) -> None:
        self._answers.put(_Cancelled())


def set_current_bridge(bridge: Optional[TUIBridge]) -> contextvars.Token:
    return _CURRENT.set(bridge)


def reset_current_bridge(token: contextvars.Token) -> None:
    _CURRENT.reset(token)


def get_current_bridge() -> Optional[TUIBridge]:
    return _CURRENT.get()
