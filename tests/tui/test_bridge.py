"""End-to-end sanity check for the sync-in-async bridge.

Confirms that a background thread calling ``ask()`` while a bridge is
bound reaches the App, receives an answer, and returns it.
"""
from __future__ import annotations

import asyncio
import threading
from typing import Any

from textual.app import App

from cirro.cli.interactive.utils import ask
from cirro.cli.tui.bridge import TUIBridge, reset_current_bridge, set_current_bridge
from cirro.cli.tui.prompt_screen import PromptScreen


class _BridgeHarness(App):
    def __init__(self):
        super().__init__()
        self.worker_result: Any = "<unset>"
        self._done = threading.Event()

    def _push_prompt(self, spec: dict, bridge: TUIBridge) -> None:
        def on_dismiss(value: Any) -> None:
            if value is None:
                bridge.cancel()
            else:
                bridge.resolve(value)

        def push() -> None:
            self.push_screen(PromptScreen(spec), on_dismiss)

        self.call_from_thread(push)

    def on_mount(self) -> None:
        bridge = TUIBridge(request_prompt=self._push_prompt)

        def worker() -> None:
            token = set_current_bridge(bridge)
            try:
                self.worker_result = ask("select", "Pick", choices=["x", "y", "z"])
            finally:
                reset_current_bridge(token)
                self._done.set()

        threading.Thread(target=worker, daemon=True).start()


def test_ask_delegates_to_bridge_end_to_end():
    async def _go():
        app = _BridgeHarness()
        async with app.run_test() as pilot:
            for _ in range(50):
                await pilot.pause()
                if isinstance(app.screen, PromptScreen):
                    break
            assert isinstance(app.screen, PromptScreen), "PromptScreen was not pushed"
            await pilot.press("down", "enter")
            for _ in range(50):
                await pilot.pause()
                if app._done.is_set():
                    break
        return app.worker_result

    assert asyncio.run(_go()) == "y"
