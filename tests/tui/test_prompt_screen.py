"""Pilot tests for the polymorphic PromptScreen.

These lock in the meta-design: one screen, one spec vocabulary, one
dispatch — any prompt type answered end-to-end.
"""
from __future__ import annotations

import asyncio
from typing import Any

from textual.app import App

from cirro.cli.tui.prompt_screen import PromptScreen


class _Harness(App):
    """Minimal App that pushes a PromptScreen and captures its result."""

    def __init__(self, spec: dict):
        super().__init__()
        self._spec = spec
        self.result: Any = "<unset>"

    def on_mount(self) -> None:
        def on_dismiss(value: Any) -> None:
            self.result = value

        self.push_screen(PromptScreen(self._spec), on_dismiss)


def _drive(spec: dict, keys: list[str]) -> Any:
    async def _go() -> Any:
        app = _Harness(spec)
        async with app.run_test() as pilot:
            for key in keys:
                await pilot.press(key)
            await pilot.pause()
        return app.result

    return asyncio.run(_go())


def test_select_returns_highlighted_choice():
    spec = {"type": "select", "message": "Pick one", "choices": ["red", "green", "blue"]}
    assert _drive(spec, ["down", "down", "enter"]) == "blue"


def test_confirm_yes_returns_true():
    spec = {"type": "confirm", "message": "Continue?"}
    assert _drive(spec, ["enter"]) is True


def test_confirm_no_returns_false():
    spec = {"type": "confirm", "message": "Continue?"}
    assert _drive(spec, ["down", "enter"]) is False


def test_text_returns_typed_value():
    spec = {"type": "text", "message": "Name", "default": "abc"}
    assert _drive(spec, ["enter"]) == "abc"


def test_text_validator_blocks_empty_and_shows_error():
    validate = lambda v: len(v.strip()) > 0 or "This field is required"  # noqa: E731
    spec = {"type": "text", "message": "Name", "default": "", "validate": validate}

    async def _go():
        app = _Harness(spec)
        async with app.run_test() as pilot:
            await pilot.press("enter")   # empty submit → blocked
            await pilot.pause()
            assert app.result == "<unset>"
            await pilot.press("a")
            await pilot.press("enter")
            await pilot.pause()
        return app.result

    assert asyncio.run(_go()) == "a"


def test_checkbox_returns_selected_labels():
    spec = {"type": "checkbox", "message": "Pick many", "choices": ["a", "b", "c"]}
    keys = ["space", "down", "down", "space", "ctrl+j"]
    assert _drive(spec, keys) == ["a", "c"]


def test_cancel_returns_none():
    spec = {"type": "select", "message": "Pick", "choices": ["a", "b"]}
    assert _drive(spec, ["escape"]) is None
