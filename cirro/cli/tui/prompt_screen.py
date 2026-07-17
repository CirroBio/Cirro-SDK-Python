"""
One screen renders any questionary-style prompt spec.

The spec vocabulary is the same one used by ``questionary.prompt`` and by
``cirro.cli.interactive.utils.ask``:

    {"type": "select" | "confirm" | "checkbox" | "text" | "input"
             | "autocomplete" | "path",
     "message": str,
     "choices": list[str],           # select / checkbox / autocomplete
     "default": Any,                 # optional
     "validate": Callable | Validator, # optional
     "meta_information": dict[str, str], # optional; select / autocomplete
     "only_directories": bool}       # path

The class dispatch below is the whole meta-design: to add a new prompt type
you register one small renderer.
"""
from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
from typing import Any, Iterable, List, Optional

from rich.text import Text
from textual import on
from textual.app import ComposeResult
from textual.binding import Binding
from textual.containers import Vertical
from textual.screen import ModalScreen
from textual.widget import Widget
from textual.widgets import (
    DirectoryTree,
    Input,
    Label,
    OptionList,
    SelectionList,
)
from textual.widgets.option_list import Option


WIDGET_ID = "prompt-widget"
ERROR_ID = "prompt-error"
TREE_ID = "prompt-tree"
AUX_ID = "prompt-aux"


# --- Renderers -------------------------------------------------------------


class _Renderer:
    """One renderer per prompt type."""

    def compose(self, spec: dict) -> Iterable[Widget]:
        raise NotImplementedError

    def read_value(self, screen: "PromptScreen") -> Any:
        raise NotImplementedError


def _label_with_meta(choice: str, meta: Optional[dict]) -> Any:
    if not meta:
        return choice
    extra = meta.get(choice)
    if not extra:
        return choice
    text = Text(choice)
    text.append(f"  {extra}", style="dim")
    return text


class _SelectRenderer(_Renderer):
    def compose(self, spec):
        choices = spec.get("choices", [])
        meta = spec.get("meta_information")
        options = [Option(_label_with_meta(c, meta), id=str(i)) for i, c in enumerate(choices)]
        widget = OptionList(*options, id=WIDGET_ID)
        default = spec.get("default")
        if default is not None and default in choices:
            widget.highlighted = choices.index(default)
        yield widget

    def read_value(self, screen):
        widget = screen.query_one(f"#{WIDGET_ID}", OptionList)
        idx = widget.highlighted
        if idx is None:
            return None
        return screen.spec["choices"][idx]


class _ConfirmRenderer(_Renderer):
    def compose(self, spec):
        default = spec.get("default", True)
        widget = OptionList(Option("Yes", id="0"), Option("No", id="1"), id=WIDGET_ID)
        widget.highlighted = 0 if default else 1
        yield widget

    def read_value(self, screen):
        widget = screen.query_one(f"#{WIDGET_ID}", OptionList)
        return widget.highlighted == 0


class _CheckboxRenderer(_Renderer):
    def compose(self, spec):
        choices = spec.get("choices", [])
        # SelectionList items: (label, value, initial_state)
        items = [(str(c), i, False) for i, c in enumerate(choices)]
        yield SelectionList[int](*items, id=WIDGET_ID)
        yield Label("Space=toggle  a=all  n=none  Enter=submit", id=AUX_ID)

    def read_value(self, screen):
        widget = screen.query_one(f"#{WIDGET_ID}", SelectionList)
        indices = list(widget.selected)
        choices = screen.spec.get("choices", [])
        return [choices[i] for i in indices]


class _TextRenderer(_Renderer):
    def compose(self, spec):
        default = spec.get("default")
        yield Input(
            value="" if default is None else str(default),
            placeholder=spec.get("placeholder", ""),
            id=WIDGET_ID,
        )

    def read_value(self, screen):
        return screen.query_one(f"#{WIDGET_ID}", Input).value


class _PathRenderer(_TextRenderer):
    def compose(self, spec):
        default = spec.get("default") or str(Path.cwd())
        default_path = Path(str(default)).expanduser()
        start_dir = default_path if default_path.is_dir() else Path.cwd()
        yield Input(value=str(default_path), id=WIDGET_ID)
        yield DirectoryTree(str(start_dir), id=TREE_ID)
        yield Label("Type a path or pick from tree (Enter on tree fills field)", id=AUX_ID)


class _AutocompleteRenderer(_Renderer):
    def compose(self, spec):
        choices = spec.get("choices", []) or []
        default = spec.get("default") or ""
        meta = spec.get("meta_information")
        yield Input(value=str(default), id=WIDGET_ID, placeholder="Type to filter…")
        options = [Option(_label_with_meta(c, meta), id=str(i)) for i, c in enumerate(choices)]
        yield OptionList(*options, id=AUX_ID)

    def read_value(self, screen):
        # Prefer the highlighted match if the input value matches; else the raw input.
        text = screen.query_one(f"#{WIDGET_ID}", Input).value
        choices = screen.spec.get("choices", []) or []
        # Exact hit
        if text in choices:
            return text
        # Case-insensitive hit
        for c in choices:
            if c.lower() == text.lower():
                return c
        # Fall back to whatever the option list currently highlights, if any
        aux = screen.query_one(f"#{AUX_ID}", OptionList)
        if aux.highlighted is not None:
            visible = getattr(screen, "_visible_choices", choices)
            if 0 <= aux.highlighted < len(visible):
                return visible[aux.highlighted]
        return text


RENDERERS: dict[str, _Renderer] = {
    "select": _SelectRenderer(),
    "confirm": _ConfirmRenderer(),
    "checkbox": _CheckboxRenderer(),
    "text": _TextRenderer(),
    "input": _TextRenderer(),
    "path": _PathRenderer(),
    "autocomplete": _AutocompleteRenderer(),
}


# --- Screen ----------------------------------------------------------------


class PromptScreen(ModalScreen[Any]):
    """Render one questionary-style spec and dismiss with the answer."""

    DEFAULT_CSS = """
    PromptScreen {
        align: center middle;
    }
    #prompt-box {
        width: 80%;
        max-width: 100;
        height: auto;
        max-height: 80%;
        padding: 1 2;
        border: round $accent;
        background: $surface;
    }
    #prompt-message {
        color: $text;
        text-style: bold;
        padding-bottom: 1;
    }
    #prompt-error {
        color: $error;
        padding-top: 1;
    }
    #prompt-help {
        color: $text-muted;
        padding-top: 1;
    }
    OptionList, SelectionList {
        height: auto;
        max-height: 20;
        margin-top: 1;
    }
    DirectoryTree {
        height: 15;
        margin-top: 1;
    }
    """

    BINDINGS = [
        Binding("escape", "cancel", "Cancel", show=True),
        Binding("ctrl+c", "cancel", "Cancel", show=False),
        Binding("ctrl+j", "submit", "Submit", show=False),  # Enter fallback
    ]

    def __init__(self, spec: dict):
        super().__init__()
        self.spec = dict(spec)
        self._renderer = RENDERERS.get(spec.get("type", "text"), RENDERERS["text"])

    def compose(self) -> ComposeResult:
        with Vertical(id="prompt-box"):
            yield Label(str(self.spec.get("message") or self.spec.get("name") or ""), id="prompt-message")
            yield from self._renderer.compose(self.spec)
            yield Label("", id=ERROR_ID)
            yield Label(self._helptext(), id="prompt-help")

    def _helptext(self) -> str:
        typ = self.spec.get("type", "text")
        if typ == "checkbox":
            return "Space=toggle   Enter=submit   Esc=cancel"
        if typ in ("select", "confirm"):
            return "↑/↓ move   Enter=select   Esc=cancel"
        if typ == "autocomplete":
            return "Type to filter   ↑/↓ move   Enter=submit   Esc=cancel"
        if typ == "path":
            return "Enter=submit   Esc=cancel   (Tree is browse-only)"
        return "Enter=submit   Esc=cancel"

    # --- Focus ------------------------------------------------------------

    def on_mount(self) -> None:
        try:
            self.query_one(f"#{WIDGET_ID}").focus()
        except Exception:
            pass

    # --- Event handlers ---------------------------------------------------

    @on(Input.Submitted, f"#{WIDGET_ID}")
    def _on_input_submitted(self, _event: Input.Submitted) -> None:
        self.action_submit()

    @on(OptionList.OptionSelected, f"#{WIDGET_ID}")
    def _on_option_selected(self, _event: OptionList.OptionSelected) -> None:
        # 'select' and 'confirm' finish on Enter over an option.
        if self.spec.get("type") in ("select", "confirm"):
            self.action_submit()

    @on(DirectoryTree.DirectorySelected, f"#{TREE_ID}")
    def _on_dir_selected(self, event: DirectoryTree.DirectorySelected) -> None:
        try:
            self.query_one(f"#{WIDGET_ID}", Input).value = str(event.path)
        except Exception:
            pass

    @on(Input.Changed, f"#{WIDGET_ID}")
    def _on_input_changed(self, event: Input.Changed) -> None:
        if self.spec.get("type") != "autocomplete":
            return
        needle = event.value.lower().strip()
        all_choices: List[str] = self.spec.get("choices") or []
        meta = self.spec.get("meta_information")
        visible = [c for c in all_choices if needle in c.lower()] if needle else list(all_choices)
        self._visible_choices = visible
        aux = self.query_one(f"#{AUX_ID}", OptionList)
        aux.clear_options()
        for i, c in enumerate(visible):
            aux.add_option(Option(_label_with_meta(c, meta), id=str(i)))

    # --- Actions ----------------------------------------------------------

    def action_cancel(self) -> None:
        self.dismiss(None)

    def action_submit(self) -> None:
        try:
            value = self._renderer.read_value(self)
        except Exception as e:  # pragma: no cover - defensive
            self._set_error(f"{type(e).__name__}: {e}")
            return

        # required (converted upstream from questionary "required=True" into a validator)
        if self.spec.get("required") and _is_empty(value):
            self._set_error("This field is required")
            return

        error = _run_validate(self.spec.get("validate"), value)
        if error:
            self._set_error(error)
            return

        # 'path' extra: honor only_directories
        if self.spec.get("type") == "path" and self.spec.get("only_directories"):
            p = Path(str(value)).expanduser()
            if not p.is_dir():
                self._set_error(f"Not a directory: {p}")
                return
            value = str(p)

        self.dismiss(value)

    def _set_error(self, msg: str) -> None:
        try:
            self.query_one(f"#{ERROR_ID}", Label).update(msg)
        except Exception:
            pass


# --- Helpers ---------------------------------------------------------------


def _is_empty(value: Any) -> bool:
    if value is None:
        return True
    if isinstance(value, str):
        return len(value.strip()) == 0
    if isinstance(value, (list, tuple, dict, set)):
        return len(value) == 0
    return False


def _run_validate(validate: Any, value: Any) -> Optional[str]:
    """Return an error message string, or None if the value is OK.

    Handles both questionary-style callable validators (returning True or
    error string) and prompt_toolkit ``Validator`` classes/instances.
    """
    if validate is None:
        return None
    # Callable form (either free function or a class we can instantiate)
    if callable(validate):
        try:
            result = validate(value) if not isinstance(validate, type) else _try_validator_class(validate, value)
        except Exception as e:
            return f"{type(e).__name__}: {e}"
        if result is True or result is None:
            return None
        if isinstance(result, str):
            return result
        return None
    return None


def _try_validator_class(cls, value: str) -> bool:
    """Support prompt_toolkit Validator subclasses (e.g. DirectoryValidator)."""
    instance = cls()
    validate = getattr(instance, "validate", None)
    if validate is None:
        return True
    doc = SimpleNamespace(text=str(value))
    validate(doc)  # raises ValidationError on failure
    return True
