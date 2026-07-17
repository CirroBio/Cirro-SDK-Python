"""
Menu — the operations exposed by the TUI, defined as data.

Every workflow reuses the existing ``run_*`` controller functions and the
existing ``gather_*`` prompt orchestrators. The TUI adds no new prompt
logic; it just runs those controllers with ``interactive=True`` inside a
:class:`LogScreen`, and the bridge takes care of routing each prompt into
a :class:`PromptScreen`.

Read-only browse actions load a small list, then push a
:class:`RecordsScreen` with an ``on_select`` callback that drills to the
next level (project → datasets → files).
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, List

from textual.app import ComposeResult
from textual.binding import Binding
from textual.screen import Screen
from textual.widgets import Footer, Header, Label

from cirro.cli.controller import (
    run_configure,
    run_create_pipeline_config,
    run_download,
    run_ingest,
    run_list_datasets,
    run_list_files,
    run_list_projects,
    run_upload_reference,
    run_validate_folder,
)
from cirro.cli.tui.log_screen import LogScreen
from cirro.cli.tui.records_screen import RecordsScreen


# --- Menu ------------------------------------------------------------------


@dataclass
class MenuItem:
    label: str
    run: Callable[[Any], None]  # takes the App


def _workflow(title: str, target: Callable[..., Any], *args, **kwargs) -> Callable[[Any], None]:
    """Return an action that pushes a LogScreen running ``target`` with the
    supplied default args."""

    def action(app) -> None:
        app.push_screen(LogScreen(title, target, *args, **kwargs))

    return action


def _empty_args(**overrides) -> dict:
    """A default arguments dict for the controllers — interactive gathers the rest."""
    base = {
        "project": None,
        "dataset": None,
        "data_directory": None,
        "name": None,
        "description": "",
        "data_type": None,
        "file": None,
        "file_limit": 100000,
        "include_hidden": False,
        "interactive": True,
        "reference_type": None,
        "reference_file": None,
        "pipeline_dir": ".",
        "entrypoint": "main.wdl",
        "output_dir": ".cirro",
    }
    base.update(overrides)
    return base


def _run_configure(app) -> None:
    app.push_screen(LogScreen("Configure Cirro", run_configure))


def _run_list_projects(app) -> None:
    """Fetch projects and drill into datasets on select."""

    def fetch():
        from cirro.cirro_client import CirroApi

        cirro = CirroApi(user_agent="Cirro TUI")
        return cirro, cirro.projects.list()

    def make_screen(result):
        cirro, projects = result
        return RecordsScreen(
            "Projects",
            projects,
            columns=[
                ("id", lambda p: p.id),
                ("name", lambda p: p.name),
                ("description", lambda p: getattr(p, "description", "")),
            ],
            on_select=lambda project: _browse_datasets(app, cirro, project),
        )

    app.push_screen(LoadingScreen("Loading projects…", fetch, make_screen))


def _browse_datasets(app, cirro, project) -> None:
    def fetch():
        from cirro.services.service_helpers import list_all_datasets

        return list_all_datasets(project_id=project.id, client=cirro)

    def make_screen(datasets):
        return RecordsScreen(
            f"Datasets in {project.name}",
            datasets,
            columns=[
                ("id", lambda d: d.id),
                ("name", lambda d: d.name),
                ("status", lambda d: getattr(getattr(d, "status", None), "value", d.status)),
                ("created", lambda d: getattr(d, "created_at", "")),
            ],
            on_select=lambda dataset: _browse_files(app, cirro, project, dataset),
        )

    app.push_screen(LoadingScreen(f"Loading datasets in {project.name}…", fetch, make_screen))


def _browse_files(app, cirro, project, dataset) -> None:
    def fetch():
        listing = cirro.datasets.get_assets_listing(project.id, dataset.id, file_limit=100000)
        return listing.files

    def make_screen(files):
        return RecordsScreen(
            f"Files in {dataset.name}",
            files,
            columns=[
                ("path", lambda f: getattr(f, "normalized_path", getattr(f, "relative_path", ""))),
                ("size", lambda f: getattr(f, "size", "")),
            ],
        )

    app.push_screen(LoadingScreen(f"Loading files in {dataset.name}…", fetch, make_screen))


MENU: List[MenuItem] = [
    MenuItem("Upload dataset",         _workflow("Upload dataset",         run_ingest,           _empty_args(), interactive=True)),
    MenuItem("Download dataset",       _workflow("Download dataset",       run_download,         _empty_args(), interactive=True)),
    MenuItem("Validate local folder",  _workflow("Validate local folder",  run_validate_folder,  _empty_args(), interactive=True)),
    MenuItem("Upload reference",       _workflow("Upload reference",       run_upload_reference, _empty_args(), interactive=True)),
    MenuItem("List datasets (table)",  _workflow("List datasets",          run_list_datasets,    _empty_args(), interactive=True)),
    MenuItem("List files (table)",     _workflow("List files",             run_list_files,       _empty_args(), interactive=True)),
    MenuItem("Browse projects",        _run_list_projects),
    MenuItem("Create pipeline config", _workflow("Create pipeline config", run_create_pipeline_config, _empty_args(), interactive=True)),
    MenuItem("Show projects (log)",    _workflow("List projects",          run_list_projects)),
    MenuItem("Configure auth",         _run_configure),
]


# --- LoadingScreen ---------------------------------------------------------


class LoadingScreen(Screen):
    """Show a message, run ``fetch()`` in a worker, replace itself with
    ``make_screen(result)``. Cancels back to previous screen on error."""

    DEFAULT_CSS = """
    LoadingScreen { align: center middle; }
    #loading-msg {
        padding: 2 4;
        border: round $accent;
        background: $surface;
        color: $text;
    }
    """

    BINDINGS = [Binding("escape", "app.pop_screen", "Back")]

    def __init__(self, title: str, fetch: Callable[[], Any], make_screen: Callable[[Any], Screen]):
        super().__init__()
        self._title = title
        self._fetch = fetch
        self._make_screen = make_screen

    def compose(self) -> ComposeResult:
        yield Header(show_clock=False)
        yield Label(self._title, id="loading-msg")
        yield Footer()

    def on_mount(self) -> None:
        self.run_worker(self._go, thread=True, exclusive=False, name=self._title)

    def _go(self) -> None:
        try:
            result = self._fetch()
        except BaseException as e:  # noqa: BLE001
            msg = f"{type(e).__name__}: {e}"
            self.app.call_from_thread(self._fail, msg)
            return
        self.app.call_from_thread(self._ok, result)

    def _ok(self, result: Any) -> None:
        self.app.pop_screen()
        self.app.push_screen(self._make_screen(result))

    def _fail(self, msg: str) -> None:
        self.app.pop_screen()
        try:
            self.app.notify(msg, severity="error", timeout=8)
        except Exception:
            pass
