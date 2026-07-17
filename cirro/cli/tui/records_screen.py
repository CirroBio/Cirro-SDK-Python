"""
One screen renders any list of records into a :class:`DataTable`.

Columns are either passed in (``columns=[("id", lambda r: r.id), ...]``) or
inferred from ``record.to_dict()`` / ``record.__dict__`` of the first
record. An optional ``on_select`` callback is fired with the record when
the user hits Enter — that's how the browse drilldown
(projects → datasets → files) is wired in a couple of lines each.
"""
from __future__ import annotations

from typing import Any, Callable, List, Optional, Sequence, Tuple

from textual import on
from textual.app import ComposeResult
from textual.binding import Binding
from textual.containers import Vertical
from textual.screen import Screen
from textual.widgets import DataTable, Footer, Header, Label


Column = Tuple[str, Callable[[Any], Any]]


def _infer_columns(record: Any, max_cols: int = 6) -> List[Column]:
    if hasattr(record, "to_dict"):
        try:
            d = record.to_dict()
        except Exception:
            d = {}
        if d:
            return [(k, (lambda r, key=k: getattr(r, key, r.to_dict().get(key)))) for k in list(d)[:max_cols]]
    d = getattr(record, "__dict__", {})
    if d:
        return [(k, (lambda r, key=k: getattr(r, key, None))) for k in list(d)[:max_cols]]
    return [("value", lambda r: r)]


class RecordsScreen(Screen):
    """Show any list of records as a DataTable."""

    DEFAULT_CSS = """
    RecordsScreen { align: center middle; }
    #records-box {
        width: 100%;
        height: 100%;
        padding: 1 2;
    }
    #records-title {
        text-style: bold;
        color: $accent;
        padding-bottom: 1;
    }
    DataTable { height: 1fr; }
    #records-empty {
        color: $text-muted;
        padding: 2 0;
    }
    """

    BINDINGS = [
        Binding("escape", "app.pop_screen", "Back"),
        Binding("q", "app.pop_screen", "Back"),
    ]

    def __init__(
        self,
        title: str,
        records: Sequence[Any],
        columns: Optional[List[Column]] = None,
        on_select: Optional[Callable[[Any], None]] = None,
    ):
        super().__init__()
        self._title = title
        self._records = list(records)
        self._columns = columns
        self._on_select = on_select

    def compose(self) -> ComposeResult:
        yield Header(show_clock=False)
        with Vertical(id="records-box"):
            yield Label(self._title, id="records-title")
            if not self._records:
                yield Label("(no records)", id="records-empty")
            else:
                yield DataTable(cursor_type="row", zebra_stripes=True, id="records-table")
        yield Footer()

    def on_mount(self) -> None:
        if not self._records:
            return
        table = self.query_one("#records-table", DataTable)
        columns = self._columns or _infer_columns(self._records[0])
        for name, _ in columns:
            table.add_column(name, key=name)
        for record in self._records:
            row = [_cell(getter(record)) for _, getter in columns]
            table.add_row(*row, key=str(id(record)))
        table.focus()

    @on(DataTable.RowSelected)
    def _on_row_selected(self, event: DataTable.RowSelected) -> None:
        if not self._on_select:
            return
        idx = event.cursor_row
        if 0 <= idx < len(self._records):
            self._on_select(self._records[idx])


def _cell(v: Any) -> str:
    if v is None:
        return ""
    if isinstance(v, (str, int, float, bool)):
        return str(v)
    return str(v)
