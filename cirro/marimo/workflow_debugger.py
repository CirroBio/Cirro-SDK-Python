import marimo

__generated_with = "0.13.0"
app = marimo.App(width="full", title="Cirro Workflow Debugger")


# ---------------------------------------------------------------------------
# Cell 1: Import marimo
# ---------------------------------------------------------------------------
@app.cell
def _():
    import marimo as mo
    return (mo,)


# ---------------------------------------------------------------------------
# Cell 2: SDK imports and Cirro API client initialization
# ---------------------------------------------------------------------------
@app.cell
def _(mo):
    import os
    import pandas as pd

    try:
        from cirro.cirro_client import CirroApi
        from cirro.sdk.dataset import DataPortalDataset
        from cirro.sdk.nextflow_utils import find_primary_failed_task
        from cirro.utils import convert_size

        # Prefer the access token injected by the CLI entrypoint so this cell
        # never has to prompt the user for credentials interactively.
        _access_token = os.environ.get("CIRRO_ACCESS_TOKEN")
        if _access_token:
            from cirro.auth.access_token import AccessTokenAuth
            _auth_info = AccessTokenAuth(token=_access_token)
            cirro_client = CirroApi(auth_info=_auth_info, user_agent="Cirro Workflow Debugger")
        else:
            cirro_client = CirroApi(user_agent="Cirro Workflow Debugger")
        _init_error = None
    except Exception as _exc:
        cirro_client = None
        DataPortalDataset = None
        find_primary_failed_task = None
        convert_size = None
        _init_error = _exc

    if _init_error is not None:
        mo.stop(
            True,
            mo.callout(
                mo.md(
                    f"**Cannot connect to Cirro**\n\n"
                    f"`{_init_error}`\n\n"
                    "Run `cirro configure` to set up your credentials, then "
                    "relaunch the debugger."
                ),
                kind="danger",
            ),
        )

    return (
        cirro_client,
        DataPortalDataset,
        find_primary_failed_task,
        convert_size,
        os,
        pd,
    )


# ---------------------------------------------------------------------------
# Cell 3: Helper rendering functions
# ---------------------------------------------------------------------------
@app.cell
def _():
    _STATUS_COLORS = {
        "COMPLETED": "#22c55e",
        "FAILED":    "#ef4444",
        "RUNNING":   "#3b82f6",
        "ABORTED":   "#f97316",
        "QUEUED":    "#8b5cf6",
    }
    _STATUS_ICONS = {
        "COMPLETED": "✓",
        "FAILED":    "✗",
        "RUNNING":   "⟳",
        "ABORTED":   "⊘",
        "QUEUED":    "○",
    }

    def status_badge(status: str) -> str:
        """Return an inline HTML pill badge for a workflow/task status."""
        s = (status or "UNKNOWN").upper()
        color = _STATUS_COLORS.get(s, "#6b7280")
        icon  = _STATUS_ICONS.get(s, "·")
        return (
            f'<span style="background:{color}; color:#fff; padding:2px 10px; '
            f'border-radius:10px; font-size:0.82em; font-weight:700; '
            f'letter-spacing:0.04em; font-family:monospace">'
            f'{icon} {s}</span>'
        )

    def fmt_size(n_bytes) -> str:
        """Human-readable file size string."""
        try:
            n = float(n_bytes)
        except (TypeError, ValueError):
            return "—"
        for unit in ("B", "KB", "MB", "GB", "TB"):
            if n < 1024:
                return f"{n:.1f} {unit}"
            n /= 1024
        return f"{n:.1f} PB"

    def shorten(text: str, max_len: int = 80) -> str:
        """Truncate a string and add ellipsis if needed."""
        return text if len(text) <= max_len else text[: max_len - 1] + "…"

    def code_block(text: str) -> str:
        """Wrap text in a scrollable, monospace pre/code block."""
        escaped = (text or "(empty)").replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
        return (
            '<pre style="background:#1e1e2e; color:#cdd6f4; padding:14px; '
            'border-radius:6px; overflow:auto; max-height:500px; '
            'font-size:0.82em; line-height:1.5; white-space:pre-wrap; '
            'word-break:break-word; font-family:\'JetBrains Mono\',\'Fira Code\',monospace">'
            f"<code>{escaped}</code></pre>"
        )

    return code_block, fmt_size, shorten, status_badge


# ---------------------------------------------------------------------------
# Cell 4: Project dropdown
# ---------------------------------------------------------------------------
@app.cell
def _(mo, cirro_client, os):
    _default_project = os.environ.get("CIRRO_DEBUG_PROJECT")

    try:
        _all_projects = sorted(cirro_client.projects.list(), key=lambda p: p.name)
        _project_opts = {p.name: p.id for p in _all_projects}
    except Exception as _e:
        _project_opts = {}

    _init_val = (
        _default_project
        if _default_project and _default_project in _project_opts
        else None
    )

    project_dropdown = mo.ui.dropdown(
        options=_project_opts,
        label="Project",
        value=_init_val,
    )
    return (project_dropdown,)


# ---------------------------------------------------------------------------
# Cell 5: Dataset dropdown (refreshes when project changes)
# ---------------------------------------------------------------------------
@app.cell
def _(mo, cirro_client, project_dropdown, os):
    _default_dataset = os.environ.get("CIRRO_DEBUG_DATASET")
    _dataset_opts: dict = {}
    _id_to_key: dict = {}

    if project_dropdown.value:
        try:
            _raw = sorted(
                cirro_client.datasets.list(project_dropdown.value),
                key=lambda d: d.created_at,
                reverse=True,
            )
            for _d in _raw:
                _status_str = (
                    _d.status.value
                    if hasattr(_d.status, "value")
                    else str(_d.status)
                )
                _key = f"{_d.name}  [{_status_str}]"
                _dataset_opts[_key] = _d.id
                _id_to_key[_d.id] = _key
        except Exception:
            pass

    _default_key = _id_to_key.get(_default_dataset) if _default_dataset else None

    dataset_dropdown = mo.ui.dropdown(
        options=_dataset_opts,
        label="Dataset",
        value=_default_key,
    )
    return (dataset_dropdown,)


# ---------------------------------------------------------------------------
# Cell 6: Reactive state — which task is open in the inspector
# ---------------------------------------------------------------------------
@app.cell
def _(mo):
    # inspected_task_name is a string (task .name) or None.
    # We use a dict so we can also store navigation history breadcrumb.
    inspected_task_name, set_inspected_task_name = mo.state(None)
    return inspected_task_name, set_inspected_task_name


# ---------------------------------------------------------------------------
# Cell 7: Load button + heavy data loading
# ---------------------------------------------------------------------------
@app.cell
def _(
    mo,
    cirro_client,
    project_dropdown,
    dataset_dropdown,
    DataPortalDataset,
    find_primary_failed_task,
):
    load_button = mo.ui.run_button(label="Load Dataset", kind="success")

    sdk_dataset = None
    tasks = None
    execution_log = ""
    primary_failed_task = None
    load_error = None

    if load_button.value and project_dropdown.value and dataset_dropdown.value:
        try:
            with mo.status.spinner("Fetching dataset metadata…"):
                _detail = cirro_client.datasets.get(
                    project_id=project_dropdown.value,
                    dataset_id=dataset_dropdown.value,
                )
                sdk_dataset = DataPortalDataset(dataset=_detail, client=cirro_client)

            with mo.status.spinner("Fetching execution log…"):
                execution_log = sdk_dataset.logs()

            with mo.status.spinner("Fetching task trace…"):
                try:
                    tasks = sdk_dataset.tasks
                    primary_failed_task = find_primary_failed_task(
                        tasks, execution_log
                    )
                except Exception as _te:
                    load_error = f"Could not load task trace: {_te}"
        except Exception as _de:
            load_error = f"Could not load dataset: {_de}"

    return (
        load_button,
        sdk_dataset,
        tasks,
        execution_log,
        primary_failed_task,
        load_error,
    )


# ---------------------------------------------------------------------------
# Cell 8: Task filter controls
# ---------------------------------------------------------------------------
@app.cell
def _(mo, tasks):
    _all_statuses = ["All"]
    if tasks:
        _seen = []
        for _t in tasks:
            if _t.status not in _seen:
                _seen.append(_t.status)
        _all_statuses += sorted(_seen)

    status_filter = mo.ui.dropdown(
        options=_all_statuses,
        value="All",
        label="Status filter",
    )
    name_search = mo.ui.text(
        placeholder="Search by task name…",
        label="Search",
    )
    return name_search, status_filter


# ---------------------------------------------------------------------------
# Cell 9: Filtered tasks list (data, not UI)
# ---------------------------------------------------------------------------
@app.cell
def _(tasks, status_filter, name_search):
    filtered_tasks = []
    if tasks:
        _query = (name_search.value or "").strip().lower()
        for _t in tasks:
            if status_filter.value != "All" and _t.status != status_filter.value:
                continue
            if _query and _query not in _t.name.lower():
                continue
            filtered_tasks.append(_t)
    return (filtered_tasks,)


# ---------------------------------------------------------------------------
# Cell 10: Tasks DataFrame table
# ---------------------------------------------------------------------------
@app.cell
def _(mo, pd, filtered_tasks):
    _rows = []
    for _t in filtered_tasks:
        _rows.append(
            {
                "#": _t.task_id,
                "Name": _t.name,
                "Status": _t.status,
                "Exit": "" if _t.exit_code is None else str(_t.exit_code),
                "Hash": _t.hash,
            }
        )

    tasks_df = pd.DataFrame(_rows) if _rows else pd.DataFrame(
        columns=["#", "Name", "Status", "Exit", "Hash"]
    )

    tasks_table = mo.ui.table(
        tasks_df,
        selection="single",
        label="",
    )
    return tasks_df, tasks_table


# ---------------------------------------------------------------------------
# Cell 11: Sync table selection → inspected task state
# ---------------------------------------------------------------------------
@app.cell
def _(tasks_table, filtered_tasks, set_inspected_task_name):
    _sel = tasks_table.value
    if _sel is not None and len(_sel) > 0:
        _row_id = int(_sel.iloc[0]["#"])
        _match = next(
            (t for t in filtered_tasks if t.task_id == _row_id), None
        )
        if _match is not None:
            set_inspected_task_name(_match.name)
    return


# ---------------------------------------------------------------------------
# Cell 12: Resolve inspected task object from name
# ---------------------------------------------------------------------------
@app.cell
def _(inspected_task_name, tasks, primary_failed_task):
    inspected_task = None
    if tasks:
        if inspected_task_name is not None:
            inspected_task = next(
                (t for t in tasks if t.name == inspected_task_name), None
            )
        # Fall back to primary failed task on first load
        if inspected_task is None and primary_failed_task is not None:
            inspected_task = primary_failed_task
    return (inspected_task,)


# ---------------------------------------------------------------------------
# Cell 13: Task inspector panel
# ---------------------------------------------------------------------------
@app.cell
def _(
    mo,
    inspected_task,
    set_inspected_task_name,
    fmt_size,
    code_block,
    status_badge,
):
    if inspected_task is None:
        task_inspector = mo.callout(
            mo.md(
                "Select a task from the **Task Explorer** tab — or load a dataset "
                "with a failed execution to jump straight to the root cause."
            ),
            kind="info",
        )
    else:
        _task = inspected_task

        # ---- Header row ----
        _status_html = mo.Html(status_badge(_task.status))
        _exit_str = str(_task.exit_code) if _task.exit_code is not None else "—"
        _header = mo.hstack(
            [
                mo.md(f"### {_task.name}"),
                _status_html,
                mo.md(f"Exit: **`{_exit_str}`**"),
                mo.md(f"Hash: `{_task.hash}`"),
            ],
            gap=2,
            align="center",
            wrap=True,
        )
        _work_dir_md = mo.md(
            f'<span style="font-size:0.8em; color:#6b7280">Work dir: '
            f'<code>{_task.work_dir or "—"}</code></span>'
        )

        # ---- Script tab ----
        _script_content = _task.script()
        _script_panel = mo.Html(code_block(_script_content or "(script not available)"))

        # ---- Log tab ----
        _log_content = _task.logs()
        _log_panel = mo.Html(code_block(_log_content or "(log not available)"))

        # ---- Inputs tab ----
        _inputs = _task.inputs
        if not _inputs:
            _inputs_panel = mo.callout(
                mo.md("No input files found (work directory may be cleaned up)."),
                kind="warn",
            )
        else:
            _rows = []
            _source_task_buttons = []
            for _i, _f in enumerate(_inputs):
                _src_name = _f.source_task.name if _f.source_task else "staged input"
                try:
                    _sz = fmt_size(_f.size)
                except Exception:
                    _sz = "unknown"
                _rows.append(
                    {
                        "File": _f.name,
                        "Size": _sz,
                        "Source Task": _src_name,
                    }
                )
                if _f.source_task is not None:
                    _src = _f.source_task
                    _btn = mo.ui.button(
                        label=f"Inspect: {_src.name[:60]}",
                        on_click=lambda _v, t=_src: set_inspected_task_name(t.name),
                        kind="neutral",
                    )
                    _source_task_buttons.append(_btn)

            import pandas as _pd
            _df = _pd.DataFrame(_rows)
            _tbl = mo.ui.table(_df, selection=None, label="")

            _nav_section = mo.md("")
            if _source_task_buttons:
                _nav_section = mo.vstack(
                    [
                        mo.md("**Navigate to source task:**"),
                        mo.vstack(_source_task_buttons, gap=1),
                    ],
                    gap=1,
                )
            _inputs_panel = mo.vstack([_tbl, _nav_section], gap=2)

        # ---- Outputs tab ----
        _outputs = _task.outputs
        if not _outputs:
            _outputs_panel = mo.callout(
                mo.md("No output files found (work directory may be cleaned up)."),
                kind="warn",
            )
        else:
            import pandas as _pd2
            _out_rows = []
            for _f in _outputs:
                try:
                    _sz = fmt_size(_f.size)
                except Exception:
                    _sz = "unknown"
                _out_rows.append({"File": _f.name, "Size": _sz})
            _outputs_panel = mo.ui.table(_pd2.DataFrame(_out_rows), selection=None, label="")

        # ---- Assemble inspector ----
        _inspector_tabs = mo.ui.tabs(
            {
                "Script (.command.sh)": _script_panel,
                "Log (.command.log)":   _log_panel,
                "Inputs":               _inputs_panel,
                "Outputs":              _outputs_panel,
            }
        )

        # Use callout as a styled header card (avoids raw HTML div nesting issues)
        _task_header_card = mo.callout(
            mo.vstack([_header, _work_dir_md], gap=1),
            kind="info",
        )

        task_inspector = mo.vstack(
            [
                _task_header_card,
                _inspector_tabs,
            ],
            gap=2,
        )
    return (task_inspector,)


# ---------------------------------------------------------------------------
# Cell 14: Assemble the complete app layout
# ---------------------------------------------------------------------------
@app.cell
def _(
    mo,
    project_dropdown,
    dataset_dropdown,
    load_button,
    sdk_dataset,
    tasks,
    execution_log,
    primary_failed_task,
    load_error,
    status_badge,
    status_filter,
    name_search,
    tasks_table,
    task_inspector,
    filtered_tasks,
):
    # ---- App header ----
    _app_title = mo.Html(
        '<h1 style="margin:0; font-size:1.6em; font-weight:700; color:#1e293b">'
        "Cirro Workflow Debugger"
        "</h1>"
    )
    _app_subtitle = mo.Html(
        '<p style="margin:4px 0 0; color:#64748b; font-size:0.9em">'
        "Interactively explore Nextflow workflow executions, failed tasks, "
        "scripts, logs, and file provenance."
        "</p>"
    )

    # ---- Selection controls ----
    _sel_controls = mo.hstack(
        [
            mo.vstack([project_dropdown], gap=0),
            mo.vstack([dataset_dropdown], gap=0),
            mo.vstack(
                [mo.Html('<div style="height:20px"></div>'), load_button], gap=0
            ),
        ],
        gap=3,
        align="end",
    )

    # ---- Top panel (always visible) ----
    _top_panel = mo.vstack(
        [
            mo.hstack([_app_title], gap=1),
            _app_subtitle,
            mo.Html('<hr style="border:none; border-top:1px solid #e2e8f0; margin:8px 0">'),
            _sel_controls,
        ],
        gap=2,
    )

    # ---- Error callout ----
    _error_section = mo.md("")
    if load_error:
        _error_section = mo.callout(
            mo.md(f"**Error:** {load_error}"), kind="danger"
        )

    # ---- Dataset info bar (visible only after loading) ----
    _info_bar = mo.md("")
    if sdk_dataset is not None:
        _st = (
            sdk_dataset.status.value
            if hasattr(sdk_dataset.status, "value")
            else str(sdk_dataset.status)
        )
        _badge_html = mo.Html(status_badge(_st))
        _created = (
            sdk_dataset.created_at.strftime("%Y-%m-%d %H:%M")
            if sdk_dataset.created_at
            else "—"
        )
        _info_bar = mo.hstack(
            [
                mo.md(f"**{sdk_dataset.name}**"),
                _badge_html,
                mo.md(f"Process: `{sdk_dataset.process_id}`"),
                mo.md(f"Created: {_created}"),
                mo.md(f"By: {sdk_dataset.created_by}"),
            ],
            gap=3,
            align="center",
            wrap=True,
        )
        _info_bar = mo.callout(_info_bar, kind="neutral")

    # ---- Primary failed task alert ----
    _failed_alert = mo.md("")
    if primary_failed_task is not None:
        _ft = primary_failed_task
        _exit_str = str(_ft.exit_code) if _ft.exit_code is not None else "—"
        _failed_alert = mo.callout(
            mo.hstack(
                [
                    mo.Html(
                        '<span style="font-size:1.3em">&#x26A0;</span>'
                    ),
                    mo.md(
                        f"**Primary failed task:** `{_ft.name}` — "
                        f"exit code `{_exit_str}` — "
                        f"hash `{_ft.hash}`"
                    ),
                ],
                gap=2,
                align="center",
            ),
            kind="danger",
        )

    # ---- Overview tab content ----
    _overview_content = mo.md("Load a dataset to view overview.")
    if sdk_dataset is not None and tasks is not None:
        _total = len(tasks)
        _by_status: dict = {}
        for _t in tasks:
            _by_status[_t.status] = _by_status.get(_t.status, 0) + 1

        def _stat_card(label: str, value: str, color: str = "#1e293b") -> object:
            return mo.Html(
                f'<div style="background:#f8fafc; border-radius:8px; padding:16px 24px; '
                f'text-align:center; border:1px solid #e2e8f0; min-width:120px">'
                f'<div style="font-size:2em; font-weight:700; color:{color}">{value}</div>'
                f'<div style="font-size:0.82em; color:#64748b; margin-top:4px">{label}</div>'
                f"</div>"
            )

        _stat_cards = [_stat_card("Total Tasks", str(_total))]
        _status_colors_map = {
            "COMPLETED": "#22c55e",
            "FAILED": "#ef4444",
            "ABORTED": "#f97316",
            "RUNNING": "#3b82f6",
        }
        for _s, _c in _by_status.items():
            _col = _status_colors_map.get(_s.upper(), "#6b7280")
            _stat_cards.append(_stat_card(_s, str(_c), _col))

        _params = {}
        try:
            _params = sdk_dataset.params or {}
        except Exception:
            pass

        _params_section = mo.md("No pipeline parameters available.")
        if _params:
            _param_rows = [
                f"| `{k}` | `{v}` |"
                for k, v in sorted(_params.items())
                if not isinstance(v, dict)
            ]
            if _param_rows:
                _params_section = mo.md(
                    "**Pipeline Parameters**\n\n"
                    "| Parameter | Value |\n"
                    "|-----------|-------|\n"
                    + "\n".join(_param_rows)
                )

        _overview_content = mo.vstack(
            [
                mo.hstack(_stat_cards, gap=2, wrap=True),
                _failed_alert,
                _params_section,
            ],
            gap=3,
        )

    # ---- Execution log tab content ----
    _log_content_view = mo.md("Load a dataset to view the execution log.")
    if execution_log:
        _log_lines = execution_log.splitlines()
        _log_len_note = (
            f"*Showing all {len(_log_lines):,} lines.*"
            if len(_log_lines) <= 2000
            else f"*Showing last 2,000 of {len(_log_lines):,} lines.*"
        )
        _truncated_log = "\n".join(_log_lines[-2000:])
        _escaped = (
            _truncated_log.replace("&", "&amp;")
            .replace("<", "&lt;")
            .replace(">", "&gt;")
        )
        _log_content_view = mo.vstack(
            [
                mo.md(_log_len_note),
                mo.Html(
                    '<pre style="background:#1e1e2e; color:#cdd6f4; padding:16px; '
                    'border-radius:8px; overflow:auto; max-height:600px; '
                    'font-size:0.78em; line-height:1.55; white-space:pre-wrap; '
                    'word-break:break-word; '
                    "font-family:'JetBrains Mono','Fira Code',monospace\">"
                    f"<code>{_escaped}</code></pre>"
                ),
            ],
            gap=1,
        )
    elif sdk_dataset is not None:
        _log_content_view = mo.callout(
            mo.md("No execution log available for this dataset."), kind="warn"
        )

    # ---- Task explorer tab content ----
    _task_count = len(filtered_tasks) if filtered_tasks else 0
    _task_total = len(tasks) if tasks else 0
    _explorer_content = mo.md("Load a dataset to explore tasks.")
    if sdk_dataset is not None and tasks is not None:
        _filter_row = mo.hstack(
            [status_filter, name_search],
            gap=2,
            align="end",
        )
        _count_note = mo.md(
            f"*Showing {_task_count} of {_task_total} tasks — "
            "click a row to open the Task Inspector.*"
        )
        _explorer_content = mo.vstack(
            [_filter_row, _count_note, tasks_table],
            gap=2,
        )
    elif sdk_dataset is not None:
        _explorer_content = mo.callout(
            mo.md("Task trace not available for this dataset."), kind="warn"
        )

    # ---- Main tabs ----
    _main_tabs = mo.ui.tabs(
        {
            "Overview":        _overview_content,
            "Execution Log":   _log_content_view,
            "Task Explorer":   _explorer_content,
        }
    )

    # ---- Task inspector section ----
    _inspector_header = mo.Html(
        '<h2 style="margin:0 0 4px; font-size:1.15em; font-weight:600; '
        'color:#1e293b; border-bottom:2px solid #3b82f6; padding-bottom:6px">'
        "Task Inspector"
        "</h2>"
    )

    # ---- Full page layout ----
    return mo.vstack(
        [
            _top_panel,
            _error_section,
            _info_bar,
            mo.Html(
                '<hr style="border:none; border-top:1px solid #e2e8f0; '
                'margin:4px 0">'
            ),
            _main_tabs,
            mo.Html(
                '<hr style="border:none; border-top:2px solid #e2e8f0; '
                'margin:16px 0 8px">'
            ),
            _inspector_header,
            task_inspector,
        ],
        gap=3,
    )


if __name__ == "__main__":
    app.run()
