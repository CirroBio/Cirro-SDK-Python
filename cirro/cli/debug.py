import json
from pathlib import Path
from typing import List, Optional, Set

from cirro_api_client.v1.models import Status

from cirro.cli.interactive.common_args import ask_project
from cirro.cli.interactive.download_args import ask_dataset
from cirro.cli.interactive.utils import get_id_from_name, get_item_from_name_or_id, InputError, ask_yes_no, ask
from cirro.cli.models import DebugArguments
from cirro.sdk.dataset import DataPortalDataset
from cirro.sdk.task import DataPortalTask
from cirro.services.service_helpers import list_all_datasets
from cirro.utils import convert_size

_BACK = "Back"
_DONE = "Done"
_SHOW_FULL_LOG = 'Show full execution log?'
_EMPTY_LABEL = '(empty)'
_STAGED_INPUT = 'staged input'
_UNKNOWN_SIZE = 'unknown size'
# Extensions that can be meaningfully displayed as text
_CSV_EXTENSIONS = {'.csv', '.tsv'}
_JSON_EXTENSIONS = {'.json'}
_TEXT_EXTENSIONS = _CSV_EXTENSIONS | _JSON_EXTENSIONS | {
    '.txt', '.log', '.out', '.err',
    '.md', '.rst',
    '.yaml', '.yml', '.toml', '.ini', '.cfg', '.conf',
    '.sh', '.bash', '.py', '.r', '.nf', '.wdl', '.cwl',
    '.html', '.xml',
    '.bed', '.vcf', '.gff', '.gff3', '.gtf', '.sam', '.fasta', '.fa', '.fastq', '.fq',
}


def run_debug(input_params: DebugArguments, interactive=False):  # NOSONAR
    """
    Debug a failed workflow execution.

    Displays the execution log, identifies the primary failed task, and
    shows its logs, inputs, and outputs.  In interactive mode the user can
    drill into the input chain to trace back the root cause.
    """
    from cirro.cli.controller import _init_cirro_client, _get_projects

    cirro = _init_cirro_client()
    projects = _get_projects(cirro)

    if interactive:
        project_name = ask_project(projects, input_params.get('project'))
        input_params['project'] = get_id_from_name(projects, project_name)
        datasets = list_all_datasets(project_id=input_params['project'], client=cirro)
        datasets = [d for d in datasets if d.status != Status.RUNNING]
        input_params['dataset'] = ask_dataset(datasets, input_params.get('dataset'), msg_action='debug')
    else:
        input_params['project'] = get_id_from_name(projects, input_params['project'])
        datasets = cirro.datasets.list(input_params['project'])
        original_dataset = input_params['dataset']
        input_params['dataset'] = get_id_from_name(datasets, input_params['dataset'])
        dataset_obj = get_item_from_name_or_id(datasets, original_dataset)
        if dataset_obj and dataset_obj.status == Status.RUNNING:
            raise InputError(
                f"Dataset '{dataset_obj.name}' ({dataset_obj.id}) is currently RUNNING. "
                "The debug command is only available for completed or failed datasets."
            )

    project_id = input_params['project']
    dataset_id = input_params['dataset']

    dataset_detail = cirro.datasets.get(project_id=project_id, dataset_id=dataset_id)
    sdk_dataset = DataPortalDataset(dataset=dataset_detail, client=cirro)

    # --- Execution log ---
    execution_log = sdk_dataset.logs
    log_lines = execution_log.splitlines()

    print("\n=== Execution Log (last 50 lines) ===")
    print('\n'.join(log_lines[-50:]))

    # Only search for a failed task when the dataset actually failed.
    if sdk_dataset.status != Status.FAILED:
        if interactive and log_lines and ask_yes_no(_SHOW_FULL_LOG):
            print(execution_log)
        return

    # --- Primary failed task ---
    try:
        if interactive:
            print("\nSearching for the primary failed task (this may take a moment)...")
        failed_task = sdk_dataset.primary_failed_task
    except Exception as e:  # NOSONAR
        print(f"\nCould not load task trace: {e}")
        if interactive and log_lines and ask_yes_no(_SHOW_FULL_LOG):
            print(execution_log)
        return

    if interactive:
        if failed_task is None:
            print("\nNo failed tasks found in this execution.")
            if log_lines and ask_yes_no(_SHOW_FULL_LOG):
                print(execution_log)
            return

        choices = [
            f"Show task info: {failed_task.name}",
            "Show full execution log",
            _DONE,
        ]
        while True:
            choice = ask('select', 'Primary failed task found. What would you like to do?', choices=choices)
            if choice.startswith("Show task info"):
                _task_menu(failed_task, depth=0)
            elif choice == "Show full execution log":
                print(execution_log)
            else:
                break
    else:
        if failed_task is None:
            print("\nNo failed tasks found in this execution.")
            return

        _print_task_debug_recursive(
            failed_task,
            max_depth=input_params.get('max_depth'),
            max_tasks=input_params.get('max_tasks'),
            show_script=input_params.get('show_script', True),
            show_log=input_params.get('show_log', True),
            show_files=input_params.get('show_files', True),
        )


def _print_task_debug(task, depth: int = 0,  # NOSONAR
                      show_script: bool = True,
                      show_log: bool = True,
                      show_files: bool = True) -> None:
    """Print all debug info for one task, indented according to its depth in the input chain."""
    indent = "  " * depth
    label = "Primary Failed Task" if depth == 0 else f"Source Task [depth {depth}]"
    _print_task_header(task, indent, label)

    if show_script:
        task_script = task.script
        print(f"\n{indent}--- Task Script ---")
        print('\n'.join(indent + line for line in (task_script or _EMPTY_LABEL).splitlines()))

    if show_log:
        task_log = task.logs
        print(f"\n{indent}--- Task Log ---")
        print('\n'.join(indent + line for line in (task_log or _EMPTY_LABEL).splitlines()))

    if show_files:
        inputs = task.inputs
        print(f"\n{indent}--- Inputs ({len(inputs)}) ---")
        for f in inputs:
            source = f"from task: {f.source_task.name}" if f.source_task else _STAGED_INPUT
            try:
                size_str = convert_size(f.size)
            except Exception:  # NOSONAR
                size_str = _UNKNOWN_SIZE
            print(f"{indent}  {f.name}  ({size_str})  [{source}]")

        outputs = task.outputs
        print(f"\n{indent}--- Outputs ({len(outputs)}) ---")
        for f in outputs:
            try:
                size_str = convert_size(f.size)
            except Exception:  # NOSONAR
                size_str = _UNKNOWN_SIZE
            print(f"{indent}  {f.name}  ({size_str})")


def _print_task_debug_recursive(
    task,
    max_depth: Optional[int],
    max_tasks: Optional[int],
    show_script: bool = True,
    show_log: bool = True,
    show_files: bool = True,
    _depth: int = 0,
    _seen: Optional[Set[str]] = None,
    _counter: Optional[List[int]] = None
) -> None:
    """
    Print debug info for a task and then recurse into the tasks that created
    each of its input files.

    Deduplicates tasks (a task that produced multiple inputs is only printed
    once).  Stops early when ``max_depth`` or ``max_tasks`` is reached and
    prints a notice so the user knows output was capped.
    """
    if _seen is None:
        _seen = set()
    if _counter is None:
        _counter = [0]

    if task.name in _seen:
        return

    if max_tasks is not None and _counter[0] >= max_tasks:
        indent = "  " * _depth
        print(f"\n{indent}[max-tasks limit reached — stopping recursion]")
        return

    _seen.add(task.name)
    _counter[0] += 1

    _print_task_debug(task, depth=_depth,
                      show_script=show_script,
                      show_log=show_log,
                      show_files=show_files)

    if max_depth is not None and _depth >= max_depth:
        source_tasks = [
            f.source_task for f in task.inputs
            if f.source_task and f.source_task.name not in _seen
        ]
        if source_tasks:
            indent = "  " * (_depth + 1)
            names = ', '.join(t.name for t in source_tasks)
            print(f"\n{indent}[max-depth limit reached — not expanding: {names}]")
        return

    for f in task.inputs:
        if f.source_task and f.source_task.name not in _seen:
            _print_task_debug_recursive(
                f.source_task, max_depth, max_tasks,
                show_script=show_script,
                show_log=show_log,
                show_files=show_files,
                _depth=_depth + 1, _seen=_seen, _counter=_counter
            )


def _print_task_header(task: DataPortalTask, indent: str, label: str) -> None:
    print(f"\n{indent}=== {label} ===")
    print(f"{indent}Name:      {task.name}")
    print(f"{indent}Status:    {task.status}")
    print(f"{indent}Exit Code: {task.exit_code}")
    print(f"{indent}Work Dir:  {task.work_dir}")


def _task_menu(task: DataPortalTask, depth: int = 0) -> None:  # NOSONAR
    """
    Menu-driven exploration of a single task.

    The user can show the script/log, browse inputs and outputs, and drill
    into any source task that produced an input file.  The menu loops until
    the user selects Back / Done.
    """
    indent = "  " * depth
    label = "Primary Failed Task" if depth == 0 else "Source Task"
    _print_task_header(task, indent, label)

    inputs = task.inputs
    outputs = task.outputs

    while True:
        choices = [
            "Show task script",
            "Show task log",
            f"Browse inputs ({len(inputs)})",
            f"Browse outputs ({len(outputs)})",
            _DONE if depth == 0 else _BACK,
        ]
        choice = ask('select', 'What would you like to do?', choices=choices)

        if choice == "Show task script":
            content = task.script
            print(f"\n{indent}--- Task Script ---")
            print(content if content else _EMPTY_LABEL)

        elif choice == "Show task log":
            content = task.logs
            print(f"\n{indent}--- Task Log ---")
            print(content if content else _EMPTY_LABEL)

        elif choice.startswith("Browse inputs"):
            _browse_files_menu(inputs, "input", depth)

        elif choice.startswith("Browse outputs"):
            _browse_files_menu(outputs, "output", depth)

        else:  # Done / Back
            break


def _browse_files_menu(files, kind: str, depth: int) -> None:
    """
    Let the user pick a file from a list, then enter its file menu.

    ``kind`` is ``'input'`` or ``'output'``, used only for the prompt label.
    When there is only one file the selection step is skipped and the file
    menu opens immediately.
    """
    indent = "  " * depth
    if not files:
        print(f"\n{indent}No {kind} files available.")
        return

    if len(files) == 1:
        _file_menu(files[0], depth)
        return

    # Build display labels — disambiguate duplicates by appending a counter
    seen: dict = {}
    labels = []
    for f in files:
        seen[f.name] = seen.get(f.name, 0) + 1
    counts: dict = {}
    for f in files:
        if seen[f.name] > 1:
            counts[f.name] = counts.get(f.name, 0) + 1
            label = f"{f.name} [{counts[f.name]}]"
        else:
            label = f.name
        source = f"from task: {f.source_task.name}" if f.source_task else _STAGED_INPUT
        try:
            size_str = convert_size(f.size)
        except Exception:  # NOSONAR
            size_str = _UNKNOWN_SIZE
        labels.append(f"{label}  ({size_str})  [{source}]")

    choices = labels + [_BACK]

    while True:
        choice = ask('select', f'Select a {kind} file to inspect', choices=choices)
        if choice == _BACK:
            break

        idx = labels.index(choice)
        _file_menu(files[idx], depth)


def _file_read_options(name: str):
    """Return the list of read-action strings appropriate for a given filename."""
    lower = name.lower()
    # Strip compression suffix to check underlying type
    for ext in ('.gz', '.bz2', '.zst'):
        if lower.endswith(ext):
            lower = lower[:-len(ext)]
            break

    suffix = Path(lower).suffix

    if suffix not in _TEXT_EXTENSIONS:
        return []

    options = []
    if suffix in _CSV_EXTENSIONS:
        options.append("Read as CSV (first 10 rows)")
    if suffix in _JSON_EXTENSIONS:
        options.append("Read as JSON")
    options.append("Read as text (first 100 lines)")
    return options


def _file_menu(wf, depth: int) -> None:  # NOSONAR
    """Menu for inspecting a single WorkDirFile: read contents or drill into source task."""
    indent = "  " * depth
    source = f"from task: {wf.source_task.name}" if wf.source_task else _STAGED_INPUT
    try:
        size_str = convert_size(wf.size)
    except Exception:  # NOSONAR
        size_str = _UNKNOWN_SIZE
    print(f"\n{indent}File: {wf.name}  ({size_str})  [{source}]")

    read_options = _file_read_options(wf.name)
    if not read_options and not wf.source_task:
        print(f"{indent}(binary file — no readable options)")
        return

    choices = list(read_options)
    if wf.source_task:
        choices.append(f"Drill into source task: {wf.source_task.name}")
    choices.append(_BACK)

    while True:
        choice = ask('select', f'What would you like to do with {wf.name!r}?',
                     choices=choices)

        if choice == _BACK:
            break

        elif choice.startswith("Read as CSV"):
            try:
                df = wf.read_csv()
                print(df.head(10).to_string())
            except Exception as e:  # NOSONAR
                print(f"Could not read as CSV: {e}")

        elif choice.startswith("Read as JSON"):
            try:
                data = wf.read_json()
                output = json.dumps(data, indent=2)
                # Cap output at ~200 lines so the terminal isn't flooded
                lines = output.splitlines()
                if len(lines) > 200:
                    print('\n'.join(lines[:200]))
                    print(f"... ({len(lines) - 200} more lines)")
                else:
                    print(output)
            except Exception as e:  # NOSONAR
                print(f"Could not read as JSON: {e}")

        elif choice.startswith("Read as text"):
            try:
                lines = wf.readlines()
                if len(lines) > 100:
                    print('\n'.join(lines[:100]))
                    print(f"... ({len(lines) - 100} more lines)")
                else:
                    print('\n'.join(lines))
            except Exception as e:  # NOSONAR
                print(f"Could not read as text: {e}")

        elif choice.startswith("Drill into source task"):
            _task_menu(wf.source_task, depth=depth + 1)
