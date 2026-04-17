import json
import logging
import os
import sys
from pathlib import Path
from typing import List, Optional, Set

from cirro.sdk.task import DataPortalTask
from cirro_api_client.v1.models import UploadDatasetRequest, Status, Executor

from cirro.cirro_client import CirroApi
from cirro.cli.interactive.auth_args import gather_auth_config
from cirro.cli.interactive.common_args import ask_project
from cirro.cli.interactive.create_pipeline_config import gather_create_pipeline_config_arguments
from cirro.cli.interactive.download_args import gather_download_arguments, ask_dataset_files, \
    ask_dataset, gather_download_arguments_dataset
from cirro.cli.interactive.list_dataset_args import gather_list_arguments
from cirro.cli.interactive.upload_args import gather_upload_arguments
from cirro.cli.interactive.upload_reference_args import gather_reference_upload_arguments
from cirro.cli.interactive.utils import get_id_from_name, get_item_from_name_or_id, InputError, \
    validate_files, ask_yes_no, ask
from cirro.cli.interactive.validate_args import gather_validate_arguments, gather_validate_arguments_dataset
from cirro.cli.models import ListArguments, UploadArguments, DownloadArguments, CreatePipelineConfigArguments, \
    UploadReferenceArguments, ValidateArguments, ListFilesArguments, DebugArguments
from cirro.config import UserConfig, save_user_config, load_user_config
from cirro.file_utils import get_files_in_directory
from cirro.models.process import PipelineDefinition, ConfigAppStatus, CONFIG_APP_URL
from cirro.sdk.dataset import DataPortalDataset
from cirro.sdk.nextflow_utils import find_primary_failed_task
from cirro.services.service_helpers import list_all_datasets
from cirro.utils import convert_size

# Log to STDOUT
log_formatter = logging.Formatter(
    '%(asctime)s %(levelname)-8s [Cirro CLI] %(message)s'
)
logger = logging.getLogger("CLI")
logger.setLevel(logging.INFO)
console_handler = logging.StreamHandler()
console_handler.setFormatter(log_formatter)
logger.addHandler(console_handler)


def run_list_datasets(input_params: ListArguments, interactive=False):
    """List the datasets available in a particular project."""
    cirro = _init_cirro_client()
    projects = _get_projects(cirro)

    if interactive:
        # Prompt the user for the project
        input_params = gather_list_arguments(input_params, projects)
    else:
        input_params['project'] = get_id_from_name(projects, input_params['project'])

    # List the datasets available in that project
    datasets = cirro.datasets.list(input_params['project'])

    sorted_datasets = sorted(datasets, key=lambda d: d.created_at, reverse=True)

    import pandas as pd
    df = pd.DataFrame.from_records([d.to_dict() for d in sorted_datasets])
    df = df[['id', 'name', 'description', 'processId', 'status', 'createdBy', 'createdAt']]
    print(df.to_string())


def run_ingest(input_params: UploadArguments, interactive=False):
    cirro = _init_cirro_client()
    projects = _get_projects(cirro)
    processes = cirro.processes.list(process_type=Executor.INGEST)

    if interactive:
        input_params, files = gather_upload_arguments(input_params, projects, processes)
        directory = input_params['data_directory']
    else:
        input_params['project'] = get_id_from_name(projects, input_params['project'])
        input_params['data_type'] = get_id_from_name(processes, input_params['data_type'])
        directory = input_params['data_directory']
        all_files = get_files_in_directory(directory)
        if input_params['file']:
            files = input_params['file']
            validate_files(all_files, files, directory)

        # Default to all files if file param is not provided
        else:
            files = all_files

    if len(files) == 0:
        raise InputError("No files to upload")

    process = get_item_from_name_or_id(processes, input_params['data_type'])
    logger.info(f"Validating expected files: {process.name}")
    try:
        cirro.processes.check_dataset_files(process_id=process.id, files=files, directory=directory)
    except ValueError as e:
        raise InputError(e)
    logger.info("Creating new dataset")

    upload_dataset_request = UploadDatasetRequest(
        process_id=process.id,
        name=input_params['name'],
        description=input_params['description'],
        expected_files=files
    )

    project_id = get_id_from_name(projects, input_params['project'])
    create_resp = cirro.datasets.create(project_id=project_id,
                                        upload_request=upload_dataset_request)

    logger.info("Uploading files")
    cirro.datasets.upload_files(project_id=project_id,
                                dataset_id=create_resp.id,
                                directory=directory,
                                files=files)
    logger.info(f"File content validated by {cirro.configuration.checksum_method_display}")


def run_validate_folder(input_params: ValidateArguments, interactive=False):
    cirro = _init_cirro_client()
    projects = _get_projects(cirro)

    if interactive:
        input_params = gather_validate_arguments(input_params, projects)

        input_params['project'] = get_id_from_name(projects, input_params['project'])
        datasets = list_all_datasets(project_id=input_params['project'], client=cirro)
        # Filter out datasets that are not complete
        datasets = [d for d in datasets if d.status == Status.COMPLETED]
        input_params = gather_validate_arguments_dataset(input_params, datasets)
        files = cirro.datasets.get_assets_listing(
            input_params['project'], input_params['dataset'],
            file_limit=input_params['file_limit']
        ).files

        if len(files) == 0:
            raise InputError('There are no files in this dataset to validate against')

        project_id = input_params['project']
        dataset_id = input_params['dataset']

    else:
        project_id = get_id_from_name(projects, input_params['project'])
        datasets = cirro.datasets.list(project_id)
        dataset_id = get_id_from_name(datasets, input_params['dataset'])

    logger.info("Validating files")

    results = cirro.datasets.validate_folder(
        project_id=project_id,
        dataset_id=dataset_id,
        local_folder=input_params['data_directory'],
        file_limit=input_params['file_limit']
    )

    for file_list, label, log_level in [
        (results.files_matching, "✅ Matched Files (identical in Cirro and locally)", logging.INFO),
        (results.files_not_matching, "⚠️ Checksum Mismatches (same file name, different content)", logging.WARNING),
        (results.files_missing, "⚠️ Missing Locally (present in system but not found locally)", logging.WARNING),
        (results.local_only_files, "⚠️ Unexpected Local Files (present locally but not in system)", logging.WARNING),
        (results.validate_errors, "⚠️ Validation Failed (checksums may not be available)", logging.WARNING)
    ]:
        logger.log(level=log_level, msg=f"{label}: {len(file_list):,}")
        for file in file_list:
            logger.log(level=log_level, msg=f" - {file}")


def run_download(input_params: DownloadArguments, interactive=False):
    cirro = _init_cirro_client()
    projects = _get_projects(cirro)

    files_to_download = None
    if interactive:
        input_params = gather_download_arguments(input_params, projects)

        input_params['project'] = get_id_from_name(projects, input_params['project'])
        datasets = list_all_datasets(project_id=input_params['project'], client=cirro)
        # Filter out datasets that are not complete
        datasets = [d for d in datasets if d.status == Status.COMPLETED]
        input_params = gather_download_arguments_dataset(input_params, datasets)
        files = cirro.datasets.get_assets_listing(
            input_params['project'], input_params['dataset'],
            file_limit=input_params['file_limit']
        ).files

        if len(files) == 0:
            raise InputError('There are no files in this dataset to download')

        files_to_download = ask_dataset_files(files)
        project_id = input_params['project']
        dataset_id = input_params['dataset']

    else:
        project_id = get_id_from_name(projects, input_params['project'])
        datasets = cirro.datasets.list(project_id)
        dataset_id = get_id_from_name(datasets, input_params['dataset'])

        if input_params['file']:
            all_files = cirro.datasets.get_assets_listing(
                project_id, dataset_id, file_limit=input_params['file_limit']
            ).files
            files_to_download = []

            for filepath in input_params['file']:
                if not filepath.startswith('data/'):
                    filepath = os.path.join('data/', filepath)
                file = next((f for f in all_files if f.relative_path == filepath), None)
                if not file:
                    logger.warning(f"Could not find file {filepath}. Skipping.")
                    continue
                files_to_download.append(file)

    logger.info("Downloading files")
    logger.info(f"File content validated by {cirro.configuration.checksum_method_display}")

    cirro.datasets.download_files(project_id=project_id,
                                  dataset_id=dataset_id,
                                  download_location=input_params['data_directory'],
                                  files=files_to_download,
                                  file_limit=input_params['file_limit'])


def run_list_projects():
    """List all available projects."""
    cirro = _init_cirro_client()
    projects = _get_projects(cirro)

    import pandas as pd
    df = pd.DataFrame([{'id': p.id, 'name': p.name} for p in projects])
    print(df.to_string(index=False))


def run_list_files(input_params: ListFilesArguments, interactive=False):
    """List files available in a dataset."""
    cirro = _init_cirro_client()
    projects = _get_projects(cirro)

    if interactive:
        from cirro.cli.interactive.common_args import ask_project, ask_dataset
        from cirro.services.service_helpers import list_all_datasets
        project_name = ask_project(projects, input_params.get('project'))
        project_id = get_id_from_name(projects, project_name)
        datasets = list_all_datasets(project_id=project_id, client=cirro)
        dataset_id = ask_dataset(datasets, input_params.get('dataset'), msg_action='list files for')
    else:
        project_id = get_id_from_name(projects, input_params['project'])
        datasets = cirro.datasets.list(project_id)
        dataset_id = get_id_from_name(datasets, input_params['dataset'])

    files = cirro.datasets.get_assets_listing(
        project_id, dataset_id, file_limit=input_params['file_limit']
    ).files

    if len(files) == 0:
        logger.info("No files found in this dataset")
        return

    import pandas as pd
    df = pd.DataFrame([{'path': f.normalized_path, 'size': f.size} for f in files])
    print(df.to_string(index=False))


def run_upload_reference(input_params: UploadReferenceArguments, interactive=False):
    cirro = _init_cirro_client()
    projects = _get_projects(cirro)
    reference_types = cirro.references.get_types()

    if interactive:
        input_params, files = gather_reference_upload_arguments(input_params, projects, reference_types)
    else:
        files = [Path(f) for f in input_params['reference_file']]

    project_id = get_id_from_name(projects, input_params['project'])
    reference_type = next((rt for rt in reference_types if rt.name == input_params['reference_type']), None)

    cirro.references.upload_reference(project_id=project_id,
                                      ref_type=reference_type,
                                      name=input_params['name'],
                                      reference_files=files)


def run_configure():
    auth_method, base_url, auth_method_config = gather_auth_config()
    save_user_config(UserConfig(auth_method=auth_method,
                                auth_method_config=auth_method_config,
                                base_url=base_url,
                                transfer_max_retries=None))


def run_create_pipeline_config(input_params: CreatePipelineConfigArguments, interactive=False):
    """
    Creates the pipeline configuration files for the CLI.
    This is a placeholder function that can be expanded in the future.
    """
    logger.info("Creating pipeline configuration files...")

    if interactive:
        input_params = gather_create_pipeline_config_arguments(input_params)
    else:
        if not input_params['pipeline_dir']:
            raise InputError("Root directory is required")
        if not os.path.isdir(input_params['pipeline_dir']):
            raise InputError(f"Root directory {input_params['pipeline_dir']} does not exist")

    logger.debug(input_params)
    pipeline_definition = PipelineDefinition(
        root_dir=input_params['pipeline_dir'],
        entrypoint=input_params.get('entrypoint'),
        logger=logger
    )

    output_dir = input_params.get('output_dir')
    output_paths = {filename: os.path.join(output_dir, filename)  # type: ignore
                    for filename in ['process-form.json', 'process-input.json']}

    logger.info(f"Writing pipeline configuration files to {output_dir}")
    os.makedirs(output_dir, exist_ok=True)

    with open(output_paths['process-form.json'], 'w') as f:
        logger.info(f"Writing form configuration to {output_paths['process-form.json']}")
        json.dump(pipeline_definition.form_configuration, f, indent=2)

    with open(output_paths['process-input.json'], 'w') as f:
        logger.info(f"Writing input configuration to {output_paths['process-input.json']}")
        json.dump(pipeline_definition.input_configuration, f, indent=2)

    logger.info("Pipeline configuration files created successfully.")

    if pipeline_definition.config_app_status == ConfigAppStatus.RECOMMENDED:
        logger.warning(
            "It is recommended that you verify your pipeline configuration "
            "using the Cirro Pipeline Configuration App for this pipeline:\n"
            f"{CONFIG_APP_URL}")


def run_debug(input_params: DebugArguments, interactive=False):
    """
    Debug a failed workflow execution.

    Displays the execution log, identifies the primary failed task, and
    shows its logs, inputs, and outputs.  In interactive mode the user can
    drill into the input chain to trace back the root cause.
    """
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
        input_params['dataset'] = get_id_from_name(datasets, input_params['dataset'])
        dataset_obj = get_item_from_name_or_id(datasets, input_params['dataset'])
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
        if interactive:
            if log_lines and ask_yes_no('Show full execution log?'):
                print(execution_log)
        return

    # --- Tasks from trace ---
    try:
        if interactive:
            print("\nSearching for the primary failed task (this may take a moment)...")
        tasks = sdk_dataset.tasks
    except Exception as e:
        print(f"\nCould not load task trace: {e}")
        if interactive and log_lines and ask_yes_no('Show full execution log?'):
            print(execution_log)
        return

    failed_task = find_primary_failed_task(tasks, execution_log)

    if interactive:
        if failed_task is None:
            print("\nNo failed tasks found in this execution.")
            if log_lines and ask_yes_no('Show full execution log?'):
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


def _print_task_debug(task, depth: int = 0,
                      show_script: bool = True,
                      show_log: bool = True,
                      show_files: bool = True) -> None:
    """Print all debug info for one task, indented according to its depth in the input chain."""
    indent = "  " * depth
    sep = "=" * 60
    label = "Primary Failed Task" if depth == 0 else f"Source Task [depth {depth}]"

    print(f"\n{indent}{sep}")
    print(f"{indent}{label}: {task.name}")
    print(f"{indent}{sep}")
    print(f"{indent}Status:    {task.status}")
    print(f"{indent}Exit Code: {task.exit_code}")
    print(f"{indent}Hash:      {task.hash}")
    print(f"{indent}Work Dir:  {task.work_dir}")

    if show_script:
        task_script = task.script()
        print(f"\n{indent}--- Task Script ---")
        print('\n'.join(indent + line for line in (task_script or "(empty)").splitlines()))

    if show_log:
        task_log = task.logs
        print(f"\n{indent}--- Task Log ---")
        print('\n'.join(indent + line for line in (task_log or "(empty)").splitlines()))

    if show_files:
        inputs = task.inputs
        print(f"\n{indent}--- Inputs ({len(inputs)}) ---")
        for f in inputs:
            source = f"from task: {f.source_task.name}" if f.source_task else "staged input"
            try:
                size_str = convert_size(f.size)
            except Exception:
                size_str = "unknown size"
            print(f"{indent}  {f.name}  ({size_str})  [{source}]")

        outputs = task.outputs
        print(f"\n{indent}--- Outputs ({len(outputs)}) ---")
        for f in outputs:
            try:
                size_str = convert_size(f.size)
            except Exception:
                size_str = "unknown size"
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


_BACK = "Back"
_DONE = "Done"
# Binary formats that cannot be meaningfully displayed as text
_BINARY_EXTENSIONS = {'.bam', '.cram', '.bai', '.crai', '.bcf', '.idx'}


def _print_task_header(task: DataPortalTask, indent: str, label: str) -> None:
    print(f"\n{indent}=== {label} ===")
    print(f"{indent}Name:      {task.name}")
    print(f"{indent}Status:    {task.status}")
    print(f"{indent}Exit Code: {task.exit_code}")
    print(f"{indent}Hash:      {task.hash}")
    print(f"{indent}Work Dir:  {task.work_dir}")


def _task_menu(task: DataPortalTask, depth: int = 0) -> None:
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
            content = task.script()
            print(f"\n{indent}--- Task Script ---")
            print(content if content else "(empty)")

        elif choice == "Show task log":
            content = task.logs
            print(f"\n{indent}--- Task Log ---")
            print(content if content else "(empty)")

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

    while True:
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
            source = f"from task: {f.source_task.name}" if f.source_task else "staged input"
            try:
                size_str = convert_size(f.size)
            except Exception:
                size_str = "unknown size"
            labels.append(f"{label}  ({size_str})  [{source}]")

        choices = labels + [_BACK]
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

    if suffix in _BINARY_EXTENSIONS:
        return []  # no readable options for binary formats

    options = []
    if suffix in ('.csv', '.tsv'):
        options.append("Read as CSV (first 10 rows)")
    if suffix == '.json':
        options.append("Read as JSON")
    options.append("Read as text (first 100 lines)")
    return options


def _file_menu(wf, depth: int) -> None:
    """Menu for inspecting a single WorkDirFile: read contents or drill into source task."""
    indent = "  " * depth
    source = f"from task: {wf.source_task.name}" if wf.source_task else "staged input"
    try:
        size_str = convert_size(wf.size)
    except Exception:
        size_str = "unknown size"
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
            except Exception as e:
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
            except Exception as e:
                print(f"Could not read as JSON: {e}")

        elif choice.startswith("Read as text"):
            try:
                lines = wf.readlines()
                if len(lines) > 100:
                    print('\n'.join(lines[:100]))
                    print(f"... ({len(lines) - 100} more lines)")
                else:
                    print('\n'.join(lines))
            except Exception as e:
                print(f"Could not read as text: {e}")

        elif choice.startswith("Drill into source task"):
            _task_menu(wf.source_task, depth=depth + 1)


def _init_cirro_client():
    _check_configure()
    cirro = CirroApi(user_agent="Cirro CLI")
    logger.info(f"Collecting data from {cirro.configuration.base_url}")
    return cirro


def _get_projects(cirro: CirroApi):
    logger.info("Listing available projects")
    projects = cirro.projects.list()
    if len(projects) == 0:
        raise InputError("No projects available")
    return projects


def _check_configure():
    """
    Prompts the user to do initial configuration if needed
    """
    config = load_user_config()
    if config is None:
        run_configure()
        return

    # Legacy check for old config
    if config.base_url == 'cirro.bio':
        run_configure()


def handle_error(e: Exception):
    logger.error(f"{e.__class__.__name__}: {e}")
    sys.exit(1)
