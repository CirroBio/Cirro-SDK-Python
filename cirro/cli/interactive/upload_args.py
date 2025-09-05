import sys
from fnmatch import fnmatch
from pathlib import Path
from typing import List

from cirro_api_client.v1.models import Process, Project
from prompt_toolkit.shortcuts import CompleteStyle

from cirro.cli.interactive.common_args import ask_project
from cirro.cli.interactive.utils import ask, prompt_wrapper, InputError, DirectoryValidator
from cirro.cli.models import UploadArguments
from cirro.file_utils import get_files_in_directory, get_files_stats


def ask_data_directory(input_value: str) -> str:
    directory_prompt = {
        'type': 'path',
        'name': 'data_directory',
        'message': 'Enter the full path of the data directory',
        'validate': DirectoryValidator,
        'default': input_value or '',
        'complete_style': CompleteStyle.READLINE_LIKE,
        'only_directories': True
    }

    answers = prompt_wrapper(directory_prompt)
    return str(Path(answers['data_directory']).expanduser())


def ask_name(input_value: str) -> str:
    name_prompt = {
        'type': 'input',
        'name': 'name',
        'message': 'What is the name of this dataset?',
        'validate': lambda val: len(val.strip()) > 0 or 'Please enter a name',
        'default': input_value or ''
    }

    answers = prompt_wrapper(name_prompt)
    return answers['name']


def ask_description(input_value: str) -> str:
    description_prompt = {
        'type': 'input',
        'name': 'description',
        'message': 'Enter a description of the dataset (optional)',
        'default': input_value or ''
    }

    answers = prompt_wrapper(description_prompt)
    return answers['description']


def confirm_data_files(data_directory: str, files: List[str]):
    stats = get_files_stats([
        Path(data_directory) / file
        for file in files
    ])

    if not ask(
        "confirm",
        f'Please confirm that you wish to upload {stats.number_of_files} files ({stats.size_friendly})'
    ):
        sys.exit(1)


def ask_process(processes: List[Process], input_value: str) -> str:
    process_names = [process.name for process in processes]
    process_names.sort()
    return ask(
        'select',
        'What type of files?',
        default=input_value if input_value in process_names else None,
        choices=process_names,
        use_shortcuts=len(process_names) < 30
    )


def ask_files_in_directory(data_directory: str, include_hidden: bool) -> List[str]:
    # Get the list of all files in the directory
    # (relative to the data_directory)
    files = get_files_in_directory(
        data_directory,
        include_hidden=include_hidden
    )

    choices = [
        "Upload all files",
        "Select files from a list",
        "Select files with a naming pattern (glob)"
    ]

    choice = ask(
        'select',
        'Which files would you like to upload from this dataset?',
        choices=choices
    )

    if choice == choices[0]:
        return files
    elif choice == choices[1]:
        return ask_dataset_files_list(files)
    else:
        return ask_dataset_files_glob(files)


def ask_dataset_files_list(files: List[str]) -> List[str]:
    return prompt_wrapper({
        'type': 'checkbox',
        'name': 'files',
        'message': 'Select the files to upload',
        'choices': files
    })['files']


def ask_dataset_files_glob(files: List[str]) -> List[str]:

    selected_files = ask_dataset_files_glob_single(files)
    confirmed = ask(
        "confirm",
        f'Number of files selected: {len(selected_files):} / {len(files):,}'
    )
    while not confirmed:
        selected_files = ask_dataset_files_glob_single(files)
        confirmed = ask(
            "confirm",
            f'Number of files selected: {len(selected_files):} / {len(files):,}'
        )

    if len(selected_files) == 0:
        raise InputError("No files selected")

    return selected_files


def ask_dataset_files_glob_single(files: List[str]) -> List[str]:

    print("All Files:")
    for file in files:
        print(f" - {file}")

    answers = prompt_wrapper({
        'type': 'text',
        'name': 'glob',
        'message': 'Select files by naming pattern (using the * wildcard)',
        'default': '*'
    })

    selected_files = [
        file
        for file in files
        if fnmatch(file, answers['glob'])
    ]

    print("Selected Files:")
    for file in selected_files:
        print(f" - {file}")

    return selected_files


def gather_upload_arguments(input_params: UploadArguments, projects: List[Project], processes: List[Process]):
    input_params['project'] = ask_project(projects, input_params.get('project'))

    input_params['data_directory'] = ask_data_directory(input_params.get('data_directory'))
    files = ask_files_in_directory(input_params['data_directory'], input_params['include_hidden'])

    confirm_data_files(input_params['data_directory'], files)

    input_params['data_type'] = ask_process(processes, input_params.get('data_type'))

    data_directory_name = Path(input_params['data_directory']).name
    default_name = input_params.get('name') or data_directory_name
    input_params['name'] = ask_name(default_name)
    input_params['description'] = ask_description(input_params.get('description'))
    return input_params, files
