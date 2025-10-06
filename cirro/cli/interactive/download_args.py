from fnmatch import fnmatch
from pathlib import Path
from typing import List

from cirro_api_client.v1.models import Dataset, Project

from cirro.cli.interactive.common_args import ask_project, ask_dataset
from cirro.cli.interactive.utils import ask, prompt_wrapper, InputError
from cirro.cli.models import DownloadArguments
from cirro.models.file import File


def ask_dataset_files(files: List[File]) -> List[File]:
    """Get the list of files which the user would like to download from the dataset."""

    choices = [
        "Download all files",
        "Select files from a list",
        "Select files with a naming pattern (glob)"
    ]

    selection_mode_prompt = {
        'type': 'select',
        'name': 'mode',
        'message': 'Which files would you like to download from this dataset?',
        'choices': choices
    }

    answers = prompt_wrapper(selection_mode_prompt)

    if answers['mode'] == choices[0]:
        return files
    elif answers['mode'] == choices[1]:
        return ask_dataset_files_list(files)
    else:
        return ask_dataset_files_glob(files)


def strip_prefix(fp: str, prefix: str):
    assert fp.startswith(prefix), f"Expected {fp} to start with {prefix}"
    return fp[len(prefix):]


def ask_dataset_files_list(files: List[File]) -> List[File]:
    answers = prompt_wrapper({
        'type': 'checkbox',
        'name': 'files',
        'message': 'Select the files to download',
        'choices': [
            strip_prefix(file.relative_path, "data/")
            for file in files
        ]
    })

    selected_files = [
        file
        for file in files
        if strip_prefix(file.relative_path, "data/") in set(answers['files'])
    ]

    if len(selected_files) == 0:
        if ask(
            "confirm",
            "No files were selected - try again?"
        ):
            return ask_dataset_files_list(files)
        else:
            raise InputError("No files selected")
    else:
        return selected_files


def ask_dataset_files_glob(files: List[File]) -> List[File]:

    confirmed = False
    while not confirmed:
        selected_files = ask_dataset_files_glob_single(files)
        confirmed = ask(
            "confirm",
            f'Number of files selected: {len(selected_files):} / {len(files):,}'
        )

    if len(selected_files) == 0:
        raise InputError("No files selected")

    return selected_files


def ask_dataset_files_glob_single(files: List[File]) -> List[File]:

    print("All Files:")
    for file in files:
        print(f" - {strip_prefix(file.relative_path, 'data/')}")

    answers = prompt_wrapper({
        'type': 'text',
        'name': 'glob',
        'message': 'Select files by naming pattern (using the * wildcard)',
        'default': '*'
    })

    selected_files = [
        file
        for file in files
        if fnmatch(strip_prefix(file.relative_path, "data/"), answers['glob'])
    ]

    print("Selected Files:")
    for file in selected_files:
        print(f" - {strip_prefix(file.relative_path, 'data/')}")

    return selected_files


def ask_directory(input_value: str) -> str:
    directory_prompt = {
        'type': 'path',
        'name': 'directory',
        'only_directories': True,
        'message': 'Where would you like to download these files?',
        'default': input_value or str(Path.cwd())
    }

    answers = prompt_wrapper(directory_prompt)
    return answers['directory']


def gather_download_arguments(input_params: DownloadArguments, projects: List[Project]):
    input_params['project'] = ask_project(projects, input_params.get('project'))
    return input_params


def gather_download_arguments_dataset(input_params: DownloadArguments, datasets: List[Dataset]):
    input_params['dataset'] = ask_dataset(datasets, input_params.get('dataset'), msg_action='download')
    input_params['data_directory'] = ask_directory(input_params.get('data_directory'))
    return input_params
