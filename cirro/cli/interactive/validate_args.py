from pathlib import Path

from cirro_api_client.v1.models import Dataset, Project

from cirro.cli.interactive.common_args import ask_project, ask_dataset
from cirro.cli.interactive.utils import prompt_wrapper
from cirro.cli.models import ValidateArguments


def ask_directory(input_value: str) -> str:
    directory_prompt = {
        'type': 'path',
        'name': 'directory',
        'only_directories': True,
        'message': 'What local folder would you like to compare data contents for?',
        'default': input_value or str(Path.cwd())
    }

    answers = prompt_wrapper(directory_prompt)
    return answers['directory']


def gather_validate_arguments(input_params: ValidateArguments, projects: list[Project]):
    input_params['project'] = ask_project(projects, input_params.get('project'))
    return input_params


def gather_validate_arguments_dataset(input_params: ValidateArguments, datasets: list[Dataset]):
    input_params['dataset'] = ask_dataset(datasets, input_params.get('dataset'), msg_action='validate')
    input_params['data_directory'] = ask_directory(input_params.get('data_directory'))
    return input_params
