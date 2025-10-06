from typing import List

from cirro_api_client.v1.models import Project, Dataset

from cirro.cli.interactive.utils import ask, prompt_wrapper, InputError
from cirro.models.dataset import DatasetWithShare
from cirro.utils import format_date


def _format_share(dataset: Dataset | DatasetWithShare) -> str:
    if isinstance(dataset, DatasetWithShare) and dataset.share:
        return f'({dataset.share.name})'
    return ''


def ask_project(projects: List[Project], input_value: str) -> str:
    project_names = sorted([project.name for project in projects])
    if len(project_names) <= 10:
        return ask(
            'select',
            'What project is this dataset associated with?',
            choices=project_names,
            default=input_value if input_value in project_names else None
        )
    else:
        return ask(
            'autocomplete',
            'What project is this dataset associated with? (use TAB to display options)',
            choices=project_names,
            default=input_value if input_value in project_names else ''
        )


def ask_dataset(datasets: List[Dataset], input_value: str, msg_action: str) -> str:
    if len(datasets) == 0:
        raise InputError("No datasets available")
    sorted_datasets = sorted(datasets, key=lambda d: d.created_at, reverse=True)
    dataset_prompt = {
        'type': 'autocomplete',
        'name': 'dataset',
        'message': f'What dataset would you like to {msg_action}? (Press Tab to see all options)',
        'choices': [f'{dataset.name} - {dataset.id}' for dataset in sorted_datasets],
        'meta_information': {
            f'{dataset.name} - {dataset.id}': f'{format_date(dataset.created_at)} {_format_share(dataset)}'
            for dataset in datasets
        },
        'ignore_case': True
    }
    answers = prompt_wrapper(dataset_prompt)
    choice = answers['dataset']
    # Map the answer to a dataset
    for dataset in datasets:
        if f'{dataset.name} - {dataset.id}' == choice:
            return dataset.id

    # The user has made a selection which does not match
    # any of the options available.
    # This is most likely because there was a typo
    if ask(
        'confirm',
        'The selection does match an option available - try again?'
    ):
        return ask_dataset(datasets, input_value, msg_action)
    raise InputError("Exiting - no dataset selected")
