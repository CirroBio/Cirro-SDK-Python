from typing import List

from cirro_api_client.v1.models import Dataset, Project

from cirro.cli.interactive.common_args import ask_project
from cirro.cli.interactive.download_args import ask_dataset
from cirro.cli.models import DebugArguments


def gather_debug_arguments(
    input_params: DebugArguments,
    projects: List[Project],
    datasets: List[Dataset]
) -> DebugArguments:
    """Prompt the user to select a project and dataset for debugging."""
    input_params['project'] = ask_project(projects, input_params.get('project'))
    input_params['dataset'] = ask_dataset(datasets, input_params.get('dataset'))
    return input_params
