from typing import List, Optional, Dict

from cirro_api_client.v1.api.execution import run_analysis, stop_analysis, get_project_summary, \
    get_tasks_for_execution, get_task_logs, get_execution_logs
from cirro_api_client.v1.api.processes import get_process_parameters
from cirro_api_client.v1.models import RunAnalysisRequest, CreateResponse, Task

from cirro.models.form_specification import ParameterSpecification
from cirro.services.base import BaseService


class ExecutionService(BaseService):
    """
    Service for interacting with the Execution endpoints
    """
    def run_analysis(self, project_id: str, request: RunAnalysisRequest) -> CreateResponse:
        """
        Launch an analysis job running a process on a set of inputs

        Args:
            project_id (str): ID of the Project
            request (cirro_api_client.v1.models.RunAnalysisRequest):

        Returns:
            The ID of the created dataset

        ```python
        from cirro_api_client.v1.models import RunAnalysisRequest, RunAnalysisRequestParams
        from cirro.cirro_client import CirroApi

        # Example:
        # Run the "process-nf-core-rnaseq-3_8" process using input data
        # from a dataset with the id "source-dataset-id"

        # Optional analysis parameters
        params = RunAnalysisRequestParams.from_dict({
            "param_a": "val_a",
            "param_b": "val_b"
        })

        cirro = CirroApi()
        request = RunAnalysisRequest(
            name="Name of the newly created dataset",
            description="Longer description of newly created dataset",
            process_id="process-nf-core-rnaseq-3_8",
            source_dataset_ids=["source-dataset-id"],
            params=params
        )
        cirro.execution.run_analysis("project-id", request)
        ```
        """

        form_spec = get_process_parameters.sync(
            process_id=request.process_id,
            client=self._api_client
        )

        ParameterSpecification(
            form_spec
        ).validate_params(
            request.params.to_dict() if request.params else {}
        )

        return run_analysis.sync(
            project_id=project_id,
            body=request,
            client=self._api_client
        )

    def stop_analysis(self, project_id: str, dataset_id: str):
        """
        Terminates all jobs related to a running analysis

        Args:
            project_id (str): ID of the Project
            dataset_id (str): ID of the Dataset
        """

        return stop_analysis.sync(
            project_id=project_id,
            dataset_id=dataset_id,
            client=self._api_client
        )

    def get_project_summary(self, project_id: str) -> Dict[str, List[Task]]:
        """
        Gets an overview of the executions currently running in the project, by job queue

        Args:
            project_id (str): ID of the Project

        Returns:
            `cirro_api_client.v1.models.GetProjectSummaryResponse200`
        """

        return get_project_summary.sync(
            project_id=project_id,
            client=self._api_client
        ).additional_properties

    def get_execution_logs(self, project_id: str, dataset_id: str, force_live=False) -> str:
        """
        Gets live logs from main execution task

        Args:
            project_id (str): ID of the Project
            dataset_id (str): ID of the Dataset
            force_live (bool): If True, it will fetch logs from CloudWatch,
                even if the execution is already completed

        """

        resp = get_execution_logs.sync(
            project_id=project_id,
            dataset_id=dataset_id,
            force_live=force_live,
            client=self._api_client
        )

        if resp is None or resp.events is None:
            return ''
        return '\n'.join(e.message for e in resp.events)

    def get_tasks_for_execution(self, project_id: str, dataset_id: str, force_live=False) -> Optional[List[Task]]:
        """
        Gets the tasks submitted by the workflow execution

        Args:
            project_id (str): ID of the Project
            dataset_id (str): ID of the Dataset
            force_live (bool): If True, it will try to get the list of jobs
                from the executor (i.e., AWS Batch), rather than the workflow report
        """

        return get_tasks_for_execution.sync(
            project_id=project_id,
            dataset_id=dataset_id,
            force_live=force_live,
            client=self._api_client
        )

    def get_task_logs(self, project_id: str, dataset_id: str, task_id: str, force_live=False) -> str:
        """
        Gets the log output from an individual task

        Args:
            project_id (str): ID of the Project
            dataset_id (str): ID of the Dataset
            task_id (str): ID of the task
            force_live (bool): If True, it will fetch logs from CloudWatch,
                even if the execution is already completed

        """

        resp = get_task_logs.sync(
            project_id=project_id,
            dataset_id=dataset_id,
            task_id=task_id,
            force_live=force_live,
            client=self._api_client
        )

        if resp is None or resp.events is None:
            return ''
        return '\n'.join(e.message for e in resp.events)
