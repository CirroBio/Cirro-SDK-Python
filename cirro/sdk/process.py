from typing import List, Union

from cirro_api_client.v1.models import Process, Executor, ProcessDetail, CustomPipelineSettings, PipelineCode, \
    RunAnalysisRequest, RunAnalysisRequestParams

from cirro.cirro_client import CirroApi
from cirro.models.form_specification import ParameterSpecification
from cirro.sdk.asset import DataPortalAssets, DataPortalAsset
from cirro.sdk.exceptions import DataPortalInputError


class DataPortalProcess(DataPortalAsset):
    """Helper functions for interacting with analysis processes."""

    def __init__(self, process: Union[Process, ProcessDetail], client: CirroApi):
        """
        Instantiate with helper method

        ```python
        from cirro import DataPortal()
        portal = DataPortal()
        process = portal.get_process_by_name("Process Name")
        ```
        """
        self._data = process
        self._client = client

    @property
    def id(self) -> str:
        """Unique identifier"""
        return self._data.id

    @property
    def name(self) -> str:
        """Readable name"""
        return self._data.name

    @property
    def description(self) -> str:
        """Longer description of process"""
        return self._data.description

    @property
    def child_process_ids(self) -> List[str]:
        """List of processes which can be run on the output of this process"""
        return self._data.child_process_ids

    @property
    def executor(self) -> Executor:
        """INGEST, CROMWELL, or NEXTFLOW"""
        return self._data.executor

    @property
    def category(self) -> str:
        """Category of process"""
        return self._data.category

    @property
    def pipeline_type(self) -> str:
        """Pipeline type"""
        return self._data.pipeline_type

    @property
    def documentation_url(self) -> str:
        """Documentation URL"""
        return self._data.documentation_url

    @property
    def file_requirements_message(self) -> str:
        """Description of files required for INGEST processes"""
        return self._data.file_requirements_message

    @property
    def code(self) -> PipelineCode:
        """Pipeline code configuration"""
        return self._get_detail().pipeline_code

    @property
    def custom_settings(self) -> CustomPipelineSettings:
        """Custom settings for the process"""
        return self._get_detail().custom_settings

    def _get_detail(self) -> ProcessDetail:
        if not isinstance(self._data, ProcessDetail):
            self._data = self._client.processes.get(self.id)
        return self._data

    def __str__(self):
        return '\n'.join([
            f"{i.title()}: {self.__getattribute__(i)}"
            for i in ['name', 'id', 'description']
        ])

    def get_parameter_spec(self) -> ParameterSpecification:
        """
        Gets a specification used to describe the parameters used in the process.
        """
        return self._client.processes.get_parameter_spec(self.id)

    def run_analysis(
            self,
            name: str = None,
            project_id: str = None,
            datasets: list = None,
            description: str = "",
            params=None,
            notifications_emails: List[str] = None,
            compute_environment: str = None,
            resume_dataset_id: str = None
    ) -> str:
        """
        Runs this process on one or more input datasets, returns the ID of the newly created dataset.

        Args:
            name (str): Name of newly created dataset
            project_id (str): ID of the project to run the analysis in
            datasets (List[DataPortalDataset or str]): One or more input datasets
             (as DataPortalDataset objects or dataset ID strings)
            description (str): Description of newly created dataset
            params (dict): Analysis parameters
            notifications_emails (List[str]): Notification email address(es)
            compute_environment (str): Name or ID of compute environment to use,
             if blank it will run in AWS
            resume_dataset_id (str): ID of dataset to resume from, used for caching task execution.
             It will attempt to re-use the previous output to minimize duplicate work.
            Note that Nextflow does not require this parameter, as it will automatically resume
             from any previous attempts using a global cache.

        Returns:
            dataset_id (str): ID of newly created dataset
        """
        if name is None:
            raise DataPortalInputError("Must specify 'name' for run_analysis")
        if project_id is None:
            raise DataPortalInputError("Must specify 'project_id' for run_analysis")
        if not datasets:
            raise DataPortalInputError("Must specify 'datasets' for run_analysis")
        if notifications_emails is None:
            notifications_emails = []
        if params is None:
            params = {}

        # Accept DataPortalDataset objects or raw ID strings
        source_dataset_ids = [
            ds if isinstance(ds, str) else ds.id
            for ds in datasets
        ]

        if compute_environment:
            compute_environment_name = compute_environment
            compute_environments = self._client.compute_environments.list_environments_for_project(
                project_id=project_id
            )
            compute_environment = next(
                (env for env in compute_environments
                 if env.name == compute_environment or env.id == compute_environment),
                None
            )
            if compute_environment is None:
                raise DataPortalInputError(f"Compute environment '{compute_environment_name}' not found")

        resp = self._client.execution.run_analysis(
            project_id=project_id,
            request=RunAnalysisRequest(
                name=name,
                description=description,
                process_id=self.id,
                source_dataset_ids=source_dataset_ids,
                params=RunAnalysisRequestParams.from_dict(params),
                notification_emails=notifications_emails,
                resume_dataset_id=resume_dataset_id,
                compute_environment_id=compute_environment.id if compute_environment else None
            )
        )
        return resp.id


class DataPortalProcesses(DataPortalAssets[DataPortalProcess]):
    """Collection of DataPortalProcess objects."""
    asset_name = "process"
