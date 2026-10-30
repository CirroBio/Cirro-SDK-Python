from typing import Dict, List, Union

from cirro_api_client.v1.models import Process, Executor, ProcessDetail, CustomPipelineSettings, PipelineCode, \
    RunAnalysisRequest, RunAnalysisRequestParams
from cirro_api_client.v1.types import UNSET

from cirro.cirro_client import CirroApi
from cirro.models.cirro_uri import find_dataset_ids
from cirro.models.form_specification import ParameterSpecification, StructuredInputRows
from cirro.sdk.asset import DataPortalAssets, DataPortalAsset
from cirro.sdk.exceptions import DataPortalInputError


class DataPortalProcess(DataPortalAsset):
    """Helper functions for interacting with analysis processes."""

    def __init__(self, process: Union[Process, ProcessDetail], client: CirroApi):
        """
        Instantiate with helper method

        ```python
        from cirro import DataPortal
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

        This is the authoritative source for the keys accepted by the `params`
        argument of `run_analysis`, along with their types and default values.
        Call `print()` on the result for a readable listing, or
        `validate_params()` to check a `params` dict before submitting it.

        Some processes take structured inputs (e.g., a samplesheet): tables of rows
        validated against their own schema, listed in `structured_inputs` on the result.
        Their rows are passed to `run_analysis` with `structured_inputs` rather than `params`.

        ```python
        spec = process.get_parameter_spec()
        spec.print()
        samplesheet = spec.structured_inputs["samplesheet"]
        samplesheet.print()         # the columns each row takes
        ```

        Returns:
            `cirro.models.form_specification.ParameterSpecification`
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
            resume_dataset_id: str = None,
            source_sample_ids: List[str] = None,
            structured_inputs: Dict[str, StructuredInputRows] = None
    ) -> str:
        """
        Runs this process on one or more input datasets, returns the ID of the newly created dataset.

        The analysis runs asynchronously; this returns as soon as the job is
        submitted. Call `get_parameter_spec` to discover which `params` this
        process accepts, and see
        `cirro.sdk.dataset.DataPortalDataset.run_analysis` for how to follow the
        analysis to completion.

        Args:
            name (str): Name of newly created dataset
            project_id (str): ID of the project to run the analysis in
            datasets (List[DataPortalDataset or str]): One or more input datasets
             (as DataPortalDataset objects or dataset ID strings).
             Datasets referenced by Cirro URIs in `params` or `structured_inputs` are added automatically,
             so this may be omitted when those reference all the inputs.
            description (str): Description of newly created dataset
            params (dict): Analysis parameters
            notifications_emails (List[str]): Notification email address(es)
            compute_environment (str): Name or ID of compute environment to use,
             if blank it will run in AWS
            resume_dataset_id (str): ID of dataset to resume from, used for caching task execution.
             It will attempt to re-use the previous output to minimize duplicate work.
            Note that Nextflow does not require this parameter, as it will automatically resume
             from any previous attempts using a global cache.
            source_sample_ids (List[str]): List of sample IDs to use as input for the analysis.
            structured_inputs (Dict[str, List[dict] or DataFrame]): Rows for each structured input
             the process takes (see `get_parameter_spec`), keyed by its name.
             Files and folders are referenced with Cirro URIs (see `cirro.sdk.file.DataPortalFile.cirro_uri`
             and `cirro.models.cirro_uri.format_cirro_uri`).

        Returns:
            dataset_id (str): ID of newly created dataset

        ```python
        files = dataset.list_files()

        process.run_analysis(
            name="Output",
            project_id=dataset.project_id,
            structured_inputs={
                "samplesheet": [
                    {
                        "sample": "sample1",
                        "fastq_1": files.get_by_id("data/sample1_R1.fastq.gz").cirro_uri,
                        "fastq_2": files.get_by_id("data/sample1_R2.fastq.gz").cirro_uri,
                    },
                ]
            }
        )
        ```

        A sheet's columns rarely match the structured input's exactly. Rename the
        ones that differ and keep only the columns the structured input defines
        if needed, or just pass sheet.to_dataframe() directly.

        ```python
        spec = process.get_parameter_spec().structured_inputs["samplesheet"]
        sheet = project.get_sheet_by_name("Samples").to_dataframe()

        samplesheet = (
            sheet
            # Sheet column -> structured input column
            .rename(columns={"sample_id": "sample", "read1": "fastq_1", "read2": "fastq_2"})
            # Drop sheet columns the structured input doesn't define (e.g., notes, tissue);
            .filter(items=[column.key for column in spec.columns])
        )

        process.run_analysis(
            name="Output",
            project_id=project.id,
            structured_inputs={"samplesheet": samplesheet}
        )
        ```
        """
        if name is None:
            raise DataPortalInputError("Must specify 'name' for run_analysis")
        if project_id is None:
            raise DataPortalInputError("Must specify 'project_id' for run_analysis")
        if notifications_emails is None:
            notifications_emails = []
        if params is None:
            params = {}

        spec = self.get_parameter_spec()
        try:
            request_structured_inputs = spec.build_structured_inputs(structured_inputs)
        except RuntimeError as e:
            raise DataPortalInputError(str(e)) from e

        # Accept DataPortalDataset objects or raw ID strings, along with
        # any dataset the parameters or structured inputs reference
        source_dataset_ids = list(dict.fromkeys([
            *(ds if isinstance(ds, str) else ds.id for ds in datasets or []),
            *find_dataset_ids(params, project_id),
            *find_dataset_ids(request_structured_inputs.to_dict(), project_id)
        ]))
        if not source_dataset_ids:
            raise DataPortalInputError("Must specify 'datasets' for run_analysis")

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
                source_sample_ids=source_sample_ids,
                structured_inputs=request_structured_inputs if structured_inputs else UNSET,
                compute_environment_id=compute_environment.id if compute_environment else None
            )
        )
        return resp.id


class DataPortalProcesses(DataPortalAssets[DataPortalProcess]):
    """Collection of DataPortalProcess objects."""
    asset_name = "process"
