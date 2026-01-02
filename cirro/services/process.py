from pathlib import Path
from typing import List, Optional

from cirro_api_client.v1.api.processes import get_processes, get_process, get_process_parameters, \
    validate_file_requirements, archive_custom_process, create_custom_process, sync_custom_process, \
    update_custom_process
from cirro_api_client.v1.models import ValidateFileRequirementsRequest, Executor, Process, ProcessDetail, \
    CustomPipelineSettings, CustomProcessInput, CreateResponse

from cirro.models.form_specification import ParameterSpecification
from cirro.services.base import BaseService


class ProcessService(BaseService):
    """
    Service for interacting with the Process endpoints
    """
    def list(self, process_type: Executor = None, include_archived=False) -> List[Process]:
        """
        Retrieves a list of available processes

        Args:
            process_type (`cirro_api_client.v1.models.Executor`): Optional process type (INGEST, CROMWELL, or NEXTFLOW)
            include_archived: Whether to include archived processes in the list (default False)
        """
        processes = get_processes.sync(client=self._api_client, include_archived=include_archived)
        return [p for p in processes if not process_type or process_type == p.executor]

    def get(self, process_id: str) -> ProcessDetail:
        """
        Retrieves detailed information on a process

        Args:
            process_id (str): Process ID
        """
        return get_process.sync(process_id=process_id, client=self._api_client)

    def archive(self, process_id: str):
        """
        Removes a custom process from the list of available processes.

        Error will be raised if the requested process does not exist. No value
        is returned, and no error raised if process exists and request is satisfied.

        Args:
            process_id (str): Process ID
        """
        archive_custom_process.sync_detailed(process_id=process_id, client=self._api_client)

    def find_by_name(self, name: str) -> Optional[ProcessDetail]:
        """
        Get a process by its display name

        Args:
            name (str): Process name
        """
        matched_process = next((p for p in self.list() if p.name == name), None)
        if not matched_process:
            return None

        return self.get(matched_process.id)

    def get_parameter_spec(self, process_id: str) -> ParameterSpecification:
        """
        Gets a specification used to describe the parameters used in the process

        Args:
            process_id (str): Process ID
        """
        form_spec = get_process_parameters.sync(process_id=process_id, client=self._api_client)
        return ParameterSpecification(form_spec)

    def check_dataset_files(self, files: List[str], process_id: str, directory: str):
        """
        Checks if the file mapping rules for a process are met by the list of files

        Error will be raised if the file mapping rules for the process are not met.
        No value is returned and no error is raised if the rules are satisfied.

        Args:
            process_id (str): ID for the process containing the file mapping rules
            directory: path to directory containing files
            files (List[str]): File names to check
        """
        # Parse sample sheet file if present
        sample_sheet = None
        sample_sheet_file = Path(directory, 'samplesheet.csv')
        if sample_sheet_file.exists():
            sample_sheet = sample_sheet_file.read_text()

        request = ValidateFileRequirementsRequest(
            file_names=files,
            sample_sheet=sample_sheet
        )
        requirements = validate_file_requirements.sync(process_id=process_id, body=request, client=self._api_client)

        # These will be sample sheet errors or no files errors
        if error_msg := requirements.error_msg:
            raise ValueError(error_msg)

        errors = [
            f'{entry.description}. {entry.error_msg}. We accept any of the following naming conventions: \n\t- ' +
            '\n\t- '.join([
                e.example_name
                for e in entry.allowed_patterns
            ])
            for entry in requirements.allowed_data_types
            if entry.error_msg is not None
        ]

        files_provided = ', '.join(files)

        if len(errors) != 0:
            raise ValueError(f"The files you have provided are: {files_provided} \n\n"
                             "They do not meet the dataset requirements. "
                             "The required file types are: \n" +
                             "\n".join(errors))

    def create_custom_process(self, process: CustomProcessInput) -> CreateResponse:
        """
        Creates a custom process (pipeline or data type) in the system.

        Whether it is a pipeline or data type is determined by the executor field.
        For pipelines, you must specify the pipeline code and configuration repository.

        This process will be available to all users in the system if `is_tenant_wide` is set to True.
        If `is_tenant_wide` is set to False, the process will only be available the projects
         specified in `linked_projects_ids`. Making it available tenant wide requires tenant admin privileges.

        If the repository is private, you must complete the authorization flow on the UI.

        See https://docs.cirro.bio/pipelines/importing-custom-pipelines/ for more info.

        Args:
            process (Process): Process to create

        Example:
        ```python
        from cirro_api_client.v1.models import CustomProcessInput, Executor, \
         PipelineCode, FileMappingRule, FileNamePattern, RepositoryType, CustomPipelineSettings
        from cirro.cirro_client import CirroApi

        cirro = CirroApi()

        # New pipeline
        new_pipeline = CustomProcessInput(
            id="my_pipeline",
            name="My Pipeline",
            description="This is a test pipeline",
            executor=Executor.CROMWELL,
            category="DNA Sequencing",
            child_process_ids=[],
            parent_process_ids=["rnaseq"],
            documentation_url="https://example.com/docs",
            pipeline_code=PipelineCode(
                repository_path="CirroBio/test-pipeline",
                version="v1.0.0",
                entry_point="main.nf",
                repository_type=RepositoryType.GITHUB_PUBLIC
            ),
            linked_project_ids=[],
            is_tenant_wide=True,
            allow_multiple_sources=True,
            uses_sample_sheet=True,
            # This can be the same or different from the pipeline_code
            custom_settings=CustomPipelineSettings(
                repository="CirroBio/test-pipeline",
                branch="v1.0.0",
                repository_type=RepositoryType.GITHUB_PUBLIC,
            ),
            file_mapping_rules=[
                FileMappingRule(
                    description="Filtered Feature Matrix",
                    file_name_patterns=[
                        FileNamePattern(
                            example_name="filtered_feature_bc_matrix.h5",
                            description="Matrix",
                            sample_matching_pattern="(?P<sampleName>[\\S ]*)\\.jpg"
                        )
                    ]
                )
            ]
        )

        cirro.processes.create_custom_process(new_pipeline)

        # New data type
        new_data_type = CustomProcessInput(
            id="images_jpg",
            name="JPG Images",
            description="Used for generic JPG images",
            executor=Executor.INGEST,
            child_process_ids=[],
            parent_process_ids=[],
            documentation_url="https://example.com/docs",
            linked_project_ids=["project_id_1", "project_id_2"],
            file_mapping_rules=[
                FileMappingRule(
                    description="Images",
                    min_=1,
                    file_name_patterns=[
                        FileNamePattern(
                            example_name="image.jpg",
                            description="JPG Image",
                            sample_matching_pattern="(?P<sampleName>[\\S ]*)/outs/image\\.jpg"
                        )
                    ]
                )
            ]
        )
        cirro.processes.create_custom_process(new_data_type)
        ```
        """
        return create_custom_process.sync(client=self._api_client, body=process)

    def update_custom_process(self, process_id: str, process: CustomProcessInput):
        """
        Updates the custom process (pipeline or data type) in the system.

        Please run `sync_custom_process` after updating to ensure the pipeline configuration is up to date.

        Args:
            process_id (str): ID of the process to update
            process (CustomProcessInput): Process to update
        """
        update_custom_process.sync_detailed(client=self._api_client,
                                            process_id=process_id,
                                            body=process)

    def sync_custom_process(self, process_id: str) -> CustomPipelineSettings:
        """
        Syncs a custom pipeline in the system.

        This updates the pipeline configurations in Cirro (form configuration,
         input mapping, preprocess etc.) to match what is in the configured repository.
        See https://docs.cirro.bio/pipelines/configuring-pipeline/ for more details.

        Args:
            process_id (str): ID of the process to sync
        """
        return sync_custom_process.sync(client=self._api_client, process_id=process_id)
