from functools import cache
from time import sleep
from typing import List, Union

from cirro_api_client.v1.models import Project, UploadDatasetRequest, Dataset, Sample, Tag, Status

from cirro.cirro_client import CirroApi
from cirro.file_utils import get_files_in_directory
from cirro.sdk.asset import DataPortalAssets, DataPortalAsset
from cirro.sdk.dataset import DataPortalDataset, DataPortalDatasets
from cirro.sdk.exceptions import DataPortalAssetNotFound, DataPortalInputError
from cirro.sdk.helpers import parse_process_name_or_id
from cirro.sdk.process import DataPortalProcess
from cirro.sdk.reference import DataPortalReference, DataPortalReferences
from cirro.sdk.reference_type import DataPortalReferenceType, DataPortalReferenceTypes
from cirro.services.service_helpers import list_all_datasets


class DataPortalProject(DataPortalAsset):
    """
    Projects in the Data Portal contain collections of Datasets.
    Users are granted permissions at the project-level, allowing them
    to view and/or modify all the datasets in that collection.
    """
    def __init__(self, proj: Project, client: CirroApi):
        """
        Instantiate with helper method

        ```python
        from cirro import DataPortal()
        portal = DataPortal()
        project = portal.get_project_by_name("Project Name")
        ```

        """
        self._data = proj
        self._client = client

    @property
    def id(self) -> str:
        """
        Unique identifier
        """
        return self._data.id

    @property
    def name(self) -> str:
        """
        Readable name
        """
        return self._data.name

    @property
    def description(self) -> str:
        """
        Longer description of the project
        """
        return self._data.description

    @property
    def status(self) -> Status:
        """
        Status of the project
        """
        return self._data.status

    def __str__(self):
        """Control how the Project is rendered as a string."""

        return '\n'.join([
            f"{i.title()}: {self.__getattribute__(i)}"
            for i in ['name', 'id', 'description']
        ])

    @cache
    def _get_datasets(self) -> List[Dataset]:
        return list_all_datasets(project_id=self.id,
                                 client=self._client)

    def list_datasets(self, force_refresh=False) -> DataPortalDatasets:
        """List all the datasets available in the project."""
        if force_refresh:
            self._get_datasets.cache_clear()

        return DataPortalDatasets(
            [
                DataPortalDataset(d, self._client)
                for d in self._get_datasets()
            ]
        )

    def get_dataset_by_name(self, name: str, force_refresh=False) -> DataPortalDataset:
        """Return the dataset with the specified name."""
        if force_refresh:
            self._get_datasets.cache_clear()

        dataset = next((d for d in self._get_datasets() if d.name == name), None)
        if dataset is None:
            raise DataPortalAssetNotFound(f'Dataset with name {name} not found')
        return self.get_dataset_by_id(dataset.id)

    def get_dataset_by_id(self, _id: str = None) -> DataPortalDataset:
        """Return the dataset with the specified id."""

        dataset = self._client.datasets.get(project_id=self.id, dataset_id=_id)
        if dataset is None:
            raise DataPortalAssetNotFound(f'Dataset with ID {_id} not found')
        return DataPortalDataset(dataset, self._client)

    def list_references(self, reference_type: str = None) -> DataPortalReferences:
        """
        List the references available in a project.
        Optionally filter to references of a particular type (identified by name)
        """

        # Get the complete list of references which are available
        reference_types = DataPortalReferenceTypes(
            [
                DataPortalReferenceType(ref)
                for ref in self._client.references.get_types()
            ]
        )

        # If a particular name was specified
        if reference_type is not None:
            reference_types = reference_types.filter_by_pattern(reference_type)
            if len(reference_types) == 0:
                msg = f"Could not find any reference types with the name {reference_type}"
                raise DataPortalAssetNotFound(msg)

        return DataPortalReferences(
            [
                DataPortalReference(ref, project_id=self.id, client=self._client)
                for ref in self._client.references.get_for_project(
                    self.id
                )
                if reference_type is None or ref.type == reference_type
            ]
        )

    def get_reference_by_name(self, name: str = None, ref_type: str = None) -> DataPortalReference:
        """Return the reference of a particular type with the specified name."""

        if name is None:
            raise DataPortalInputError("Must specify the reference name")

        return self.list_references(ref_type).get_by_name(name)

    def upload_dataset(
        self,
        name: str = None,
        description='',
        process: Union[DataPortalProcess, str] = None,
        upload_folder: str = None,
        files: List[str] = None,
        tags: List[str] = None,
    ):
        """
        Upload a set of files to the Data Portal, creating a new dataset.

        If the files parameter is not provided, it will upload all files in the upload folder

        Args:
            name (str): Name of newly created dataset
            description (str): Description of newly created dataset
            process (str | DataPortalProcess): Process to run may be referenced by name, ID, or object
            upload_folder (str): Folder containing files to upload
            files (List[str]): Optional subset of files to upload from the folder
            tags (List[str]): Optional list of tags to apply to the dataset
        """

        if name is None:
            raise DataPortalInputError("Must provide name for new dataset")
        if process is None:
            raise DataPortalInputError("Must provide the process which is used for ingest")
        if upload_folder is None:
            raise DataPortalInputError("Must provide upload_folder -- folder containing files to upload")

        # Parse the process provided by the user
        process = parse_process_name_or_id(process, self._client)

        # If no files were provided
        if files is None:
            # Get the list of files in the upload folder
            files = get_files_in_directory(upload_folder)

        if files is None or len(files) == 0:
            raise RuntimeWarning("No files to upload, exiting")

        # Normalize into Tag object
        if tags is not None:
            tags = [Tag(value=value) for value in tags]

        # Make sure that the files match the expected pattern
        self._client.processes.check_dataset_files(files, process.id, upload_folder)

        # Create the ingest process request
        dataset_create_request = UploadDatasetRequest(
            process_id=process.id,
            name=name,
            description=description,
            expected_files=files,
            tags=tags,
        )

        # Get the response
        create_response = self._client.datasets.create(project_id=self.id,
                                                       upload_request=dataset_create_request)

        # Upload the files
        self._client.datasets.upload_files(
            project_id=self.id,
            dataset_id=create_response.id,
            directory=upload_folder,
            files=files
        )

        # Return the dataset which was created, which might take a second to update
        max_attempts = 5
        for attempt in range(max_attempts):
            try:
                return self.get_dataset_by_id(create_response.id)
            except DataPortalAssetNotFound as e:
                if attempt == max_attempts - 1:
                    raise e
                else:
                    sleep(2)

    def samples(self, max_items: int = 10000) -> List[Sample]:
        """
        Retrieves a list of samples associated with a project along with their metadata

        Args:
            max_items (int): Maximum number of records to get (default 10,000)
        """
        return self._client.metadata.get_project_samples(self.id, max_items)


class DataPortalProjects(DataPortalAssets[DataPortalProject]):
    """Collection of DataPortalProject objects"""
    asset_name = "project"
