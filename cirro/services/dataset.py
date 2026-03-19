import logging
from pathlib import Path
from typing import List, Optional, Union, Dict

from cirro_api_client.v1.api.datasets import get_datasets, get_dataset, import_public_dataset, upload_dataset, \
    update_dataset, delete_dataset, get_dataset_manifest
from cirro_api_client.v1.api.sharing import get_shared_datasets
from cirro_api_client.v1.models import ImportDataRequest, UploadDatasetRequest, UpdateDatasetRequest, Dataset, \
    DatasetDetail, CreateResponse, UploadDatasetCreateResponse, FileEntry

from cirro.file_utils import is_hidden_file
from cirro.models.assets import DatasetAssets, Artifact
from cirro.models.dataset import DatasetValidationResponse
from cirro.models.file import FileAccessContext, File, PathLike
from cirro.services.base import get_all_records
from cirro.services.file import FileEnabledService

logger = logging.getLogger()


class DatasetService(FileEnabledService):
    """
    Service for interacting with the Dataset endpoints
    """

    def list(self, project_id: str, max_items: int = 10000) -> List[Dataset]:
        """List datasets

        Retrieves a list of datasets for a given project

        Args:
            project_id (str): ID of the Project
            max_items (int): Maximum number of records to get (default 10,000)
        """
        return get_all_records(
            records_getter=lambda page_args: get_datasets.sync(
                project_id=project_id,
                client=self._api_client,
                next_token=page_args.next_token,
                limit=page_args.limit
            ),
            max_items=max_items
        )

    def list_shared(self, project_id: str, share_id: str, max_items: int = 10000) -> List[Dataset]:
        """
        Retrieves a list of shared datasets for a given project and share

        Args:
            project_id (str): ID of the Project
            share_id (str): ID of the Share
            max_items (int): Maximum number of records to get (default 10,000)

        Example:
        ```python
        from cirro_api_client.v1.models import ShareType
        from cirro.cirro_client import CirroApi

        cirro = CirroApi()

        # List shares that are subscribed to
        subscribed_shares = cirro.shares.list(project_id="project-id", share_type=ShareType.SUBSCRIBER)
        cirro.datasets.list_shared("project-id", subscribed_shares[0].id)
        ```
        """
        return get_all_records(
            records_getter=lambda page_args: get_shared_datasets.sync(
                project_id=project_id,
                share_id=share_id,
                client=self._api_client,
                next_token=page_args.next_token,
                limit=page_args.limit
            ),
            max_items=max_items
        )

    def import_public(self, project_id: str, import_request: ImportDataRequest) -> CreateResponse:
        """
        Download data from public repositories

        Args:
            project_id (str): ID of the Project
            import_request (cirro_api_client.v1.models.ImportDataRequest):

        Returns:
            ID of the created dataset

        ```python
        from cirro_api_client.v1.models import ImportDataRequest, Tag
        from cirro.cirro_client import CirroApi

        cirro = CirroApi()
        request = ImportDataRequest(
            name="Imported dataset",
            description="Description of the dataset",
            public_ids=["SRR123456", "SRR123457"],
            tags=[Tag(value="tag1")]
        )
        cirro.datasets.import_public("project-id", request)
        ```
        """
        return import_public_dataset.sync(project_id=project_id, client=self._api_client, body=import_request)

    def create(self, project_id: str, upload_request: UploadDatasetRequest) -> UploadDatasetCreateResponse:
        """
        Registers a dataset in Cirro, which can subsequently have files uploaded to it

        Args:
            project_id (str): ID of the Project
            upload_request (cirro_api_client.v1.models.UploadDatasetRequest):

        Returns:
            ID of the created dataset and the path to upload files

        ```python
        from cirro_api_client.v1.models import UploadDatasetRequest, Tag
        from cirro.cirro_client import CirroApi

        cirro = CirroApi()
        request = UploadDatasetRequest(
            name="Name of new dataset",
            process_id="paired_dnaseq",
            expected_files=["read_1.fastq.gz", "read_2.fastq.gz"],
            description="Description of the dataset",
            tags=[Tag(value="tag1"), Tag(value="tag2")]
        )
        cirro.datasets.create("project-id", request)
        ```
        """
        return upload_dataset.sync(project_id=project_id, client=self._api_client, body=upload_request)

    def get(self, project_id: str, dataset_id: str) -> Optional[DatasetDetail]:
        """
        Gets detailed information about a dataset

        Args:
            project_id (str): ID of the Project
            dataset_id (str): ID of the Dataset

        Returns:
            The dataset, if found
        """
        return get_dataset.sync(project_id=project_id, dataset_id=dataset_id, client=self._api_client)

    def update(self, project_id: str, dataset_id: str, request: UpdateDatasetRequest) -> DatasetDetail:
        """
        Update info on a dataset (name, description, and/or tags)

        Args:
            project_id (str): ID of the Project
            dataset_id (str): ID of the Dataset
            request (cirro_api_client.v1.models.UpdateDatasetRequest):

        Returns:
            The updated dataset

        ```python
        from cirro_api_client.v1.models import UpdateDatasetRequest
        from cirro.cirro_client import CirroApi

        cirro = CirroApi()
        request = UpdateDatasetRequest(
            name="Name of new dataset",
            process_id="paired_dnaseq",
            description="Description of the dataset"
        )
        cirro.datasets.update("project-id", "dataset-id", request)
        ```
        """
        return update_dataset.sync(project_id=project_id, dataset_id=dataset_id, body=request, client=self._api_client)

    def delete(self, project_id: str, dataset_id: str) -> None:
        """
        Delete a dataset

        After a dataset has been deleted, the files associated with that
        dataset are saved according to the project's retention time.

        Args:
            project_id (str): ID of the Project
            dataset_id (str): ID of the Dataset
        """
        delete_dataset.sync_detailed(project_id=project_id, dataset_id=dataset_id, client=self._api_client)

    def get_assets_listing(self, project_id: str, dataset_id: str, file_limit: int = 100000) -> DatasetAssets:
        """
        Gets a listing of files, charts, and other assets available for the dataset

        Args:
            project_id (str): ID of the Project
            dataset_id (str): ID of the Dataset
            file_limit (int): Maximum number of files to get (default 100,000)
        """
        dataset = self.get(project_id, dataset_id)
        if file_limit < 1:
            raise ValueError("file_limit must be greater than 0")
        all_files = []
        file_offset = 0
        domain = None
        artifacts = None

        while len(all_files) < file_limit:
            manifest = get_dataset_manifest.sync(
                project_id=project_id,
                dataset_id=dataset_id,
                file_offset=file_offset,
                client=self._api_client
            )
            all_files.extend(manifest.files)
            file_offset += len(manifest.files)

            if not artifacts:
                artifacts = manifest.artifacts

            domain = manifest.domain
            if len(all_files) >= manifest.total_files or len(manifest.files) == 0:
                break

        files = [
            File.from_file_entry(
                f,
                project_id=project_id,
                dataset=dataset,
                domain=domain
            )
            for f in all_files
        ]
        artifacts = [
            Artifact(
                artifact_type=a.type_,
                file=File.from_file_entry(
                    FileEntry(a.path),
                    project_id=project_id,
                    dataset=dataset,
                    domain=domain
                )
            )
            for a in artifacts
        ]
        return DatasetAssets(files=files, artifacts=artifacts)

    def upload_files(self,
                     project_id: str,
                     dataset_id: str,
                     directory: PathLike,
                     files: List[PathLike] = None,
                     file_path_map: Dict[PathLike, str] = None) -> None:
        """
        Uploads files to a given dataset from the specified directory.

        All files must be relative to the specified directory.
        If files need to be flattened, or you are sourcing files from multiple directories,
        please include `file_path_map` or call this method multiple times.

        Args:
            project_id (str): ID of the Project
            dataset_id (str): ID of the Dataset
            directory (str|Path): Path to directory
            files (typing.List[str|Path]): List of paths to files within the directory,
                must be the same type as directory.
            file_path_map (typing.Dict[str|Path, str|Path]): Optional mapping of file paths to upload
             from source path to destination path, used to "re-write" paths within the dataset.
        ```python
        from cirro.cirro_client import CirroApi
        from cirro.file_utils import generate_flattened_file_map

        cirro = CirroApi()

        directory = "~/Downloads"
        # Re-write file paths
        file_map = {
            "data1/file1.fastq.gz": "file1.fastq.gz",
            "data2/file2.fastq.gz": "file2.fastq.gz",
            "file3.fastq.gz": "new_file3.txt"
        }

        # Or you could automate the flattening
        files = ["data1/file1.fastq.gz"]
        file_map = generate_flattened_file_map(files)

        cirro.datasets.upload_files(
            project_id="project-id",
            dataset_id="dataset-id",
            directory=directory,
            files=list(file_map.keys()),
            file_path_map=file_map
        )
        ```
        """
        if file_path_map is None:
            file_path_map = {}

        dataset = self.get(project_id, dataset_id)

        access_context = FileAccessContext.upload_dataset(
            project_id=project_id,
            dataset_id=dataset_id,
            base_url=dataset.s3
        )

        self._file_service.upload_files(
            access_context=access_context,
            directory=directory,
            files=files,
            file_path_map=file_path_map
        )

    def validate_folder(
        self,
        project_id: str,
        dataset_id: str,
        local_folder: PathLike
    ) -> DatasetValidationResponse:
        """
        Validates that the contents of a dataset match that of a local folder.
        """
        ds_files = self.get_assets_listing(project_id, dataset_id).files

        local_folder = Path(local_folder)
        if not local_folder.is_dir():
            raise ValueError(f"{local_folder} is not a valid local folder")

        # Keep track of files from the dataset which match by checksum, don't match, or are missing
        ds_files_matching = []
        ds_files_not_matching = []
        ds_files_missing = []
        ds_validate_failed = []
        for ds_file in ds_files:
            ds_file_path = ds_file.normalized_path
            # Get the corresponding local file
            local_file = local_folder / ds_file_path
            if not local_file.exists():
                ds_files_missing.append(ds_file_path)
            else:
                try:
                    if self._file_service.is_valid_file(ds_file, local_file):
                        ds_files_matching.append(ds_file_path)
                    else:
                        ds_files_not_matching.append(ds_file_path)
                except RuntimeWarning as e:
                    logger.warning(f"File validation failed: {e}")
                    ds_validate_failed.append(ds_file_path)

        # Find local files that are not in the dataset
        local_file_paths = [
            file.relative_to(local_folder).as_posix()
            for file in local_folder.rglob("*")
            if not file.is_dir() and not is_hidden_file(file)
        ]
        dataset_file_paths = [file.normalized_path for file in ds_files]
        local_only_files = [
            file
            for file in local_file_paths
            if file not in dataset_file_paths
        ]

        return DatasetValidationResponse(
            files_matching=ds_files_matching,
            files_not_matching=ds_files_not_matching,
            files_missing=ds_files_missing,
            local_only_files=local_only_files,
            validate_errors=ds_validate_failed,
        )

    def download_files(
        self,
        project_id: str,
        dataset_id: str,
        download_location: str,
        files: Union[List[File], List[str]] = None
    ) -> None:
        """
        Downloads files from a dataset

        The `files` argument is used to optionally specify a subset of files
        to be downloaded. By default, all files are downloaded.

        Args:
            project_id (str): ID of the Project
            dataset_id (str): ID of the Dataset
            download_location (str): Local destination for downloaded files
            files (typing.List[str]): Optional list of files to download
        """
        if files is None:
            files = self.get_assets_listing(project_id, dataset_id).files

        if len(files) == 0:
            return

        first_file = files[0]
        if isinstance(first_file, File):
            files = [file.relative_path for file in files]
            access_context = first_file.access_context
        else:
            dataset = self.get(project_id, dataset_id)
            if dataset.share:
                access_context = FileAccessContext.download_shared_dataset(project_id=project_id,
                                                                           dataset_id=dataset_id,
                                                                           base_url=dataset.s3)
            else:
                access_context = FileAccessContext.download(project_id=project_id,
                                                            base_url=dataset.s3)

        self._file_service.download_files(access_context, download_location, files)

    def update_samplesheet(
        self,
        project_id: str,
        dataset_id: str,
        samplesheet: str
    ):
        """
        Updates a samplesheet on a dataset

        Args:
            project_id (str): ID of the Project
            dataset_id (str): ID of the Dataset
            samplesheet (str): Samplesheet contents to update (should be a CSV string)
        """
        dataset = self.get(project_id, dataset_id)
        access_context = FileAccessContext.upload_sample_sheet(project_id=project_id,
                                                               dataset_id=dataset_id,
                                                               base_url=dataset.s3)

        samplesheet_key = f'{access_context.prefix}/samplesheet.csv'
        self._file_service.create_file(
            access_context=access_context,
            key=samplesheet_key,
            contents=samplesheet,
            content_type='text/csv'
        )
