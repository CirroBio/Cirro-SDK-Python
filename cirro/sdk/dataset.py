import datetime
from pathlib import Path
from typing import Union, List, Optional, Iterator, Tuple, Any

from cirro_api_client.v1.api.processes import validate_file_requirements
from cirro_api_client.v1.models import Dataset, DatasetDetail, RunAnalysisRequest, ProcessDetail, Status, \
    RunAnalysisRequestParams, Tag, ArtifactType, NamedItem, Executor, ValidateFileRequirementsRequest

from cirro.cirro_client import CirroApi
from cirro.file_utils import filter_files_by_pattern
from cirro.models.assets import DatasetAssets
from cirro.models.file import PathLike
from cirro.sdk.asset import DataPortalAssets, DataPortalAsset
from cirro.sdk.exceptions import DataPortalAssetNotFound
from cirro.sdk.exceptions import DataPortalInputError
from cirro.sdk.file import DataPortalFile, DataPortalFiles
from cirro.sdk.helpers import parse_process_name_or_id
from cirro.sdk.process import DataPortalProcess


def _infer_file_format(path: str) -> str:
    """Infer the file format from the file extension."""
    path_lower = path.lower()
    for ext in ('.gz', '.bz2', '.xz', '.zst'):
        if path_lower.endswith(ext):
            path_lower = path_lower[:-len(ext)]
            break
    if path_lower.endswith('.csv') or path_lower.endswith('.tsv'):
        return 'csv'
    elif path_lower.endswith('.h5ad'):
        return 'h5ad'
    elif path_lower.endswith('.json'):
        return 'json'
    elif path_lower.endswith('.parquet'):
        return 'parquet'
    elif path_lower.endswith('.feather'):
        return 'feather'
    elif path_lower.endswith('.pkl') or path_lower.endswith('.pickle'):
        return 'pickle'
    elif path_lower.endswith('.xlsx') or path_lower.endswith('.xls'):
        return 'excel'
    else:
        return 'text'


def _read_file_with_format(file: DataPortalFile, file_format: Optional[str], **kwargs) -> Any:
    """Read a file using the specified format, or auto-detect from extension."""
    if file_format is None:
        file_format = _infer_file_format(file.relative_path)
    if file_format == 'csv':
        return file.read_csv(**kwargs)
    elif file_format == 'h5ad':
        return file.read_h5ad()
    elif file_format == 'json':
        return file.read_json(**kwargs)
    elif file_format == 'parquet':
        return file.read_parquet(**kwargs)
    elif file_format == 'feather':
        return file.read_feather(**kwargs)
    elif file_format == 'pickle':
        return file.read_pickle(**kwargs)
    elif file_format == 'excel':
        return file.read_excel(**kwargs)
    elif file_format == 'text':
        return file.read(**kwargs)
    else:
        raise DataPortalInputError(
            f"Unsupported file_format: '{file_format}'. "
            f"Supported values: 'csv', 'h5ad', 'json', 'parquet', 'feather', 'pickle', 'excel', 'text'"
        )


class DataPortalDataset(DataPortalAsset):
    """
    Datasets in the Data Portal are collections of files which have
    either been uploaded directly, or which have been output by
    an analysis pipeline or notebook.
    """

    def __init__(self, dataset: Union[Dataset, DatasetDetail], client: CirroApi):
        """
        Instantiate a dataset object

        Should be invoked from a top-level constructor, for example:

        ```python
        from cirro import DataPortal()
        portal = DataPortal()
        dataset = portal.get_dataset(
            project="id-or-name-of-project",
            dataset="id-or-name-of-dataset"
        )
        ```

        """
        assert dataset.project_id is not None, "Must provide dataset with project_id attribute"
        self._data = dataset
        self._assets: Optional[DatasetAssets] = None
        self._client = client

    @property
    def id(self) -> str:
        """Unique identifier for the dataset"""
        return self._data.id

    @property
    def name(self) -> str:
        """Editable name for the dataset"""
        return self._data.name

    @property
    def description(self) -> str:
        """Longer name for the dataset"""
        return self._data.description

    @property
    def process_id(self) -> str:
        """Unique ID of process used to create the dataset"""
        return self._data.process_id

    @property
    def process(self) -> ProcessDetail:
        """
        Object representing the process used to create the dataset
        """
        return self._client.processes.get(self.process_id)

    @property
    def project_id(self) -> str:
        """ID of the project containing the dataset"""
        return self._data.project_id

    @property
    def status(self) -> Status:
        """
        Status of the dataset
        """
        return self._data.status

    @property
    def source_dataset_ids(self) -> List[str]:
        """IDs of the datasets used as sources for this dataset (if any)"""
        return self._data.source_dataset_ids

    @property
    def source_datasets(self) -> List['DataPortalDataset']:
        """
        Objects representing the datasets used as sources for this dataset (if any)
        """
        return [
            DataPortalDataset(
                dataset=self._client.datasets.get(project_id=self.project_id, dataset_id=dataset_id),
                client=self._client
            )
            for dataset_id in self.source_dataset_ids
        ]

    @property
    def params(self) -> dict:
        """
        Parameters used to generate the dataset
        """
        return self._get_detail().params.to_dict()

    @property
    def info(self) -> dict:
        """
        Extra information about the dataset
        """
        return self._get_detail().info.to_dict()

    @property
    def tags(self) -> List[Tag]:
        """
        Tags applied to the dataset
        """
        return self._data.tags

    @property
    def share(self) -> Optional[NamedItem]:
        """
        Share associated with the dataset, if any.
        """
        return self._get_detail().share

    @property
    def created_by(self) -> str:
        """User who created the dataset"""
        return self._data.created_by

    @property
    def created_at(self) -> datetime.datetime:
        """Timestamp of dataset creation"""
        return self._data.created_at

    def _get_detail(self):
        if not isinstance(self._data, DatasetDetail):
            self._data = self._client.datasets.get(project_id=self.project_id, dataset_id=self.id)
        return self._data

    def _get_assets(self):
        if not self._assets:
            self._assets = self._client.datasets.get_assets_listing(
                project_id=self.project_id,
                dataset_id=self.id
            )
        return self._assets

    def __str__(self):
        return '\n'.join([
            f"{i.title()}: {self.__getattribute__(i)}"
            for i in ['name', 'id', 'description', 'status']
        ])

    def get_file(self, relative_path: str) -> DataPortalFile:
        """
        Get a file from the dataset using its relative path.

        Args:
            relative_path (str): Relative path of file within the dataset

        Returns:
            `from cirro.sdk.file import DataPortalFile`
        """

        # Get the list of files in this dataset
        files = self.list_files()

        # Try getting the file using the relative path provided by the user
        try:
            return files.get_by_id(relative_path)
        except DataPortalAssetNotFound:
            # Try getting the file with the 'data/' prefix prepended
            try:
                return files.get_by_id("data/" + relative_path)
            except DataPortalAssetNotFound:
                # If not found, raise the exception using the string provided
                # by the user, not the data/ prepended version (which may be
                # confusing to the user)
                msg = '\n'.join([f"No file found with path '{relative_path}'."])
                raise DataPortalAssetNotFound(msg)

    def list_files(self) -> DataPortalFiles:
        """
        Return the list of files which make up the dataset.
        """
        files = self._get_assets().files
        return DataPortalFiles(
            [
                DataPortalFile(file=file, client=self._client)
                for file in files
            ]
        )

    def read_files(
            self,
            pattern: str,
            file_format: str = None,
            **kwargs
    ) -> Iterator[Tuple[DataPortalFile, Any]]:
        """
        Read the contents of files in the dataset matching the given glob pattern.

        Uses standard glob pattern matching (e.g., ``*.csv``, ``data/**/*.tsv.gz``).
        ``*`` matches any sequence of characters within a single path segment;
        ``**`` matches zero or more path segments.

        Args:
            pattern (str): Glob pattern used to match file paths within the dataset
                (e.g., ``'*.csv'``, ``'counts/**/*.tsv.gz'``)
            file_format (str): File format used to parse each file. Supported values:

                - ``'csv'``: parse with :func:`pandas.read_csv`, returns a ``DataFrame``
                - ``'h5ad'``: parse as AnnData (requires ``anndata`` package)
                - ``'json'``: parse with :func:`json.loads`, returns a Python object
                - ``'parquet'``: parse with :func:`pandas.read_parquet`, returns a ``DataFrame``
                  (requires ``pyarrow`` or ``fastparquet``)
                - ``'feather'``: parse with :func:`pandas.read_feather`, returns a ``DataFrame``
                  (requires ``pyarrow``)
                - ``'pickle'``: deserialize with :mod:`pickle`, returns a Python object
                - ``'excel'``: parse with :func:`pandas.read_excel`, returns a ``DataFrame``
                  (requires ``openpyxl`` for ``.xlsx`` or ``xlrd`` for ``.xls``)
                - ``'text'``: read as plain text, returns a ``str``
                - ``None`` (default): infer from file extension
                  (``.csv``/``.tsv`` → ``'csv'``, ``.h5ad`` → ``'h5ad'``,
                  ``.json`` → ``'json'``, ``.parquet`` → ``'parquet'``,
                  ``.feather`` → ``'feather'``, ``.pkl``/``.pickle`` → ``'pickle'``,
                  ``.xlsx``/``.xls`` → ``'excel'``, otherwise ``'text'``)
            **kwargs: Additional keyword arguments forwarded to the file-parsing function.
                For ``'csv'`` format these are passed to :func:`pandas.read_csv`
                (e.g., ``sep='\\t'`` for TSV files).
                For ``'text'`` format these are passed to
                :meth:`~cirro.sdk.file.DataPortalFile.read`.

        Yields:
            Tuple[DataPortalFile, Any]: ``(file, content)`` for each matching file,
            where *content* type depends on *file_format*.

        Example:
            ```python
            # Read all CSV files in a dataset
            for file, df in dataset.read_files('*.csv'):
                print(file.relative_path, df.shape)

            # Read gzip-compressed TSV files using explicit format and separator
            for file, df in dataset.read_files('**/*.tsv.gz', file_format='csv', sep='\\t'):
                print(file.relative_path, df.shape)

            # Read plain-text log files
            for file, text in dataset.read_files('logs/*.log', file_format='text'):
                print(file.relative_path, text[:200])
            ```
        """
        for file in filter_files_by_pattern(list(self.list_files()), pattern):
            yield file, _read_file_with_format(file, file_format, **kwargs)

    def get_artifact(self, artifact_type: ArtifactType) -> DataPortalFile:
        """
        Get the artifact of a particular type from the dataset
        """
        artifacts = self._get_assets().artifacts
        artifact = next((a for a in artifacts if a.artifact_type == artifact_type), None)
        if artifact is None:
            raise DataPortalAssetNotFound(f"No artifact found with type '{artifact_type}'")
        return DataPortalFile(file=artifact.file, client=self._client)

    def list_artifacts(self) -> List[DataPortalFile]:
        """
        Return the list of artifacts associated with the dataset

        An artifact may be something generated as part of the analysis or other process.
        See `cirro_api_client.v1.models.ArtifactType` for the list of possible artifact types.

        """
        artifacts = self._get_assets().artifacts
        return DataPortalFiles(
            [
                DataPortalFile(file=artifact.file, client=self._client)
                for artifact in artifacts
            ]
        )

    def download_files(self, download_location: str = None) -> None:
        """
        Download all the files from the dataset to a local directory.

        Args:
            download_location (str): Path to local directory
        """

        # Alias for internal method
        self.list_files().download(download_location)

    def run_analysis(
            self,
            name: str = None,
            description: str = "",
            process: Union[DataPortalProcess, str] = None,
            params=None,
            notifications_emails: List[str] = None,
            compute_environment: str = None,
            resume_dataset_id: str = None
    ) -> str:
        """
        Runs an analysis on a dataset, returns the ID of the newly created dataset.

        The process can be provided as either a DataPortalProcess object,
        or a string which corresponds to the name or ID of the process.

        Args:
            name (str): Name of newly created dataset
            description (str): Description of newly created dataset
            process (DataPortalProcess or str): Process to run
            params (dict): Analysis parameters
            notifications_emails (List[str]): Notification email address(es)
            compute_environment (str): Name or ID of compute environment to use,
             if blank it will run in AWS
            resume_dataset_id (str): ID of dataset to resume from, used for caching task execution.
             It will attempt to re-use the previous output to minimize duplicate work

        Returns:
            dataset_id (str): ID of newly created dataset
        """
        if name is None:
            raise DataPortalInputError("Must specify 'name' for run_analysis")
        if process is None:
            raise DataPortalInputError("Must specify 'process' for run_analysis")
        if notifications_emails is None:
            notifications_emails = []
        if params is None:
            params = {}

        # If the process is a string, try to parse it as a process name or ID
        process = parse_process_name_or_id(process, self._client)

        if compute_environment:
            compute_environments = self._client.compute_environments.list_environments_for_project(
                project_id=self.project_id
            )
            compute_environment = next(
                (env for env in compute_environments
                 if env.name == compute_environment or env.id == compute_environment),
                None
            )
            if compute_environment is None:
                raise DataPortalInputError(f"Compute environment '{compute_environment}' not found")

        resp = self._client.execution.run_analysis(
            project_id=self.project_id,
            request=RunAnalysisRequest(
                name=name,
                description=description,
                process_id=process.id,
                source_dataset_ids=[self.id],
                params=RunAnalysisRequestParams.from_dict(params),
                notification_emails=notifications_emails,
                resume_dataset_id=resume_dataset_id,
                compute_environment_id=compute_environment.id if compute_environment else None
            )
        )
        return resp.id

    def update_samplesheet(self,
                           contents: str = None,
                           file_path: PathLike = None):
        """
        Updates the samplesheet metadata of a dataset.
        Provide either the contents (as a string) or a file path.
        Both must be in the format of a CSV.

        Args:
            contents (str): Samplesheet contents to update (should be a CSV string)
            file_path (PathLike): Path of file to update (should be a CSV file)

        Example:
        ```python
        dataset.update_samplesheet(
            file_path=Path('~/samplesheet.csv')
        )
        ```
        """

        if contents is None and file_path is None:
            raise DataPortalInputError("Must specify either 'contents' or 'file_path' when updating samplesheet")

        if self.process.executor != Executor.INGEST:
            raise DataPortalInputError("Cannot update a samplesheet on a non-ingest dataset")

        samplesheet_contents = contents
        if file_path is not None:
            samplesheet_contents = Path(file_path).expanduser().read_text()

        # Validate samplesheet
        file_names = [f.file_name for f in self.list_files()]
        request = ValidateFileRequirementsRequest(
            file_names=file_names,
            sample_sheet=samplesheet_contents,
        )
        requirements = validate_file_requirements.sync(process_id=self.process_id,
                                                       body=request,
                                                       client=self._client.api_client)
        if error_msg := requirements.error_msg:
            raise DataPortalInputError(error_msg)

        # Update the samplesheet if everything looks ok
        self._client.datasets.update_samplesheet(
            project_id=self.project_id,
            dataset_id=self.id,
            samplesheet=samplesheet_contents
        )


class DataPortalDatasets(DataPortalAssets[DataPortalDataset]):
    """Collection of multiple DataPortalDataset objects."""
    asset_name = "dataset"
