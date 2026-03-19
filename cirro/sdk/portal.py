from cirro_api_client.v1.models import Executor

from cirro.cirro_client import CirroApi
from cirro.sdk.dataset import DataPortalDataset
from cirro.sdk.developer import DeveloperHelper
from cirro.sdk.exceptions import DataPortalAssetNotFound
from cirro.sdk.process import DataPortalProcess, DataPortalProcesses
from cirro.sdk.project import DataPortalProject, DataPortalProjects
from cirro.sdk.reference_type import DataPortalReferenceType, DataPortalReferenceTypes


class DataPortal:
    """
    Helper functions for exploring the Projects, Datasets, Samples, and Files
    available in the Data Portal.
    """

    def __init__(self, base_url: str = None, client: CirroApi = None):
        """
        Set up the DataPortal object, establishing an authenticated connection.

        Args:
            base_url (str): Optional base URL of the Cirro instance
             (if not provided, it uses the `CIRRO_BASE_URL` environment variable, or the config file)
            client (`cirro.cirro_client.CirroApi`): Optional pre-configured client

        Example:
        ```python
        from cirro import DataPortal

        portal = DataPortal(base_url="app.cirro.bio")
        portal.list_projects()
        ```
        """

        if client is not None:
            self._client = client

        # Set up default client if not provided
        else:
            self._client = CirroApi(base_url=base_url)

    def list_projects(self) -> DataPortalProjects:
        """List all the projects available in the Data Portal."""

        return DataPortalProjects(
            [
                DataPortalProject(proj, self._client)
                for proj in self._client.projects.list()
            ]
        )

    def get_project_by_name(self, name: str = None) -> DataPortalProject:
        """Return the project with the specified name."""

        return self.list_projects().get_by_name(name)

    def get_project_by_id(self, _id: str = None) -> DataPortalProject:
        """Return the project with the specified id."""

        return self.list_projects().get_by_id(_id)

    def get_project(self, project: str = None) -> DataPortalProject:
        """
        Return a project identified by ID or name.

        Args:
            project (str): ID or name of project

        Returns:
            `from cirro.sdk.project import DataPortalProject`
        """
        try:
            return self.get_project_by_id(project)
        except DataPortalAssetNotFound:
            return self.get_project_by_name(project)

    def get_dataset(self, project: str = None, dataset: str = None) -> DataPortalDataset:
        """
        Return a dataset identified by ID or name.

        Args:
            project (str): ID or name of project
            dataset (str): ID or name of dataset

        Returns:
            `cirro.sdk.dataset.DataPortalDataset`

            ```python
            from cirro import DataPortal()
            portal = DataPortal()
            dataset = portal.get_dataset(
                project="id-or-name-of-project",
                dataset="id-or-name-of-dataset"
            )
            ```
        """
        try:
            project: DataPortalProject = self.get_project_by_id(project)
        except DataPortalAssetNotFound:
            project: DataPortalProject = self.get_project_by_name(project)

        return project.get_dataset(dataset)

    def read_files(
            self,
            project: str,
            dataset: str,
            glob: str = None,
            pattern: str = None,
            filetype: str = None,
            **kwargs
    ):
        """
        Read the contents of files from a dataset.

        The project and dataset can each be identified by name or ID.
        Exactly one of ``glob`` or ``pattern`` must be provided.

        **glob** — standard wildcard matching; yields the file content for each
        matching file:

        - ``*`` matches any characters within a single path segment
        - ``**`` matches zero or more path segments
        - Matching is suffix-anchored (``*.csv`` matches at any depth)

        **pattern** — like ``glob`` but ``{name}`` placeholders capture portions
        of the path automatically; yields ``(content, meta)`` pairs where
        *meta* is a ``dict`` of extracted values:

        - ``{name}`` captures one path segment (no ``/``)
        - ``*`` and ``**`` wildcards work as in ``glob``

        Args:
            project (str): ID or name of the project.
            dataset (str): ID or name of the dataset.
            glob (str): Wildcard expression to match files
                (e.g., ``'*.csv'``, ``'data/**/*.tsv.gz'``).
                Yields one item per matching file: the parsed content.
            pattern (str): Wildcard expression with ``{name}`` capture
                placeholders (e.g., ``'{sample}.csv'``,
                ``'{condition}/{sample}.csv'``).
                Yields ``(content, meta)`` per matching file.
            filetype (str): File format used to parse each file. Supported values:

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
            **kwargs: Additional keyword arguments forwarded to the file-parsing
                function (e.g., ``sep='\\t'`` for CSV/TSV files).

        Yields:
            - When using ``glob``: *content* for each matching file
            - When using ``pattern``: ``(content, meta)`` for each matching file,
              where *meta* is a ``dict`` of values extracted from ``{name}``
              placeholders

        Raises:
            DataPortalInputError: if both ``glob`` and ``pattern`` are provided,
                or if neither is provided.

        Example:
            ```python
            # Read all CSV files — just the content
            for df in portal.read_files('My Project', 'My Dataset', glob='*.csv'):
                print(df.shape)

            # Extract sample names from filenames automatically
            for df, meta in portal.read_files('My Project', 'My Dataset', pattern='{sample}.csv'):
                print(meta['sample'], df.shape)

            # Multi-level capture: condition directory + sample filename
            for df, meta in portal.read_files('My Project', 'My Dataset', pattern='{condition}/{sample}.csv'):
                print(meta['condition'], meta['sample'], df.shape)

            # Read gzip-compressed TSV files with explicit separator
            for df in portal.read_files('My Project', 'My Dataset', glob='**/*.tsv.gz', filetype='csv', sep='\\t'):
                print(df.shape)
            ```
        """
        ds = self.get_dataset(project=project, dataset=dataset)
        yield from ds.read_files(glob=glob, pattern=pattern, filetype=filetype, **kwargs)

    def read_file(
            self,
            project: str,
            dataset: str,
            path: str = None,
            glob: str = None,
            filetype: str = None,
            **kwargs
    ):
        """
        Read the contents of a single file from a dataset.

        The project and dataset can each be identified by name or ID.
        Provide either ``path`` (exact relative path) or ``glob`` (wildcard
        expression). If ``glob`` is used it must match exactly one file.

        Args:
            project (str): ID or name of the project.
            dataset (str): ID or name of the dataset.
            path (str): Exact relative path of the file within the dataset.
            glob (str): Wildcard expression matching exactly one file.
            filetype (str): File format used to parse the file. Supported values
                are the same as :meth:`read_files`.
            **kwargs: Additional keyword arguments forwarded to the
                file-parsing function.

        Returns:
            Parsed file content.

        Raises:
            DataPortalInputError: if both or neither of ``path``/``glob`` are
                provided, or if ``glob`` matches zero or more than one file.
        """
        ds = self.get_dataset(project=project, dataset=dataset)
        return ds.read_file(path=path, glob=glob, filetype=filetype, **kwargs)

    def list_processes(self, ingest=False) -> DataPortalProcesses:
        """
        List all the processes available in the Data Portal.
        By default, only list non-ingest processes (those which can be run on existing datasets).
        To list the processes which can be used to upload datasets, use `ingest = True`.

        Args:
            ingest (bool): If True, only list those processes which can be used to ingest datasets directly
        """

        return DataPortalProcesses(
            [
                DataPortalProcess(p, self._client)
                for p in self._client.processes.list()
                if not ingest or p.executor == Executor.INGEST
            ]
        )

    def get_process_by_name(self, name: str, ingest=False) -> DataPortalProcess:
        """
        Return the process with the specified name.

        Args:
            name (str): Name of process
        """

        return self.list_processes(ingest=ingest).get_by_name(name)

    def get_process_by_id(self, id: str, ingest=False) -> DataPortalProcess:
        """
        Return the process with the specified id

        Args:
            id (str): ID of process
        """

        return self.list_processes(ingest=ingest).get_by_id(id)

    def list_reference_types(self) -> DataPortalReferenceTypes:
        """
        Return the list of all available reference types
        """

        return DataPortalReferenceTypes(
            [
                DataPortalReferenceType(ref)
                for ref in self._client.references.get_types()
            ]
        )

    @property
    def developer_helper(self) -> DeveloperHelper:
        return DeveloperHelper(self._client)
