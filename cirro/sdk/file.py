import gzip
from io import BytesIO, StringIO
from pathlib import Path
from typing import List

from typing import TYPE_CHECKING
if TYPE_CHECKING:
    import anndata
    from pandas import DataFrame

from cirro.cirro_client import CirroApi
from cirro.models.file import File, PathLike
from cirro.sdk.asset import DataPortalAssets, DataPortalAsset
from cirro.sdk.exceptions import DataPortalInputError
from cirro.utils import convert_size


class DataPortalFile(DataPortalAsset):
    """
    Datasets are made up of a collection of File objects in the Data Portal.
    """

    def __init__(self, file: File, client: CirroApi):
        """
        Instantiate by listing files from a dataset.

        ```python
        from cirro import DataPortal()
        portal = DataPortal()
        dataset = portal.get_dataset(
            project="id-or-name-of-project",
            dataset="id-or-name-of-dataset"
        )
        files = dataset.list_files()
        ```
        """
        # Attach the file object
        self._file = file
        self._client = client

    # Note that the 'name' and 'id' attributes are set to the relative path
    # The purpose of this is to support the DataPortalAssets class functions
    @property
    def id(self) -> str:
        """Relative path of file within the dataset"""
        return self._file.relative_path

    @property
    def name(self) -> str:
        """Relative path of file within the dataset"""
        return self._file.relative_path

    @property
    def file_name(self) -> str:
        """Name of file, excluding the full folder path within the dataset"""
        return self._file.name

    @property
    def relative_path(self) -> str:
        """Relative path of file within the dataset"""
        return self._file.relative_path

    @property
    def absolute_path(self) -> str:
        """Fully URI to file object in AWS S3"""
        return self._file.absolute_path

    @property
    def metadata(self) -> dict:
        """File metadata"""
        return self._file.metadata

    @property
    def size_bytes(self) -> int:
        """File size (in bytes)"""
        return self._file.size

    @property
    def size(self) -> str:
        """File size converted to human-readable (e.g., 4.50 GB)"""
        return convert_size(self._file.size)

    def __str__(self):
        return f"{self.relative_path} ({self.size})"

    def _get(self) -> bytes:
        """Internal method to call client.file.get_file"""

        return self._client.file.get_file(self._file)

    def read_csv(self, compression='infer', encoding='utf-8', **kwargs) -> 'DataFrame':
        """
        Parse the file as a Pandas DataFrame.

        The default field separator is a comma (for CSV), use sep='\\t' for TSV.

        File compression is inferred from the extension, but can be set
        explicitly with the compression= flag.

        All other keyword arguments are passed to pandas.read_csv
        https://pandas.pydata.org/docs/reference/api/pandas.read_csv.html
        """
        import pandas

        if compression == 'infer':
            # If the file appears to be compressed
            if self.relative_path.endswith('.gz'):
                compression = dict(method='gzip')
            elif self.relative_path.endswith('.bz2'):
                compression = dict(method='bz2')
            elif self.relative_path.endswith('.xz'):
                compression = dict(method='zstd')
            elif self.relative_path.endswith('.zst'):
                compression = dict(method='zstd')
            else:
                compression = None

        if compression is not None:
            handle = BytesIO(self._get())
        else:
            handle = StringIO(self._get().decode(encoding))

        df = pandas.read_csv(
            handle,
            compression=compression,
            encoding=encoding,
            **kwargs
        )
        handle.close()
        return df

    def read_h5ad(self) -> 'anndata.AnnData':
        """Read an AnnData object from a file."""
        # Import the anndata library, and raise an error if it is not available
        try:
            import anndata as ad # noqa
        except ImportError:
            raise ImportError("The anndata library is required to read AnnData files. "
                              "Please install it using 'pip install anndata'.")

        # Download the file to a temporary file handle and parse the contents
        with BytesIO(self._get()) as handle:
            return ad.read_h5ad(handle)

    def readlines(self, encoding='utf-8', compression=None) -> List[str]:
        """Read the file contents as a list of lines."""

        return self.read(
            encoding=encoding,
            compression=compression
        ).splitlines()

    def read(self, encoding='utf-8', compression=None) -> str:
        """Read the file contents as text."""

        # Get the raw file contents
        cont = self._get()

        # If the file is uncompressed
        if compression is None:
            return cont.decode(encoding)
        # If the file is compressed
        else:

            # Only gzip-compression is supported currently
            if compression != "gzip":
                raise DataPortalInputError("compression may be 'gzip' or None")

            with gzip.open(
                BytesIO(
                    cont
                ),
                'rt',
                encoding=encoding
            ) as handle:
                return handle.read()

    def read_bytes(self) -> BytesIO:
        """Get a generic BytesIO object representing the Data Portal File, to be passed into readers."""
        return BytesIO(self._get())

    def download(self, download_location: str = None) -> Path:
        """
        Download the file to a local directory.

        Returns:
            Path to download file
        """

        if download_location is None:
            raise DataPortalInputError("Must provide download location")

        return self._client.file.download_files(
            self._file.access_context,
            download_location,
            [self.relative_path]
        )[0]

    def validate(self, local_path: PathLike):
        """
        Validate that the local file matches the remote file by comparing checksums.

        Args:
            local_path (PathLike): Path to the local file to validate
        Raises:
            ValueError: If checksums do not match
            RuntimeWarning: If the remote checksum is not available or not supported
        """
        self._client.file.validate_file(self._file, local_path)

    def is_valid(self, local_path: PathLike) -> bool:
        """
        Check if the local file matches the remote file by comparing checksums.

        Args:
            local_path (PathLike): Path to the local file to validate
        Returns:
            bool: True if the local file matches the remote file, False otherwise
        Raises:
            RuntimeWarning: If the remote checksum is not available or not supported
        """
        if not local_path:
            raise DataPortalInputError("Must provide local path to validate file")

        return self._client.file.is_valid_file(self._file, local_path)


class DataPortalFiles(DataPortalAssets[DataPortalFile]):
    """Collection of DataPortalFile objects."""

    asset_name = "file"

    def download(self, download_location: str = None) -> List[Path]:
        """
        Download the collection of files to a local directory.

        Returns:
            List of paths to downloaded files.
        """

        local_paths = []
        for f in self:
            local_paths += f.download(download_location)
        return local_paths
