import gzip
import json
import pickle
from abc import ABC, abstractmethod
from io import BytesIO, StringIO
from typing import List, TYPE_CHECKING

if TYPE_CHECKING:
    import anndata
    from pandas import DataFrame

from cirro.sdk.exceptions import DataPortalInputError


class FileReadMixin(ABC):
    """
    Mixin that adds file-reading methods to any class that provides
    ``_get() -> bytes`` and a ``name`` property.
    """

    @property
    @abstractmethod
    def name(self) -> str:
        """Filename."""

    @abstractmethod
    def _get(self) -> bytes:
        """Return the raw file bytes."""

    def read(self, encoding='utf-8', compression=None) -> str:
        """
        Read the file contents as text.

        Args:
            encoding (str): Text encoding to decode with.
            compression (str): Pass `'gzip'` to decompress first, or `None` to
                read the bytes as-is. Unlike `read_csv`, this is not inferred
                from the file extension.

        Raises:
            DataPortalInputError: if `compression` is anything other than
                `'gzip'` or `None`.
        """
        cont = self._get()
        if compression is None:
            return cont.decode(encoding)
        if compression != 'gzip':
            raise DataPortalInputError("compression may be 'gzip' or None")
        with gzip.open(BytesIO(cont), 'rt', encoding=encoding) as handle:
            return handle.read()

    def readlines(self, encoding='utf-8', compression=None) -> List[str]:
        """
        Read the file contents as a list of lines, without trailing newlines.

        Args:
            encoding (str): Text encoding to decode with.
            compression (str): Pass `'gzip'` to decompress first, or `None` to
                read the bytes as-is.
        """
        return self.read(encoding=encoding, compression=compression).splitlines()

    def read_bytes(self) -> BytesIO:
        """
        Get a BytesIO object for the file contents, to pass into arbitrary readers.

        Use this for formats the mixin does not cover -- the whole file is read
        into memory and handed over as a file-like object.
        """
        return BytesIO(self._get())

    def read_csv(self, compression='infer', encoding='utf-8', **kwargs) -> 'DataFrame':
        """
        Parse the file as a Pandas DataFrame.

        Args:
            compression (str | dict): How the file is compressed. The default,
                `'infer'`, picks gzip/bz2/xz/zstd from a `.gz`, `.bz2`, `.xz`,
                or `.zst` extension, and no compression otherwise. Pass `None`
                to force reading as plain text.
            encoding (str): Text encoding to decode with.
            **kwargs: Passed through to
                [pandas.read_csv](https://pandas.pydata.org/docs/reference/api/pandas.read_csv.html).
                The field separator defaults to a comma, so pass `sep='\\t'`
                for TSV files.

        Returns:
            `pandas.DataFrame`
        """
        import pandas

        if compression == 'infer':
            if self.name.endswith('.gz'):
                compression = {'method': 'gzip'}
            elif self.name.endswith('.bz2'):
                compression = {'method': 'bz2'}
            elif self.name.endswith('.xz'):
                compression = {'method': 'xz'}
            elif self.name.endswith('.zst'):
                compression = {'method': 'zstd'}
            else:
                compression = None

        if compression is not None:
            handle = BytesIO(self._get())
            try:
                return pandas.read_csv(handle, compression=compression, encoding=encoding, **kwargs)
            finally:
                handle.close()
        else:
            handle = StringIO(self._get().decode(encoding))
            try:
                return pandas.read_csv(handle, **kwargs)
            finally:
                handle.close()

    def read_json(self, **kwargs):
        """Read the file contents as a parsed JSON object (dict, list, etc.)."""
        return json.loads(self._get(), **kwargs)

    def read_h5ad(self) -> 'anndata.AnnData':
        """Read an AnnData object from a file."""
        try:
            import anndata as ad  # noqa
        except ImportError:
            raise ImportError("The anndata library is required to read AnnData files. "
                              "Please install it using 'pip install anndata'.")
        with BytesIO(self._get()) as handle:
            return ad.read_h5ad(handle)

    def read_parquet(self, **kwargs) -> 'DataFrame':
        """
        Read a Parquet file as a Pandas DataFrame.

        Requires ``pyarrow`` or ``fastparquet`` to be installed.
        All keyword arguments are passed to :func:`pandas.read_parquet`.
        """
        import pandas
        return pandas.read_parquet(BytesIO(self._get()), **kwargs)

    def read_feather(self, **kwargs) -> 'DataFrame':
        """
        Read a Feather file as a Pandas DataFrame.

        Requires ``pyarrow`` to be installed.
        All keyword arguments are passed to :func:`pandas.read_feather`.
        """
        import pandas
        return pandas.read_feather(BytesIO(self._get()), **kwargs)

    def read_pickle(self, **kwargs):
        """Read the file contents as a Python pickle object."""
        return pickle.loads(self._get(), **kwargs)

    def read_excel(self, **kwargs) -> 'DataFrame':
        """
        Read an Excel file (``.xlsx`` / ``.xls``) as a Pandas DataFrame.

        Requires ``openpyxl`` (for ``.xlsx``) or ``xlrd`` (for ``.xls``).
        All keyword arguments are passed to :func:`pandas.read_excel`.
        """
        import pandas
        return pandas.read_excel(BytesIO(self._get()), **kwargs)
