import json
import logging
import os
import warnings
from io import StringIO
from pathlib import Path
from typing import TYPE_CHECKING, Union, Optional

import boto3

if TYPE_CHECKING:
    from pandas import DataFrame

from cirro.models.s3_path import S3Path

logger = logging.getLogger(__name__)


def write_json(dat, local_path: str, indent=4):
    """Write a JSON object to a local file."""
    with Path(local_path).open(mode="wt") as handle:
        return json.dump(dat, handle, indent=indent)


def read_csv(path: str, required_columns=None) -> 'DataFrame':
    """Read a CSV from the dataset and check for any required columns."""
    if required_columns is None:
        required_columns = []

    import pandas as pd
    df = pd.read_csv(path)
    for col in required_columns:
        assert col in df.columns.values, f"Did not find expected columns {col} in {path}"
    return df


def read_json(path: str):
    """Read a JSON object from a local file or S3 path."""
    s3_path = S3Path(path)

    if s3_path.valid:
        s3 = boto3.client('s3')
        retr = s3.get_object(Bucket=s3_path.bucket, Key=s3_path.key)
        text = retr['Body'].read().decode()
    else:
        with Path(path).open() as handle:
            text = handle.read()

    # Parse JSON
    return json.loads(text)


class PreprocessDataset:
    """
    Helper functions for performing preparatory tasks immediately before launching
    the analysis workflow for a dataset.

    For use in the `preprocess.py` script.
    More info: https://docs.cirro.bio/pipelines/preprocess-script/
    """
    samplesheet: 'DataFrame'
    """
    A pandas DataFrame containing all of the metadata assigned to the samples present
    in the input datasets (at the time of analysis).

    More info: https://docs.cirro.bio/pipelines/preprocess-script/#dssamplesheet
    """
    files: 'DataFrame'
    """
    A DataFrame containing information on the files contained in the input datasets,
    and the sample that each file is assigned to.

    More info: https://docs.cirro.bio/pipelines/preprocess-script/#dsfiles
    """
    params: dict
    """
    A dictionary with all of the parameter values populated by user input
    using the process-form.json and process-input.json configurations.

    This is read-only, use `add_param` to add new parameters or `remove_param` to remove them.

    More info: https://docs.cirro.bio/pipelines/preprocess-script/#dsparams
    """
    metadata: dict
    """
    Detailed information about the dataset at the time of analysis,
    including the project, process, and input datasets.

    More info: https://docs.cirro.bio/pipelines/preprocess-script/#dsmetadata
    """
    dataset_root: str
    """
    Base path to the dataset
    """

    _PARAMS_FILE = "params.json"
    _REFERENCES_BASE = "s3://pubweb-references"

    def __init__(self,
                 samplesheet: Union['DataFrame', str, Path],
                 files: Union['DataFrame', str, Path],
                 params: dict = None,
                 metadata: dict = None,
                 dataset_root: str = None):
        import pandas as pd
        # Convert DataFrame to string if necessary
        if isinstance(samplesheet, str):
            samplesheet = pd.read_csv(StringIO(samplesheet))
        if isinstance(samplesheet, Path):
            samplesheet = read_csv(str(samplesheet))
        if isinstance(files, str):
            files = pd.read_csv(StringIO(files))
        if isinstance(files, Path):
            files = read_csv(str(files))
        if params is None:
            params = {}
        if metadata is None:
            metadata = {}

        self.samplesheet = samplesheet
        self.files = files
        self.params = params
        self.metadata = metadata
        self.dataset_root = dataset_root

    @classmethod
    def from_path(cls, dataset_root: str, config_directory='config'):
        """
        Creates an instance from a path
        (useful for testing or when running the script outside Cirro)
        """
        config_directory = Path(dataset_root, config_directory)

        files = read_csv(
            str(Path(config_directory, "files.csv")),
            required_columns=["sample", "file"]
        )

        samplesheet = read_csv(
            str(Path(config_directory, "samplesheet.csv")),
            required_columns=["sample"]
        )

        params = read_json(
            str(Path(config_directory, "params.json")),
        )

        metadata = read_json(
            str(Path(config_directory, "metadata.json")),
        )

        return cls(files=files,
                   samplesheet=samplesheet,
                   params=params,
                   metadata=metadata,
                   dataset_root=dataset_root)

    @classmethod
    def from_running(cls):
        """
        Creates an instance from the currently running dataset
        (expected to be called from inside a Cirro analysis process)
        """
        logging.basicConfig(level=logging.INFO,
                            format='%(asctime)s %(levelname)-8s [PreprocessDataset] %(message)s')
        dataset_path = os.getenv("PW_S3_DATASET")
        return cls.from_path(dataset_path)

    @property
    def references_base(self):
        """
        Returns the base URL for references.
        This is used to access public references in the Cirro system.
        """
        return self._REFERENCES_BASE

    def log(self):
        """Print logging messages about the dataset."""
        logger.info(f"Storage location for dataset: {self.dataset_root}")
        logger.info(f"Number of files in dataset: {self.files.shape[0]:,}")
        logger.info(f"Number of samples in dataset: {self.samplesheet.shape[0]:,}")

    def add_param(self, name: str, value, overwrite=False, log=True):
        """Add a parameter to the dataset."""

        assert overwrite or name not in self.params, \
            f"Cannot add parameter {name}, already exists (and overwrite=False)"

        if log:
            logger.info(f"Adding parameter {name} = {value}")
        self.params[name] = value

        if log:
            logger.info("Saving parameters")
        write_json(self.params, self._PARAMS_FILE)

    def remove_param(self, name: str, force=False):
        """Remove a parameter from the dataset."""

        assert force or name in self.params, \
            f"Cannot remove parameter {name}, does not exist (and force=False)"

        logger.info(f"Removing parameter {name}")
        if name in self.params:
            del self.params[name]

        logger.info("Saving parameters")
        write_json(self.params, self._PARAMS_FILE)

    def keep_params(self, params_to_keep: list[str]):
        """Keep only the specified parameters in the dataset."""
        logger.info(f"Keeping parameters: {params_to_keep}")
        self.params = {
            k: v for k, v in self.params.items()
            if k in params_to_keep
        }
        write_json(self.params, self._PARAMS_FILE)

    def update_compute(self, from_str: str, to_str: str, fp="nextflow-override.config"):
        """Replace all instances of a text string in the compute config file."""

        assert os.path.exists(fp), f"File does not exist: {fp}"
        with open(fp, 'r') as handle:
            compute = handle.read()
        n = len(compute.split(from_str)) - 1
        logger.info(f"Replacing {n:,} instances of {from_str} with {to_str} in {fp}")
        compute = compute.replace(from_str, to_str)
        with open(fp, 'wt') as handle:
            handle.write(compute)

    def pivot_samplesheet(
            self,
            index=None,
            pivot_columns: Union[Optional[str], list[str]] = 'read',
            metadata_columns: list[str] = None,
            column_prefix: str = "fastq_",
            file_filter_predicate: str = None
    ):
        """
        Combines data from both the samples and files table into a wide format with
        each sample on a row and each file in a column.
        The file column indexes are created by default from the `read` column, but can be customized.

        For example, if the `files` table has columns `sample`, `read`, and `file`,
        and the `samplesheet` has columns `sample`, `status`, and `group`, the output
        will have columns `sample`, `fastq_1`, `fastq_2`, `status`, and `group`.

        Args:
            index: List[str], used to make the frames new index, defaults to
             `["sampleIndex", "sample", "lane"]`
            pivot_columns: str or List[str], columns to pivot on and create the new column,
             defaults to 'read'. This effectively makes the column `<column_prefix><read>'.
             If the column is not defined or not present, the pivot column will be generated
             from the file number index.
            metadata_columns: List[str], metadata columns to include in the output,
             defaults to all columns that are available from the sample metadata.
             If your pipeline doesn't like extra columns, make sure to specify the allowed columns here.
            column_prefix: str, optional, prefix for the new columns, defaults to `fastq_`.
            file_filter_predicate: str, optional, a pandas query string to filter the files table.
             A common use case would be to filter out indexed reads, e.g. `readType == "R"`.

        Returns:
            DataFrame: A wide-format sample sheet with the specified columns pivoted.
        """
        import pandas as pd

        pivoted_files = self.pivot_files(index=index,
                                         pivot_columns=pivot_columns,
                                         column_prefix=column_prefix,
                                         file_filter_predicate=file_filter_predicate)
        combined = pd.merge(pivoted_files, self.samplesheet, on='sample', how="inner", validate="many_to_many")

        # Default to keeping all columns
        if metadata_columns is None:
            metadata_columns = self.samplesheet.columns.tolist() + pivoted_files.columns.tolist()

        # Keep only the specified metadata columns
        all_columns = combined.axes[1]
        for column in all_columns:
            if (column not in metadata_columns
                    # These columns are required, never drop them
                    and column_prefix not in column
                    and 'sample' != column):
                combined = combined.drop(columns=[column])

        return combined

    def pivot_files(
            self,
            index: list[str] = None,
            pivot_columns: Union[str, list[str]] = 'read',
            column_prefix: str = "fastq_",
            file_filter_predicate: str = None
    ):
        """
        Format the files table into a wide format with each sample on a row
        and each file in a column. The column indexes are created by default
        from the `read` column, but can be customized. This is useful for
        paired-end sequencing data where you want to have the columns
        `sample`, `fastq_1`, and `fastq_2` as the output.

        Args:
            index: List[str], used to make the frames new index, defaults to
            pivot_columns: str or List[str], columns to pivot on and create the new column,
             defaults to 'read'. This effectively makes the column `<column_prefix><read>`
            column_prefix: str, optional, prefix for the new columns, defaults to `fastq_`.
            file_filter_predicate: str, optional, a pandas query string to filter the files table.

        Returns:
            DataFrame: A wide-format sample sheet with the specified columns pivoted.
        """
        if index is None:
            index = ["sampleIndex", "sample", "lane"]
        logger.info("Formatting a wide files table")
        logger.info("File table (long)")
        logger.info(self.files.head().to_csv(index=False))

        files = self.files

        if file_filter_predicate is not None:
            # Filter the files table based on the predicate
            files = files.query(file_filter_predicate)

        # If we don't have access to the column defined, just use the file number
        # By default this is 'read' but the data might not be paired
        pivot_columns_defined = pivot_columns is not None and len(pivot_columns) > 0
        if not pivot_columns_defined or pivot_columns not in files.columns.values:
            logger.warning("Pivot column not found, grouping by sample instead.")
            files['file_num'] = files.groupby('sample').cumcount() + 1
            pivot_columns = 'file_num'

        if isinstance(pivot_columns, str):
            pivot_columns = [pivot_columns]

        assert pivot_columns in files.columns.values, f"Column '{pivot_columns}' not found in file table"
        assert 'file' in files.columns.values, "Column 'file' must be present in the file table"
        assert isinstance(index, list), f"index must be a list (not {type(index)})"

        # Get the list of columns from the inputs
        input_columns = files.columns.values

        # Format as a wide dataset
        # Note that all the columns in `index` will be added if they are not already present
        wide_df = files.reindex(
            columns=index + pivot_columns + ['file']
        )
        wide_df = wide_df.pivot(
            index=index,
            columns=pivot_columns,
            values='file'
        )
        # Rename the columns to have a prefix, e.g. 'fastq_'
        wide_df = wide_df.rename(
            columns=lambda i: f"{column_prefix}{int(i)}"
        )
        wide_df = wide_df.reset_index()

        # Remove any columns from the output which were added from `index`
        for cname in index:
            if cname not in input_columns:
                wide_df = wide_df.drop(columns=[cname])
        # Remove any extra unnecessary columns
        wide_df = wide_df.drop(columns=pivot_columns, errors='ignore')
        return wide_df

    def wide_samplesheet(
            self,
            index=None,
            columns='read',
            values="file",  # noqa
            column_prefix="fastq_"
    ):
        """
        Format the samplesheet into a wide format with each sample on a row

        This is a legacy method, please use `pivot_samplesheet` instead.
        """
        warnings.warn("`wide_samplesheet` is deprecated, use `pivot_samplesheet` instead.",
                      DeprecationWarning, stacklevel=2)
        if values != "file":
            raise ValueError("The only supported value for `values` is 'file'")
        return self.pivot_files(index=index, pivot_columns=[columns], column_prefix=column_prefix)
