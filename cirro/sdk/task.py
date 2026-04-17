import csv
from functools import cached_property
import gzip
import json
from io import BytesIO, StringIO
from pathlib import PurePath
import re
from typing import Any, List, Optional, TYPE_CHECKING

from cirro.models.file import FileAccessContext
from cirro.models.s3_path import S3Path
from cirro.sdk.exceptions import DataPortalAssetNotFound
from cirro.sdk.nextflow_utils import parse_inputs_from_command_run

if TYPE_CHECKING:
    from cirro.cirro_client import CirroApi
    from pandas import DataFrame


class WorkDirFile:
    """
    A file that lives in a Nextflow work directory or a dataset staging area.

    Each WorkDirFile either originated from another task's work directory
    (``source_task`` is set) or was a primary/staged input to the workflow
    (``source_task`` is ``None``).
    """

    def __init__(
        self,
        s3_uri: str,
        client: 'CirroApi',
        project_id: str,
        size: Optional[int] = None,
        source_task: Optional['DataPortalTask'] = None,
        dataset_id: str = ''
    ):
        """
        Obtained from a task's ``inputs`` or ``outputs`` property.

        ```python
        for task in dataset.tasks:
            for f in task.inputs:
                print(f.name, f.source_task)
        ```
        """
        self._s3_uri = s3_uri
        self._client = client
        self._project_id = project_id
        self._dataset_id = dataset_id
        self._size = size
        self._source_task = source_task
        self._s3_path = S3Path(s3_uri)

    @property
    def source_task(self) -> Optional['DataPortalTask']:
        """The task that produced this file, or ``None`` for staged/primary inputs."""
        return self._source_task

    @property
    def name(self) -> str:
        """Filename (last component of the S3 URI)."""
        return PurePath(self._s3_uri).name

    @property
    def size(self) -> int:
        """File size in bytes (fetched lazily via head_object if not pre-populated)."""
        if self._size is None:
            try:
                s3 = self._get_s3_client()
                resp = s3.head_object(Bucket=self._s3_path.bucket, Key=self._s3_path.key)
                self._size = resp['ContentLength']
            except Exception as e:
                raise DataPortalAssetNotFound(
                    f"Could not determine size of {self.name!r} — "
                    f"the work directory may have been cleaned up: {e}"
                ) from e
        return self._size

    def _access_context(self) -> FileAccessContext:
        """Return the appropriate FileAccessContext for this file's location."""
        if self._dataset_id:
            return FileAccessContext.scratch_download(
                project_id=self._project_id,
                dataset_id=self._dataset_id,
                base_url=self._s3_path.base
            )
        return FileAccessContext.download(
            project_id=self._project_id,
            base_url=self._s3_path.base
        )

    def _get(self) -> bytes:
        """Return the raw bytes of the file."""
        try:
            return self._client.file.get_file_from_path(self._access_context(), self._s3_path.key)
        except Exception as e:
            raise DataPortalAssetNotFound(
                f"Could not read {self.name!r} — "
                f"the work directory may have been cleaned up: {e}"
            ) from e

    def read(self, encoding: str = 'utf-8', compression: Optional[str] = None) -> str:
        """
        Read the file contents as text.

        Args:
            encoding (str): Character encoding (default 'utf-8').
            compression (str): ``'gzip'`` to decompress on the fly, or ``None``
                (default) to read as-is.
        """
        raw = self._get()
        if compression is None:
            return raw.decode(encoding, errors='replace')
        if compression == 'gzip':
            with gzip.open(BytesIO(raw), 'rt', encoding=encoding) as fh:
                return fh.read()
        raise ValueError(f"Unsupported compression: {compression!r} (use 'gzip' or None)")

    def readlines(self, encoding: str = 'utf-8', compression: Optional[str] = None) -> List[str]:
        """Read the file contents as a list of lines."""
        return self.read(encoding=encoding, compression=compression).splitlines()

    def read_json(self, encoding: str = 'utf-8') -> Any:
        """
        Parse the file as JSON.

        Returns whatever the top-level JSON value is (dict, list, etc.).
        """
        try:
            return json.loads(self.read(encoding=encoding))
        except json.JSONDecodeError as e:
            raise ValueError(f"Could not parse {self.name!r} as JSON: {e}") from e

    def read_csv(self, compression: str = 'infer', encoding: str = 'utf-8',
                 **kwargs) -> 'DataFrame':
        """
        Parse the file as a Pandas DataFrame.

        The default separator is a comma; pass ``sep='\\t'`` for TSV files.
        Compression is inferred from the file extension by default, but can be
        overridden with ``compression='gzip'`` or ``compression=None``.

        All additional keyword arguments are forwarded to
        ``pandas.read_csv``.
        """
        try:
            import pandas
        except ImportError:
            raise ImportError(
                "pandas is required to read CSV files. "
                "Install it with: pip install pandas"
            )

        if compression == 'infer':
            name = self.name
            if name.endswith('.gz'):
                compression = dict(method='gzip')
            elif name.endswith('.bz2'):
                compression = dict(method='bz2')
            elif name.endswith('.zst'):
                compression = dict(method='zstd')
            else:
                compression = None

        raw = self._get()
        handle = BytesIO(raw) if compression is not None else StringIO(raw.decode(encoding))
        try:
            return pandas.read_csv(handle, compression=compression, encoding=encoding, **kwargs)
        finally:
            handle.close()

    def _get_s3_client(self):
        return self._client.file.get_aws_s3_client(self._access_context())

    def __str__(self):
        return self.name

    def __repr__(self):
        return f'WorkDirFile(name={self.name!r})'


class DataPortalTask:
    """
    Represents a single task from a Nextflow workflow execution.

    Task metadata (name, status, exit code, work directory, etc.) is read
    from the workflow trace artifact.  Log contents and input/output files are
    fetched from the task's S3 work directory on demand.
    """

    def __init__(
        self,
        trace_row: dict,
        client: 'CirroApi',
        project_id: str,
        dataset_id: str = '',
        all_tasks_ref: Optional[list] = None
    ):
        """
        Obtained from a dataset's ``tasks`` property.

        ```python
        for task in dataset.tasks:
            print(task.name, task.status)
            print(task.logs())
        ```

        Args:
            trace_row (dict): A row from the Nextflow trace TSV, parsed as a dict.
            client (CirroApi): Authenticated CirroApi client.
            project_id (str): ID of the project that owns this dataset.
            dataset_id (str): ID of the dataset (execution) that owns this task.
            all_tasks_ref (list): A shared list that will contain all tasks once they
                are all built.  Used by ``inputs`` to resolve ``source_task``.
        """
        self._trace = trace_row
        self._client = client
        self._project_id = project_id
        self._dataset_id = dataset_id
        self._all_tasks_ref: list = all_tasks_ref if all_tasks_ref is not None else []
        self._inputs: Optional[List[WorkDirFile]] = None
        self._outputs: Optional[List[WorkDirFile]] = None

    # ------------------------------------------------------------------ #
    # Trace-derived properties                                             #
    # ------------------------------------------------------------------ #

    @property
    def task_id(self) -> int:
        """Sequential task identifier from the trace."""
        try:
            return int(self._trace.get('task_id', 0))
        except (ValueError, TypeError):
            return 0

    @property
    def name(self) -> str:
        """Full task name, e.g. ``NFCORE_RNASEQ:RNASEQ:TRIMGALORE (sample1)``."""
        return self._trace.get('name', '')

    @property
    def status(self) -> str:
        """Task status string from the trace, e.g. ``COMPLETED``, ``FAILED``, ``ABORTED``."""
        return self._trace.get('status', '')

    @property
    def hash(self) -> str:
        """Short hash prefix used by Nextflow, e.g. ``99/b42c07``."""
        return self._trace.get('hash', '')

    @property
    def work_dir(self) -> str:
        """Full S3 URI of the task's work directory."""
        return self._trace.get('workdir', '')

    @property
    def native_id(self) -> str:
        """Native job ID on the underlying executor (e.g. AWS Batch job ID)."""
        return self._trace.get('native_id', '')

    @property
    def exit_code(self) -> Optional[int]:
        """Process exit code, or ``None`` if the task did not reach completion."""
        val = self._trace.get('exit', '')
        if val in ('', None, '-'):
            return None
        try:
            return int(val)
        except (ValueError, TypeError):
            return None

    # ------------------------------------------------------------------ #
    # Work-directory file access                                           #
    # ------------------------------------------------------------------ #

    def _get_access_context(self) -> FileAccessContext:
        if not self.work_dir:
            raise DataPortalAssetNotFound(
                f"Task {self.name!r} has no work directory recorded in the trace"
            )
        s3_path = S3Path(self.work_dir)
        if self._dataset_id:
            return FileAccessContext.scratch_download(
                project_id=self._project_id,
                dataset_id=self._dataset_id,
                base_url=s3_path.base
            )
        return FileAccessContext.download(
            project_id=self._project_id,
            base_url=s3_path.base
        )

    def _read_work_file(self, filename: str) -> str:
        """
        Read a file from the task's work directory.

        Returns an empty string if the work directory has been cleaned up or
        the file does not exist.
        """
        if not self.work_dir:
            return ''
        try:
            s3_path = S3Path(self.work_dir)
            key = f'{s3_path.key}/{filename}'
            access_context = self._get_access_context()
            return self._client.file.get_file_from_path(
                access_context, key
            ).decode('utf-8', errors='replace')
        except Exception:
            return ''

    @cached_property
    def logs(self) -> str:
        """
        Return the task log (combined stdout/stderr of the task process).

        Fetches via the Cirro execution API when a native job ID is available,
        which works even when the S3 scratch bucket is not directly accessible.
        Falls back to reading ``.command.log`` from the S3 work directory.
        Returns an empty string if neither source can be read.
        """
        if self._dataset_id and self.native_id:
            try:
                return self._client.execution.get_task_logs(
                    project_id=self._project_id,
                    dataset_id=self._dataset_id,
                    task_id=self.native_id
                )
            except Exception:
                pass
        return self._read_work_file('.command.log')

    def script(self) -> str:
        """
        Return the contents of ``.command.sh`` from the task's work directory.

        This is the actual shell script that Nextflow executed — the user's
        pipeline code for this task.  Falls back to parsing the script from the
        ``WORKFLOW_LOGS`` artifact when the work directory is not accessible
        (scratch bucket requires elevated permissions).
        Returns an empty string if the script cannot be obtained.
        """
        content = self._read_work_file('.command.sh')
        if content:
            return content
        return self._script_from_workflow_log()

    def _script_from_workflow_log(self) -> str:
        """
        Parse this task's shell script from the WORKFLOW_LOGS artifact.

        When a Nextflow task fails the head-node log includes a block:

            Error executing process > 'TASK_NAME'
            ...
            Command executed:
              <script lines>
            Command exit status:

        This method extracts that block and returns the dedented script.
        Returns an empty string when the artifact is absent or the task name
        does not appear in the log.
        """
        if not self._dataset_id:
            return ''
        try:
            from cirro_api_client.v1.models import ArtifactType

            assets = self._client.datasets.get_assets_listing(
                project_id=self._project_id,
                dataset_id=self._dataset_id
            )
            log_artifact = next(
                (a for a in assets.artifacts if a.artifact_type == ArtifactType.WORKFLOW_LOGS),
                None
            )
            if log_artifact is None:
                return ''

            log_text = self._client.file.get_file(log_artifact.file).decode(
                'utf-8', errors='replace'
            )

            # Nextflow error block format:
            #   Error executing process > 'TASK_NAME'
            #   ...blank / metadata lines...
            #   Command executed:
            #   <indented script>
            #   Command exit status:
            pattern = (
                r"Error executing process > '"
                + re.escape(self.name)
                + r"'[\s\S]*?Command executed:\n([\s\S]*?)Command exit status:"
            )
            m = re.search(pattern, log_text)
            if not m:
                return ''

            lines = m.group(1).splitlines()
            non_empty = [ln for ln in lines if ln.strip()]
            if not non_empty:
                return ''
            # Strip the common leading indent
            min_indent = min(len(ln) - len(ln.lstrip()) for ln in non_empty)
            return '\n'.join(ln[min_indent:] for ln in lines).strip()
        except Exception:
            return ''

    # ------------------------------------------------------------------ #
    # Inputs                                                               #
    # ------------------------------------------------------------------ #

    @property
    def inputs(self) -> List[WorkDirFile]:
        """
        List of input files for this task.

        Parsed from ``.command.run`` (the Nextflow staging script).  Each file
        is annotated with ``source_task`` if it was produced by another task in
        the same workflow.
        """
        if self._inputs is None:
            self._inputs = self._build_inputs()
        return self._inputs

    def _build_inputs(self) -> List[WorkDirFile]:
        """Parse input URIs from ``.command.run`` and link each to its source task."""
        content = self._read_work_file('.command.run')
        if content:
            uris = parse_inputs_from_command_run(content)
            result = []
            for uri in uris:
                source_task = None
                for other_task in self._all_tasks_ref:
                    if other_task is not self and other_task.work_dir and uri.startswith(
                        other_task.work_dir.rstrip('/') + '/'
                    ):
                        source_task = other_task
                        break
                result.append(WorkDirFile(
                    s3_uri=uri,
                    client=self._client,
                    project_id=self._project_id,
                    source_task=source_task,
                    dataset_id=self._dataset_id
                ))
            return result

        # Fallback: try to identify staged inputs from the workflow's FILES artifact.
        # This is used when the scratch bucket is not directly accessible.
        return self._build_inputs_from_files_artifact()

    def _build_inputs_from_files_artifact(self) -> List[WorkDirFile]:
        """
        Fallback: identify input files from the workflow's FILES artifact.

        Used when the task work directory (scratch bucket) is not accessible.
        Matches staged input files based on the identifier embedded in the task name,
        e.g. ``BWA_INDEX (genome.fasta)`` → looks for a file named ``genome.fasta``.
        """
        if not self._dataset_id:
            return []
        try:
            from cirro_api_client.v1.models import ArtifactType

            assets = self._client.datasets.get_assets_listing(
                project_id=self._project_id,
                dataset_id=self._dataset_id
            )

            files_artifact = next(
                (a for a in assets.artifacts if a.artifact_type == ArtifactType.FILES),
                None
            )
            if files_artifact is None:
                return []

            content = self._client.file.get_file(files_artifact.file).decode('utf-8')

            # Extract the identifier from the task name, e.g. "BWA_INDEX (genome.fasta)" → "genome.fasta"
            match = re.search(r'\((.+?)\)', self.name)
            if not match:
                return []
            identifier = match.group(1)

            result = []
            reader = csv.DictReader(StringIO(content))
            for row in reader:
                file_uri = row.get('file', '')
                sample = row.get('sample', '')
                if not file_uri:
                    continue
                file_basename = PurePath(file_uri).name
                file_stem = PurePath(file_basename).stem
                if identifier in (file_basename, file_stem, sample):
                    result.append(WorkDirFile(
                        s3_uri=file_uri,
                        client=self._client,
                        project_id=self._project_id,
                    ))
            return result
        except Exception:
            return []

    # ------------------------------------------------------------------ #
    # Outputs                                                              #
    # ------------------------------------------------------------------ #

    @property
    def outputs(self) -> List[WorkDirFile]:
        """
        List of non-hidden output files in the task's work directory.

        Returns an empty list if the directory has been cleaned up or cannot
        be listed.
        """
        if self._outputs is None:
            self._outputs = self._build_outputs()
        return self._outputs

    def _build_outputs(self) -> List[WorkDirFile]:
        """List non-hidden files directly under the task's S3 work directory."""
        if not self.work_dir:
            return []
        try:
            s3_path = S3Path(self.work_dir)
            access_context = self._get_access_context()
            s3 = self._client.file.get_aws_s3_client(access_context)

            prefix = s3_path.key.rstrip('/') + '/'
            result = []

            paginator = s3.get_paginator('list_objects_v2')
            for page in paginator.paginate(Bucket=s3_path.bucket, Prefix=prefix):
                for obj in page.get('Contents', []):
                    key = obj['Key']
                    remainder = key[len(prefix):]
                    # Skip subdirectory contents and hidden files
                    if '/' in remainder or remainder.startswith('.'):
                        continue
                    full_uri = f's3://{s3_path.bucket}/{key}'
                    result.append(WorkDirFile(
                        s3_uri=full_uri,
                        client=self._client,
                        project_id=self._project_id,
                        size=obj['Size'],
                        dataset_id=self._dataset_id
                    ))
            return result
        except Exception:
            return []

    # ------------------------------------------------------------------ #
    # Repr                                                                 #
    # ------------------------------------------------------------------ #

    def __str__(self):
        return f'Task(name={self.name}, status={self.status})'

    def __repr__(self):
        return f'DataPortalTask(name={self.name!r}, status={self.status!r})'
