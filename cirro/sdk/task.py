from pathlib import PurePath
from typing import List, Optional, TYPE_CHECKING

from cirro.models.file import FileAccessContext
from cirro.models.s3_path import S3Path
from cirro.sdk.nextflow_utils import parse_inputs_from_command_run

if TYPE_CHECKING:
    from cirro.cirro_client import CirroApi


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
        source_task: Optional['DataPortalTask'] = None
    ):
        self._s3_uri = s3_uri
        self._client = client
        self._project_id = project_id
        self._size = size
        self.source_task = source_task
        self._s3_path = S3Path(s3_uri)

    @property
    def name(self) -> str:
        """Filename (last component of the S3 URI)."""
        return PurePath(self._s3_uri).name

    @property
    def size(self) -> int:
        """File size in bytes (fetched lazily via head_object if not pre-populated)."""
        if self._size is None:
            s3 = self._get_s3_client()
            resp = s3.head_object(Bucket=self._s3_path.bucket, Key=self._s3_path.key)
            self._size = resp['ContentLength']
        return self._size

    def read(self) -> str:
        """Read the file contents as a UTF-8 string."""
        access_context = FileAccessContext.download(
            project_id=self._project_id,
            base_url=self._s3_path.base
        )
        return self._client.file.get_file_from_path(
            access_context, self._s3_path.key
        ).decode('utf-8', errors='replace')

    def _get_s3_client(self):
        access_context = FileAccessContext.download(
            project_id=self._project_id,
            base_url=self._s3_path.base
        )
        return self._client.file.get_aws_s3_client(access_context)

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
        all_tasks_ref: Optional[list] = None
    ):
        """
        Args:
            trace_row: A row from the Nextflow trace TSV, parsed as a dict.
            client: Authenticated CirroApi client.
            project_id: ID of the project that owns this dataset.
            all_tasks_ref: A shared list that will contain all tasks once they
                are all built.  Used by ``inputs`` to resolve ``source_task``.
        """
        self._trace = trace_row
        self._client = client
        self._project_id = project_id
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
        s3_path = S3Path(self.work_dir)
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

    def logs(self) -> str:
        """
        Return the contents of ``.command.log`` from the task's work directory.

        This file contains the combined stdout/stderr output of the task process.
        Returns an empty string if the file cannot be read.
        """
        return self._read_work_file('.command.log')

    def script(self) -> str:
        """
        Return the contents of ``.command.sh`` from the task's work directory.

        This is the actual shell script that Nextflow executed — the user's
        pipeline code for this task.
        Returns an empty string if the file cannot be read.
        """
        return self._read_work_file('.command.sh')

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
        content = self._read_work_file('.command.run')
        if not content:
            return []

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
                source_task=source_task
            ))
        return result

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
                        size=obj['Size']
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
