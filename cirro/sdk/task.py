from functools import cached_property
from pathlib import PurePath
import re
from typing import List, Optional, TYPE_CHECKING

from cirro_api_client.v1.errors import CirroException, UnexpectedStatus
from cirro_api_client.v1.models import Task
from cirro_api_client.v1.types import Unset
from cirro.models.file import FileAccessContext
from cirro.models.s3_path import S3Path
from cirro.sdk.exceptions import DataPortalAssetNotFound
from cirro.sdk.file_mixins import FileReadMixin

if TYPE_CHECKING:
    from cirro.cirro_client import CirroApi

# Nextflow stages these bookkeeping files alongside real inputs; they carry
# no data value for the user and are excluded from task.inputs.
_NEXTFLOW_COORDINATION_FILES = {
    '.command.sh', '.command.run', '.command.log', '.command.out',
    '.command.err', '.command.begin', '.command.exit', '.command.trace',
    '.exitcode',
}


class WorkDirFile(FileReadMixin):
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
            first_error = None
            for ctx in self._access_contexts():
                try:
                    s3 = self._client.file.get_aws_s3_client(ctx)
                    self._size = s3.head_object(
                        Bucket=self._s3_path.bucket, Key=self._s3_path.key
                    )['ContentLength']
                    break
                except Exception as e:  # NOSONAR
                    if first_error is None:
                        first_error = e
            else:
                raise DataPortalAssetNotFound(
                    f"Could not determine size of {self.name!r} — "
                    f"the work directory may have been cleaned up: {first_error}"
                ) from first_error
        return self._size

    def _access_contexts(self):
        """Return access contexts to try in order: scratch first, then regular download."""
        return [
            FileAccessContext.scratch_download(
                project_id=self._project_id,
                base_url=self._s3_path.base
            ),
            FileAccessContext.download(
                project_id=self._project_id,
                base_url=self._s3_path.base
            ),
        ]

    def _get(self) -> bytes:
        """Return the raw bytes of the file."""
        first_error = None
        for ctx in self._access_contexts():
            try:
                return self._client.file.get_file_from_path(ctx, self._s3_path.key)
            except Exception as e:  # NOSONAR
                if first_error is None:
                    first_error = e
        raise DataPortalAssetNotFound(
            f"Could not read {self.name!r} — "
            f"the work directory may have been cleaned up: {first_error}"
        ) from first_error

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
        task: Task,
        client: 'CirroApi',
        project_id: str,
        dataset_id: str = '',
        all_tasks_ref: Optional[list] = None,
        task_id: int = 0
    ):
        """
        Obtained from a dataset's ``tasks`` property.

        ```python
        for task in dataset.tasks:
            print(task.name, task.status)
            print(task.logs)
        ```

        Args:
            task (Task): Task object returned by the execution API.
            client (CirroApi): Authenticated CirroApi client.
            project_id (str): ID of the project that owns this dataset.
            dataset_id (str): ID of the dataset (execution) that owns this task.
            all_tasks_ref (list): A shared list that will contain all tasks once they
                are all built.  Used by ``inputs`` to resolve ``source_task``.
            task_id (int): Numeric index of this task in the execution's task list.
        """
        self._task = task
        self._client = client
        self._project_id = project_id
        self._dataset_id = dataset_id
        self._all_tasks_ref: list = all_tasks_ref if all_tasks_ref is not None else []
        self._task_id = task_id

    # ------------------------------------------------------------------ #
    # Task properties                                                      #
    # ------------------------------------------------------------------ #

    @property
    def task_id(self) -> int:
        """Sequential task identifier — the 0-based index of this task in the execution's task list."""
        return self._task_id

    @property
    def name(self) -> str:
        """Full task name, e.g. ``NFCORE_RNASEQ:RNASEQ:TRIMGALORE (sample1)``."""
        return self._task.name

    @property
    def status(self) -> str:
        """Task status string, e.g. ``COMPLETED``, ``FAILED``, ``ABORTED``."""
        return self._task.status

    @property
    def work_dir(self) -> str:
        """S3 URI of the task's work directory."""
        val = self._task.work_dir
        if isinstance(val, Unset):
            val = self._task_details.work_dir
        if isinstance(val, Unset) or val is None:
            return ''
        return val

    @property
    def native_id(self) -> str:
        """Native job ID on the underlying executor (e.g. AWS Batch job ID)."""
        val = self._task.native_job_id
        if isinstance(val, Unset) or val is None:
            return ''
        return val

    @property
    def command_line(self) -> str:
        """The shell command that was executed for this task."""
        val = self._task.command_line
        if isinstance(val, Unset) or val is None:
            return ''
        return val

    @property
    def log_location(self) -> str:
        """S3 URI or path to the task log file."""
        val = self._task.log_location
        if isinstance(val, Unset) or val is None:
            return ''
        return val

    @cached_property
    def exit_code(self) -> Optional[int]:
        """Process exit code."""
        val = self._task.exit_code
        if isinstance(val, Unset):
            val = self._task_details.exit_code
        if isinstance(val, Unset) or val is None:
            return None
        return val

    @cached_property
    def _task_details(self) -> Task:
        """Fetch full task details from the API (lazy, cached)."""
        if not self._dataset_id or not self.native_id:
            return self._task
        detail = self._client.execution.get_task(
            project_id=self._project_id,
            dataset_id=self._dataset_id,
            task_id=self.native_id
        )
        return detail if detail is not None else self._task

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
        except Exception:  # NOSONAR
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
            except (CirroException, UnexpectedStatus):
                pass
        return self._read_work_file('.command.log')

    @cached_property
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
        except Exception:  # NOSONAR
            return ''

    # ------------------------------------------------------------------ #
    # Inputs                                                               #
    # ------------------------------------------------------------------ #

    @cached_property
    def inputs(self) -> List[WorkDirFile]:
        """
        List of input files for this task, fetched from the execution API.

        Each file is annotated with ``source_task`` if its URI falls within
        another task's work directory.
        """
        return self._build_inputs()

    @cached_property
    def _task_files(self):
        """Fetch input and output files from the API (lazy, cached)."""
        if not self._dataset_id or not self.native_id:
            return None
        try:
            task_files = self._client.execution.get_task_files(
                project_id=self._project_id,
                dataset_id=self._dataset_id,
                task_id=self.native_id
            )
            if task_files is None:
                raise DataPortalAssetNotFound
            return task_files
        except Exception:  # NOSONAR
            return None

    def _build_inputs(self) -> List[WorkDirFile]:
        """Return input files from the cached task files API response."""
        task_files = self._task_files
        if task_files is None:
            return []
        native_id_to_task = {
            t.native_id: t
            for t in self._all_tasks_ref
            if t is not self and t.native_id
        }
        path_to_task = {
            t.work_dir: t
            for t in self._all_tasks_ref
            if t is not self and t.work_dir
        }
        result = []
        for tf in task_files.input_files:
            if PurePath(tf.path).name in _NEXTFLOW_COORDINATION_FILES:
                continue
            # Prefer 'uri' from additional_properties (full S3 URI); fall back to path-based matching
            source_native_id = tf.additional_properties.get('sourceTask')
            source_task = (
                native_id_to_task.get(source_native_id)
                if source_native_id else (
                    path_to_task.get(tf.path.rsplit("/", 1)[0])
                    if tf.path else None
                )
            )
            size = tf.size if not isinstance(tf.size, Unset) else None
            result.append(WorkDirFile(
                s3_uri=tf.path,
                client=self._client,
                project_id=self._project_id,
                size=size,
                source_task=source_task,
                dataset_id=self._dataset_id
            ))
        return result

    # ------------------------------------------------------------------ #
    # Outputs                                                            #
    # ------------------------------------------------------------------ #

    @cached_property
    def outputs(self) -> List[WorkDirFile]:
        """
        List of non-hidden output files in the task's work directory.

        Returns an empty list if the directory has been cleaned up or cannot
        be listed.
        """
        return self._build_outputs()

    def _build_outputs(self) -> List[WorkDirFile]:
        """Return output files from the cached task files API response."""
        task_files = self._task_files
        if task_files is None:
            return []
        result = []
        for tf in task_files.output_files:
            size = tf.size if not isinstance(tf.size, Unset) else None
            result.append(WorkDirFile(
                s3_uri=tf.path,
                client=self._client,
                project_id=self._project_id,
                size=size,
                dataset_id=self._dataset_id
            ))
        return result

    # ------------------------------------------------------------------ #
    # Summary / Repr                                                       #
    # ------------------------------------------------------------------ #

    @cached_property
    def summary(self) -> str:
        """Human-readable summary of this task including script, logs, inputs, and outputs."""
        lines = [
            f"Name:      {self.name}",
            f"Status:    {self.status}",
            f"Exit Code: {self.exit_code}",
            f"Work Dir:  {self.work_dir}",
            "",
            "--- Script ---",
            self.script or "(empty)",
            "",
            "--- Logs ---",
            self.logs or "(empty)",
        ]

        inputs = self.inputs
        lines.append(f"\n--- Inputs ({len(inputs)}) ---")
        for f in inputs:
            source = f"from task: {f.source_task.name}" if f.source_task else "staged input"
            lines.append(f"  {f.name}  [{source}]")

        outputs = self.outputs
        lines.append(f"\n--- Outputs ({len(outputs)}) ---")
        for f in outputs:
            lines.append(f"  {f.name}")

        return "\n".join(lines)

    def __str__(self):
        return f'Task(name={self.name}, status={self.status})'

    def __repr__(self):
        return f'DataPortalTask(name={self.name!r}, status={self.status!r})'
