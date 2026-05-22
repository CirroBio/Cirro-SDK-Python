import gzip
import json
import unittest
from unittest.mock import MagicMock, Mock, patch

from cirro_api_client.v1.models import Task
from cirro_api_client.v1.types import Unset

from cirro.sdk.task import WorkDirFile, DataPortalTask
from cirro.sdk.exceptions import DataPortalAssetNotFound


def _make_client(file_bytes=b'hello world'):
    """Return a minimal CirroApi mock with a file service."""
    client = Mock()
    client.file.get_file_from_path.return_value = file_bytes
    client.execution.get_task_logs.return_value = ''
    return client


def _make_wf(uri='s3://bucket/proj/work/ab/cdef/file.txt',
             file_bytes=b'hello world',
             size=None,
             source_task=None):
    """Construct a WorkDirFile with a mocked client."""
    client = _make_client(file_bytes)
    with patch('cirro.sdk.task.FileAccessContext'):
        return WorkDirFile(
            s3_uri=uri,
            client=client,
            project_id='proj-1',
            size=size,
            source_task=source_task,
        ), client


def _default_task():
    return Task(name='NFCORE:RNASEQ:FASTQC (sample1)', status='FAILED', native_job_id='job-1')


def _make_task(task=None, file_bytes=b'log content', all_tasks_ref=None, dataset_id=''):
    """Construct a DataPortalTask with a mocked client."""
    client = _make_client(file_bytes)
    t = DataPortalTask(
        task=task if task is not None else _default_task(),
        client=client,
        project_id='proj-1',
        dataset_id=dataset_id,
        all_tasks_ref=all_tasks_ref,
    )
    return t, client


class TestWorkDirFileName(unittest.TestCase):

    def test_name_extracted_from_uri(self):
        wf, _ = _make_wf(uri='s3://bucket/proj/work/ab/cdef/reads.fastq.gz')
        self.assertEqual(wf.name, 'reads.fastq.gz')

    def test_name_simple(self):
        wf, _ = _make_wf(uri='s3://bucket/proj/work/ab/cdef/report.html')
        self.assertEqual(wf.name, 'report.html')


class TestWorkDirFileSize(unittest.TestCase):

    def test_size_prepopulated(self):
        wf, _ = _make_wf(size=1024)
        self.assertEqual(wf.size, 1024)

    def test_size_lazy_head_object(self):
        wf, client = _make_wf()
        s3_mock = Mock()
        s3_mock.head_object.return_value = {'ContentLength': 512}
        client.file.get_aws_s3_client.return_value = s3_mock
        with patch('cirro.sdk.task.FileAccessContext'):
            result = wf.size
        self.assertEqual(result, 512)
        self.assertEqual(wf.size, 512)  # cached — head_object called only once
        s3_mock.head_object.assert_called_once()

    def test_size_raises_on_s3_error(self):
        wf, client = _make_wf()
        s3_mock = Mock()
        s3_mock.head_object.side_effect = Exception("NoSuchKey")
        client.file.get_aws_s3_client.return_value = s3_mock
        with patch('cirro.sdk.task.FileAccessContext'):
            with self.assertRaises(DataPortalAssetNotFound):
                _ = wf.size


class TestWorkDirFileRead(unittest.TestCase):

    def test_read_text(self):
        wf, _ = _make_wf(file_bytes=b'line1\nline2\n')
        with patch('cirro.sdk.task.FileAccessContext'):
            result = wf.read()
        self.assertEqual(result, 'line1\nline2\n')

    def test_read_gzip(self):
        import io
        buf = io.BytesIO()
        with gzip.GzipFile(fileobj=buf, mode='wb') as gz:
            gz.write(b'compressed content')
        wf, _ = _make_wf(file_bytes=buf.getvalue())
        with patch('cirro.sdk.task.FileAccessContext'):
            result = wf.read(compression='gzip')
        self.assertEqual(result, 'compressed content')

    def test_read_unsupported_compression_raises(self):
        wf, _ = _make_wf()
        with patch('cirro.sdk.task.FileAccessContext'):
            with self.assertRaises(ValueError):
                wf.read(compression='bz2')

    def test_readlines(self):
        wf, _ = _make_wf(file_bytes=b'a\nb\nc')
        with patch('cirro.sdk.task.FileAccessContext'):
            lines = wf.readlines()
        self.assertEqual(lines, ['a', 'b', 'c'])

    def test_read_raises_on_s3_error(self):
        wf, client = _make_wf()
        client.file.get_file_from_path.side_effect = Exception("access denied")
        with patch('cirro.sdk.task.FileAccessContext'):
            with self.assertRaises(DataPortalAssetNotFound):
                wf.read()

    def test_read_json(self):
        payload = {'key': 'value', 'count': 42}
        wf, _ = _make_wf(file_bytes=json.dumps(payload).encode())
        with patch('cirro.sdk.task.FileAccessContext'):
            result = wf.read_json()
        self.assertEqual(result, payload)

    def test_read_json_invalid_raises(self):
        wf, _ = _make_wf(file_bytes=b'not json {{{')
        with patch('cirro.sdk.task.FileAccessContext'):
            with self.assertRaises(ValueError):
                wf.read_json()


class TestWorkDirFileSourceTask(unittest.TestCase):

    def test_source_task_none_by_default(self):
        wf, _ = _make_wf()
        self.assertIsNone(wf.source_task)

    def test_source_task_set(self):
        mock_task = MagicMock()
        mock_task.name = 'upstream_task'
        wf, _ = _make_wf(source_task=mock_task)
        self.assertIs(wf.source_task, mock_task)


class TestWorkDirFileRepr(unittest.TestCase):

    def test_str(self):
        wf, _ = _make_wf(uri='s3://bucket/proj/work/ab/cdef/output.bam')
        self.assertEqual(str(wf), 'output.bam')

    def test_repr(self):
        wf, _ = _make_wf(uri='s3://bucket/proj/work/ab/cdef/output.bam')
        self.assertIn('output.bam', repr(wf))


class TestDataPortalTaskProperties(unittest.TestCase):

    def test_task_id_default(self):
        task, _ = _make_task()
        self.assertEqual(task.task_id, 0)

    def test_task_id_custom(self):
        client = _make_client()
        t = DataPortalTask(
            task=_default_task(),
            client=client,
            project_id='proj-1',
            dataset_id='ds-1',
            task_id=5,
        )
        self.assertEqual(t.task_id, 5)

    def test_name(self):
        task, _ = _make_task()
        self.assertEqual(task.name, 'NFCORE:RNASEQ:FASTQC (sample1)')

    def test_status(self):
        task, _ = _make_task()
        self.assertEqual(task.status, 'FAILED')

    def test_hash_unset(self):
        task, client = _make_task()
        client.execution.get_task.return_value = None
        self.assertEqual(task.hash, '')

    def test_work_dir_present_on_task(self):
        task, _ = _make_task(task=Task(name='X', status='COMPLETED', work_dir='s3://bucket/work/ab/cd'))
        self.assertEqual(task.work_dir, 's3://bucket/work/ab/cd')

    def test_work_dir_unset(self):
        task, client = _make_task()
        client.execution.get_task.return_value = None
        self.assertEqual(task.work_dir, '')

    def test_exit_code_fetched_via_get_task(self):
        task, client = _make_task(dataset_id='ds-123')
        client.execution.get_task.return_value = Task(
            name='FASTQC', status='FAILED', exit_code=1, work_dir='s3://b/w', hash='ab/cd'
        )
        self.assertEqual(task.exit_code, 1)
        client.execution.get_task.assert_called_once_with(
            project_id='proj-1', dataset_id='ds-123', task_id='job-1'
        )

    def test_exit_code_none_when_api_returns_none(self):
        task, client = _make_task(dataset_id='ds-123')
        client.execution.get_task.return_value = Task(name='X', status='FAILED', exit_code=None)
        self.assertIsNone(task.exit_code)

    def test_exit_code_from_task_if_already_set(self):
        t = Task(name='X', status='COMPLETED', native_job_id='job-1', exit_code=0)
        task, client = _make_task(task=t, dataset_id='ds-123')
        self.assertEqual(task.exit_code, 0)
        client.execution.get_task.assert_not_called()

    def test_work_dir_fetched_via_get_task(self):
        task, client = _make_task(dataset_id='ds-123')
        client.execution.get_task.return_value = Task(
            name='FASTQC', status='FAILED', work_dir='s3://bucket/work/ab/cd'
        )
        self.assertEqual(task.work_dir, 's3://bucket/work/ab/cd')

    def test_hash_fetched_via_get_task(self):
        task, client = _make_task(dataset_id='ds-123')
        client.execution.get_task.return_value = Task(
            name='FASTQC', status='FAILED', hash='ab/cdef12'
        )
        self.assertEqual(task.hash, 'ab/cdef12')

    def test_get_task_called_once_for_multiple_fields(self):
        task, client = _make_task(dataset_id='ds-123')
        detail = Task(name='FASTQC', status='FAILED', exit_code=1, work_dir='s3://b/w', hash='ab/cd')
        client.execution.get_task.return_value = detail
        _ = task.exit_code
        _ = task.work_dir
        _ = task.hash
        client.execution.get_task.assert_called_once()

    def test_native_id(self):
        task, _ = _make_task()
        self.assertEqual(task.native_id, 'job-1')

    def test_native_id_unset(self):
        task, _ = _make_task(task=Task(name='X', status='COMPLETED', native_job_id=Unset()))
        self.assertEqual(task.native_id, '')

    def test_native_id_none(self):
        task, _ = _make_task(task=Task(name='X', status='COMPLETED', native_job_id=None))
        self.assertEqual(task.native_id, '')


class TestDataPortalTaskLogs(unittest.TestCase):

    def test_logs_fetched_via_api(self):
        task, client = _make_task(dataset_id='ds-123')
        client.execution.get_task_logs.return_value = 'execution output'
        result = task.logs
        self.assertEqual(result, 'execution output')
        client.execution.get_task_logs.assert_called_once_with(
            project_id='proj-1', dataset_id='ds-123', task_id='job-1'
        )

    def test_logs_empty_on_api_error(self):
        from cirro_api_client.v1.errors import UnexpectedStatus
        task, client = _make_task(dataset_id='ds-123')
        client.execution.get_task_logs.side_effect = UnexpectedStatus(404, b'Not found')
        result = task.logs
        self.assertEqual(result, '')

    def test_logs_empty_when_no_dataset_id(self):
        task, _ = _make_task(dataset_id='')
        result = task.logs
        self.assertEqual(result, '')

    def test_script_empty_without_work_dir(self):
        task, _ = _make_task()
        result = task.script
        self.assertEqual(result, '')

    def test_outputs_empty_without_dataset_id(self):
        task, _ = _make_task(dataset_id='')
        result = task.outputs
        self.assertEqual(result, [])


class TestDataPortalTaskInputs(unittest.TestCase):

    def test_inputs_empty_when_no_dataset_id(self):
        task, _ = _make_task(dataset_id='')
        self.assertEqual(task.inputs, [])

    def test_inputs_empty_when_no_native_id(self):
        task, _ = _make_task(task=Task(name='X', status='COMPLETED'), dataset_id='ds-1')
        self.assertEqual(task.inputs, [])

    def test_inputs_cached(self):
        from cirro_api_client.v1.models import TaskFilesResponse
        task, client = _make_task(dataset_id='ds-1')
        client.execution.get_task_files.return_value = TaskFilesResponse(input_files=[], output_files=[])
        first = task.inputs
        second = task.inputs
        self.assertIs(first, second)
        client.execution.get_task_files.assert_called_once()

    def test_inputs_from_api(self):
        from cirro_api_client.v1.models import TaskFile, TaskFilesResponse
        task, client = _make_task(dataset_id='ds-1')
        client.execution.get_task_files.return_value = TaskFilesResponse(
            input_files=[TaskFile(uri='s3://bucket/work/ab/cd/reads.fastq.gz', size=100)],
            output_files=[],
        )
        inputs = task.inputs
        self.assertEqual(len(inputs), 1)
        self.assertEqual(inputs[0].name, 'reads.fastq.gz')
        self.assertEqual(inputs[0].size, 100)
        client.execution.get_task_files.assert_called_once_with(
            project_id='proj-1', dataset_id='ds-1', task_id='job-1'
        )

    def test_inputs_source_task_linked(self):
        from cirro_api_client.v1.models import TaskFile, TaskFilesResponse
        upstream_task = MagicMock()
        upstream_task.native_id = 'job-upstream'
        all_tasks_ref = [upstream_task]

        task, client = _make_task(dataset_id='ds-1', all_tasks_ref=all_tasks_ref)
        all_tasks_ref.append(task)
        client.execution.get_task_files.return_value = TaskFilesResponse(
            input_files=[TaskFile(uri='s3://bucket/work/ab/cd/reads.fastq.gz', size=50, source_task='job-upstream')],
            output_files=[],
        )
        inputs = task.inputs
        self.assertEqual(len(inputs), 1)
        self.assertIs(inputs[0].source_task, upstream_task)

    def test_inputs_source_task_null(self):
        from cirro_api_client.v1.models import TaskFile, TaskFilesResponse
        task, client = _make_task(dataset_id='ds-1')
        client.execution.get_task_files.return_value = TaskFilesResponse(
            input_files=[TaskFile(uri='s3://external/reference/genome.fa', size=1000, source_task=None)],
            output_files=[],
        )
        inputs = task.inputs
        self.assertEqual(len(inputs), 1)
        self.assertIsNone(inputs[0].source_task)

    def test_inputs_empty_on_api_error(self):
        task, client = _make_task(dataset_id='ds-1')
        client.execution.get_task_files.side_effect = Exception('network error')
        self.assertEqual(task.inputs, [])


class TestDataPortalTaskOutputs(unittest.TestCase):

    def test_outputs_empty_when_no_dataset_id(self):
        task, _ = _make_task(dataset_id='')
        self.assertEqual(task.outputs, [])

    def test_outputs_empty_when_no_native_id(self):
        task, _ = _make_task(task=Task(name='X', status='COMPLETED'), dataset_id='ds-1')
        self.assertEqual(task.outputs, [])

    def test_outputs_from_api(self):
        from cirro_api_client.v1.models import TaskFile, TaskFilesResponse
        task, client = _make_task(dataset_id='ds-1')
        client.execution.get_task_files.return_value = TaskFilesResponse(
            input_files=[],
            output_files=[TaskFile(uri='s3://bucket/work/ab/cd/result.bam', size=2048)],
        )
        outputs = task.outputs
        self.assertEqual(len(outputs), 1)
        self.assertEqual(outputs[0].name, 'result.bam')
        self.assertEqual(outputs[0].size, 2048)
        client.execution.get_task_files.assert_called_once_with(
            project_id='proj-1', dataset_id='ds-1', task_id='job-1'
        )

    def test_outputs_empty_on_api_error(self):
        task, client = _make_task(dataset_id='ds-1')
        client.execution.get_task_files.side_effect = Exception('network error')
        self.assertEqual(task.outputs, [])

    def test_api_called_once_for_inputs_and_outputs(self):
        from cirro_api_client.v1.models import TaskFile, TaskFilesResponse
        task, client = _make_task(dataset_id='ds-1')
        client.execution.get_task_files.return_value = TaskFilesResponse(
            input_files=[TaskFile(uri='s3://b/w/in.txt', size=10)],
            output_files=[TaskFile(uri='s3://b/w/out.txt', size=20)],
        )
        _ = task.inputs
        _ = task.outputs
        client.execution.get_task_files.assert_called_once()


class TestDataPortalTaskRepr(unittest.TestCase):

    def test_str(self):
        task, _ = _make_task()
        s = str(task)
        self.assertIn('FASTQC', s)
        self.assertIn('FAILED', s)

    def test_repr(self):
        task, _ = _make_task()
        r = repr(task)
        self.assertIn('FASTQC', r)
        self.assertIn('FAILED', r)


if __name__ == '__main__':
    unittest.main()
