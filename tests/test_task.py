import gzip
import json
import unittest
from unittest.mock import MagicMock, Mock, patch

from cirro_api_client.v1.models import ArtifactType, Task
from cirro_api_client.v1.types import Unset

from cirro.models.assets import DatasetAssets, Artifact
from cirro.models.file import File
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


API_TASK = Task(
    name='NFCORE:RNASEQ:FASTQC (sample1)',
    status='FAILED',
    native_job_id='job-1',
)


def _make_task(task=None, file_bytes=b'log content', all_tasks_ref=None, dataset_id=''):
    """Construct a DataPortalTask with a mocked client."""
    client = _make_client(file_bytes)
    t = DataPortalTask(
        task=task if task is not None else API_TASK,
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

    def test_task_id(self):
        task, _ = _make_task()
        self.assertEqual(task.task_id, 0)

    def test_name(self):
        task, _ = _make_task()
        self.assertEqual(task.name, 'NFCORE:RNASEQ:FASTQC (sample1)')

    def test_status(self):
        task, _ = _make_task()
        self.assertEqual(task.status, 'FAILED')

    def test_hash(self):
        task, _ = _make_task()
        self.assertEqual(task.hash, '')

    def test_work_dir(self):
        task, _ = _make_task()
        self.assertEqual(task.work_dir, '')

    def test_exit_code(self):
        task, _ = _make_task()
        self.assertIsNone(task.exit_code)

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

    def test_outputs_empty_without_work_dir(self):
        task, _ = _make_task()
        result = task.outputs
        self.assertEqual(result, [])


class TestDataPortalTaskInputs(unittest.TestCase):

    def test_inputs_empty_when_no_dataset_id(self):
        task, _ = _make_task(dataset_id='')
        self.assertEqual(task.inputs, [])

    def test_inputs_cached(self):
        task, _ = _make_task(dataset_id='')
        first = task.inputs
        second = task.inputs
        self.assertIs(first, second)

    def _make_task_with_files_artifact(self, task_name, files_csv):
        t, client = _make_task(
            task=Task(name=task_name, status='FAILED', native_job_id='job-1'),
            dataset_id='ds-123',
        )
        files_file = MagicMock(spec=File)
        files_artifact = Artifact(artifact_type=ArtifactType.FILES, file=files_file)
        assets = DatasetAssets(files=[], artifacts=[files_artifact])
        client.datasets.get_assets_listing.return_value = assets
        client.file.get_file.return_value = files_csv.encode()
        return t, client

    def test_inputs_from_files_artifact(self):
        files_csv = (
            'sample,file,process,dataset,sampleIndex\n'
            'sample1,s3://bucket/datasets/src/data/reads.fastq.gz,reads,src-ds,1\n'
        )
        task, _ = self._make_task_with_files_artifact('FASTQC (sample1)', files_csv)
        with patch('cirro.sdk.task.FileAccessContext'):
            inputs = task.inputs
        self.assertEqual(len(inputs), 1)
        self.assertEqual(inputs[0].name, 'reads.fastq.gz')

    def test_inputs_files_artifact_matches_by_filename(self):
        files_csv = (
            'sample,file,process,dataset,sampleIndex\n'
            'genome,s3://bucket/datasets/src/data/genome.fasta,genome_fasta,src-ds,1\n'
        )
        task, _ = self._make_task_with_files_artifact('BWA_INDEX (genome.fasta)', files_csv)
        with patch('cirro.sdk.task.FileAccessContext'):
            inputs = task.inputs
        self.assertEqual(len(inputs), 1)
        self.assertEqual(inputs[0].name, 'genome.fasta')

    def test_inputs_files_artifact_empty_when_no_match(self):
        files_csv = (
            'sample,file,process,dataset,sampleIndex\n'
            'other,s3://bucket/datasets/src/data/other.fasta,genome_fasta,src-ds,1\n'
        )
        task, _ = self._make_task_with_files_artifact('BWA_INDEX (genome.fasta)', files_csv)
        with patch('cirro.sdk.task.FileAccessContext'):
            inputs = task.inputs
        self.assertEqual(inputs, [])


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
