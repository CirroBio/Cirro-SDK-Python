import gzip
import json
import unittest
from unittest.mock import MagicMock, Mock, patch

from cirro.sdk.task import WorkDirFile, DataPortalTask
from cirro.sdk.exceptions import DataPortalAssetNotFound


def _make_client(file_bytes=b'hello world'):
    """Return a minimal CirroApi mock with a file service."""
    client = Mock()
    client.file.get_file_from_path.return_value = file_bytes
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


TRACE_ROW = {
    'task_id': '3',
    'name': 'NFCORE:RNASEQ:FASTQC (sample1)',
    'status': 'FAILED',
    'hash': 'ab/cdef12',
    'workdir': 's3://bucket/proj/work/ab/cdef12',
    'exit': '1',
}


def _make_task(trace_row=None, file_bytes=b'log content', all_tasks_ref=None):
    """Construct a DataPortalTask with a mocked client."""
    client = _make_client(file_bytes)
    task = DataPortalTask(
        trace_row=trace_row if trace_row is not None else dict(TRACE_ROW),
        client=client,
        project_id='proj-1',
        all_tasks_ref=all_tasks_ref,
    )
    return task, client


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
        self.assertEqual(task.task_id, 3)

    def test_task_id_missing(self):
        task, _ = _make_task(trace_row={})
        self.assertEqual(task.task_id, 0)

    def test_name(self):
        task, _ = _make_task()
        self.assertEqual(task.name, 'NFCORE:RNASEQ:FASTQC (sample1)')

    def test_status(self):
        task, _ = _make_task()
        self.assertEqual(task.status, 'FAILED')

    def test_hash(self):
        task, _ = _make_task()
        self.assertEqual(task.hash, 'ab/cdef12')

    def test_work_dir(self):
        task, _ = _make_task()
        self.assertEqual(task.work_dir, 's3://bucket/proj/work/ab/cdef12')

    def test_exit_code_int(self):
        task, _ = _make_task()
        self.assertEqual(task.exit_code, 1)

    def test_exit_code_none_when_missing(self):
        task, _ = _make_task(trace_row={**TRACE_ROW, 'exit': ''})
        self.assertIsNone(task.exit_code)

    def test_exit_code_none_when_dash(self):
        task, _ = _make_task(trace_row={**TRACE_ROW, 'exit': '-'})
        self.assertIsNone(task.exit_code)


class TestDataPortalTaskWorkDirFiles(unittest.TestCase):

    def test_logs_returns_content(self):
        task, client = _make_task(file_bytes=b'execution output')
        with patch('cirro.sdk.task.FileAccessContext'):
            result = task.logs()
        self.assertEqual(result, 'execution output')

    def test_logs_returns_empty_on_error(self):
        task, client = _make_task()
        client.file.get_file_from_path.side_effect = Exception("not found")
        with patch('cirro.sdk.task.FileAccessContext'):
            result = task.logs()
        self.assertEqual(result, '')

    def test_logs_empty_when_no_work_dir(self):
        task, _ = _make_task(trace_row={**TRACE_ROW, 'workdir': ''})
        result = task.logs()
        self.assertEqual(result, '')

    def test_script_returns_content(self):
        task, client = _make_task(file_bytes=b'#!/bin/bash\necho hello')
        with patch('cirro.sdk.task.FileAccessContext'):
            result = task.script()
        self.assertEqual(result, '#!/bin/bash\necho hello')

    def test_outputs_empty_on_error(self):
        task, client = _make_task()
        client.file.get_aws_s3_client.side_effect = Exception("no credentials")
        with patch('cirro.sdk.task.FileAccessContext'):
            result = task.outputs
        self.assertEqual(result, [])

    def test_outputs_empty_when_no_work_dir(self):
        task, _ = _make_task(trace_row={**TRACE_ROW, 'workdir': ''})
        result = task.outputs
        self.assertEqual(result, [])


class TestDataPortalTaskInputs(unittest.TestCase):

    def test_inputs_parses_s3_uris(self):
        command_run = (
            b"aws s3 cp --only-show-errors "
            b"s3://bucket/proj/work/aa/bb/reads.fastq.gz ./reads.fastq.gz\n"
        )
        task, client = _make_task(file_bytes=command_run)

        with patch('cirro.sdk.task.FileAccessContext'):
            inputs = task.inputs

        self.assertEqual(len(inputs), 1)
        self.assertEqual(inputs[0].name, 'reads.fastq.gz')
        self.assertIsNone(inputs[0].source_task)

    def test_inputs_links_source_task(self):
        source_work_dir = 's3://bucket/proj/work/aa/bb'
        command_run = (
            f"aws s3 cp --only-show-errors "
            f"{source_work_dir}/reads.fastq.gz ./reads.fastq.gz\n"
        ).encode()

        upstream = MagicMock()
        upstream.work_dir = source_work_dir
        all_tasks_ref = [upstream]

        task, client = _make_task(file_bytes=command_run, all_tasks_ref=all_tasks_ref)
        all_tasks_ref.append(task)

        with patch('cirro.sdk.task.FileAccessContext'):
            inputs = task.inputs

        self.assertEqual(len(inputs), 1)
        self.assertIs(inputs[0].source_task, upstream)

    def test_inputs_empty_when_no_work_dir(self):
        task, _ = _make_task(trace_row={**TRACE_ROW, 'workdir': ''})
        result = task.inputs
        self.assertEqual(result, [])

    def test_inputs_cached(self):
        task, client = _make_task(file_bytes=b'')
        with patch('cirro.sdk.task.FileAccessContext'):
            first = task.inputs
            second = task.inputs
        self.assertIs(first, second)


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
