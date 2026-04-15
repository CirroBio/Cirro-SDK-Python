import unittest
from unittest.mock import MagicMock, Mock, patch

from cirro_api_client.v1.models import ArtifactType

from cirro.models.assets import DatasetAssets, Artifact
from cirro.models.file import File
from cirro.sdk.dataset import DataPortalDataset
from cirro.sdk.exceptions import DataPortalInputError


TRACE_TSV = (
    "task_id\tname\tstatus\thash\tworkdir\texit\n"
    "1\tFASTQC (s1)\tCOMPLETED\tab/cd01\ts3://b/proj/work/ab/cd01\t0\n"
    "2\tTRIMGALORE (s1)\tFAILED\tef/gh02\ts3://b/proj/work/ef/gh02\t1\n"
)


def _make_dataset(execution_log='', trace_content=None):
    """
    Build a DataPortalDataset backed by a fully mocked CirroApi client.

    If ``trace_content`` is a string the mock will serve it as the
    WORKFLOW_TRACE artifact; if it is None the artifact is absent.
    """
    dataset_detail = MagicMock()
    dataset_detail.id = 'ds-123'
    dataset_detail.project_id = 'proj-1'
    dataset_detail.name = 'Test Dataset'

    client = Mock()
    client.execution.get_execution_logs.return_value = execution_log

    # Build asset listing with or without a trace artifact
    if trace_content is not None:
        trace_file = MagicMock(spec=File)
        trace_file.absolute_path = 's3://bucket/proj/artifacts/trace.tsv'
        trace_artifact = Artifact(artifact_type=ArtifactType.WORKFLOW_TRACE, file=trace_file)
        assets = DatasetAssets(files=[], artifacts=[trace_artifact])
        client.file.get_file_from_path.return_value = trace_content.encode()
    else:
        assets = DatasetAssets(files=[], artifacts=[])

    client.datasets.get_assets_listing.return_value = assets

    return DataPortalDataset(dataset=dataset_detail, client=client), client


class TestDataPortalDatasetLogs(unittest.TestCase):

    def test_logs_returns_string(self):
        dataset, client = _make_dataset(execution_log='workflow started\nworkflow ended\n')
        result = dataset.logs()
        self.assertEqual(result, 'workflow started\nworkflow ended\n')
        client.execution.get_execution_logs.assert_called_once_with(
            project_id='proj-1',
            dataset_id='ds-123'
        )

    def test_logs_returns_empty_string_on_error(self):
        dataset, client = _make_dataset()
        client.execution.get_execution_logs.side_effect = Exception("CloudWatch unavailable")
        result = dataset.logs()
        self.assertEqual(result, '')

    def test_logs_returns_empty_string_when_no_log(self):
        dataset, _ = _make_dataset(execution_log='')
        self.assertEqual(dataset.logs(), '')


class TestDataPortalDatasetTasks(unittest.TestCase):

    def test_tasks_parsed_from_trace(self):
        dataset, _ = _make_dataset(trace_content=TRACE_TSV)
        with patch('cirro.sdk.task.FileAccessContext'):
            tasks = dataset.tasks
        self.assertEqual(len(tasks), 2)
        self.assertEqual(tasks[0].name, 'FASTQC (s1)')
        self.assertEqual(tasks[0].status, 'COMPLETED')
        self.assertEqual(tasks[0].exit_code, 0)
        self.assertEqual(tasks[1].name, 'TRIMGALORE (s1)')
        self.assertEqual(tasks[1].status, 'FAILED')
        self.assertEqual(tasks[1].exit_code, 1)

    def test_tasks_cached(self):
        dataset, _ = _make_dataset(trace_content=TRACE_TSV)
        with patch('cirro.sdk.task.FileAccessContext'):
            first = dataset.tasks
            second = dataset.tasks
        self.assertIs(first, second)

    def test_tasks_raises_for_non_nextflow_dataset(self):
        dataset, _ = _make_dataset(trace_content=None)
        with self.assertRaises(DataPortalInputError):
            _ = dataset.tasks

    def test_tasks_empty_list_for_empty_trace(self):
        # Trace file exists but has no rows (header only)
        dataset, _ = _make_dataset(trace_content='task_id\tname\tstatus\thash\tworkdir\texit\n')
        with patch('cirro.sdk.task.FileAccessContext'):
            tasks = dataset.tasks
        self.assertEqual(tasks, [])

    def test_tasks_all_tasks_ref_populated(self):
        """All tasks share a common all_tasks_ref so source_task resolution works."""
        dataset, _ = _make_dataset(trace_content=TRACE_TSV)
        with patch('cirro.sdk.task.FileAccessContext'):
            tasks = dataset.tasks
        # Each task's _all_tasks_ref should contain all tasks
        self.assertEqual(len(tasks[0]._all_tasks_ref), 2)
        self.assertIs(tasks[0]._all_tasks_ref, tasks[1]._all_tasks_ref)


class TestDataPortalDatasetPrimaryFailedTask(unittest.TestCase):

    def test_returns_failed_task(self):
        dataset, _ = _make_dataset(trace_content=TRACE_TSV)
        with patch('cirro.sdk.task.FileAccessContext'):
            result = dataset.primary_failed_task
        self.assertIsNotNone(result)
        self.assertEqual(result.name, 'TRIMGALORE (s1)')

    def test_returns_none_for_non_nextflow_dataset(self):
        dataset, _ = _make_dataset(trace_content=None)
        result = dataset.primary_failed_task
        self.assertIsNone(result)

    def test_returns_none_when_no_tasks_failed(self):
        trace = (
            "task_id\tname\tstatus\thash\tworkdir\texit\n"
            "1\tFASTQC (s1)\tCOMPLETED\tab/cd01\ts3://b/proj/work/ab/cd01\t0\n"
        )
        dataset, _ = _make_dataset(trace_content=trace)
        with patch('cirro.sdk.task.FileAccessContext'):
            result = dataset.primary_failed_task
        self.assertIsNone(result)

    def test_returns_none_for_empty_trace(self):
        dataset, _ = _make_dataset(trace_content='task_id\tname\tstatus\thash\tworkdir\texit\n')
        with patch('cirro.sdk.task.FileAccessContext'):
            result = dataset.primary_failed_task
        self.assertIsNone(result)

    def test_uses_execution_log_for_disambiguation(self):
        trace = (
            "task_id\tname\tstatus\thash\tworkdir\texit\n"
            "1\tFASTQC (s1)\tFAILED\tab/cd01\ts3://b/proj/work/ab/cd01\t1\n"
            "2\tTRIMGALORE (s1)\tFAILED\tef/gh02\ts3://b/proj/work/ef/gh02\t1\n"
        )
        log = "Error executing process > 'TRIMGALORE (s1)'"
        dataset, _ = _make_dataset(execution_log=log, trace_content=trace)
        with patch('cirro.sdk.task.FileAccessContext'):
            result = dataset.primary_failed_task
        self.assertEqual(result.name, 'TRIMGALORE (s1)')


if __name__ == '__main__':
    unittest.main()
