import unittest
from unittest.mock import MagicMock, Mock

from cirro_api_client.v1.errors import UnexpectedStatus
from cirro_api_client.v1.models import Task
from cirro_api_client.v1.types import Unset

from cirro.sdk.dataset import DataPortalDataset


def _make_dataset(execution_log=''):
    dataset_detail = MagicMock()
    dataset_detail.id = 'ds-123'
    dataset_detail.project_id = 'proj-1'
    dataset_detail.name = 'Test Dataset'

    client = Mock()
    client.execution.get_execution_logs.return_value = execution_log
    client.execution.get_tasks_for_execution.return_value = []

    return DataPortalDataset(dataset=dataset_detail, client=client), client


class TestDataPortalDatasetLogs(unittest.TestCase):

    def test_logs_returns_string(self):
        dataset, client = _make_dataset(execution_log='workflow started\nworkflow ended\n')
        result = dataset.logs
        self.assertEqual(result, 'workflow started\nworkflow ended\n')
        client.execution.get_execution_logs.assert_called_once_with(
            project_id='proj-1',
            dataset_id='ds-123'
        )

    def test_logs_returns_empty_string_on_error(self):
        dataset, client = _make_dataset()
        client.execution.get_execution_logs.side_effect = UnexpectedStatus(404, b'Not found')
        result = dataset.logs
        self.assertEqual(result, '')

    def test_logs_returns_empty_string_when_no_log(self):
        dataset, _ = _make_dataset(execution_log='')
        self.assertEqual(dataset.logs, '')


class TestDataPortalDatasetTasks(unittest.TestCase):

    def test_tasks_from_api(self):
        dataset, client = _make_dataset()
        client.execution.get_tasks_for_execution.return_value = [
            Task(name='FASTQC (s1)', status='COMPLETED', native_job_id='job-1'),
            Task(name='TRIMGALORE (s1)', status='FAILED', native_job_id='job-2'),
        ]
        tasks = dataset.tasks
        self.assertEqual(len(tasks), 2)
        self.assertEqual(tasks[0].name, 'FASTQC (s1)')
        self.assertEqual(tasks[0].status, 'COMPLETED')
        self.assertEqual(tasks[0].native_id, 'job-1')
        self.assertEqual(tasks[1].name, 'TRIMGALORE (s1)')
        self.assertEqual(tasks[1].status, 'FAILED')

    def test_tasks_cached(self):
        dataset, client = _make_dataset()
        first = dataset.tasks
        second = dataset.tasks
        self.assertIs(first, second)
        client.execution.get_tasks_for_execution.assert_called_once()

    def test_tasks_empty_when_api_returns_none(self):
        dataset, client = _make_dataset()
        client.execution.get_tasks_for_execution.return_value = None
        self.assertEqual(dataset.tasks, [])

    def test_tasks_empty_when_api_returns_empty_list(self):
        dataset, _ = _make_dataset()
        self.assertEqual(dataset.tasks, [])

    def test_tasks_api_called_with_correct_ids(self):
        dataset, client = _make_dataset()
        _ = dataset.tasks
        client.execution.get_tasks_for_execution.assert_called_once_with(
            project_id='proj-1',
            dataset_id='ds-123'
        )

    def test_api_task_native_id_mapping(self):
        dataset, client = _make_dataset()
        client.execution.get_tasks_for_execution.return_value = [
            Task(name='TRIM', status='COMPLETED', native_job_id='batch-job-123')
        ]
        self.assertEqual(dataset.tasks[0].native_id, 'batch-job-123')

    def test_api_task_unset_native_id(self):
        dataset, client = _make_dataset()
        client.execution.get_tasks_for_execution.return_value = [
            Task(name='BWA', status='FAILED', native_job_id=Unset())
        ]
        self.assertEqual(dataset.tasks[0].native_id, '')

    def test_api_task_none_native_id(self):
        dataset, client = _make_dataset()
        client.execution.get_tasks_for_execution.return_value = [
            Task(name='BWA', status='FAILED', native_job_id=None)
        ]
        self.assertEqual(dataset.tasks[0].native_id, '')


class TestDataPortalDatasetPrimaryFailedTask(unittest.TestCase):

    def test_returns_failed_task(self):
        dataset, client = _make_dataset()
        client.execution.get_tasks_for_execution.return_value = [
            Task(name='FASTQC (s1)', status='COMPLETED', native_job_id=None),
            Task(name='TRIMGALORE (s1)', status='FAILED', native_job_id=None),
        ]
        result = dataset.primary_failed_task
        self.assertIsNotNone(result)
        self.assertEqual(result.name, 'TRIMGALORE (s1)')

    def test_returns_none_when_api_returns_empty(self):
        dataset, _ = _make_dataset()
        self.assertIsNone(dataset.primary_failed_task)

    def test_returns_none_when_no_tasks_failed(self):
        dataset, client = _make_dataset()
        client.execution.get_tasks_for_execution.return_value = [
            Task(name='FASTQC (s1)', status='COMPLETED', native_job_id=None),
        ]
        self.assertIsNone(dataset.primary_failed_task)

    def test_uses_execution_log_for_disambiguation(self):
        log = "Error executing process > 'TRIMGALORE (s1)'"
        dataset, client = _make_dataset(execution_log=log)
        client.execution.get_tasks_for_execution.return_value = [
            Task(name='FASTQC (s1)', status='FAILED', native_job_id=None),
            Task(name='TRIMGALORE (s1)', status='FAILED', native_job_id=None),
        ]
        result = dataset.primary_failed_task
        self.assertEqual(result.name, 'TRIMGALORE (s1)')


if __name__ == '__main__':
    unittest.main()
