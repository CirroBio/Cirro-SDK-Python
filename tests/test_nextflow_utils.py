import unittest
from unittest.mock import MagicMock

from cirro.sdk.nextflow_utils import find_primary_failed_task


def _make_task(task_id, name, status, exit_code=None):
    """Build a minimal DataPortalTask-like mock."""
    task = MagicMock()
    task.task_id = task_id
    task.name = name
    task.status = status
    task.exit_code = exit_code
    return task


class TestFindPrimaryFailedTask(unittest.TestCase):

    def test_no_tasks(self):
        result = find_primary_failed_task([], "")
        self.assertIsNone(result)

    def test_no_failed_tasks(self):
        tasks = [
            _make_task(1, 'FASTQC (sample1)', 'COMPLETED', exit_code=0),
            _make_task(2, 'TRIMGALORE (sample1)', 'COMPLETED', exit_code=0),
        ]
        result = find_primary_failed_task(tasks, "")
        self.assertIsNone(result)

    def test_single_failed_task(self):
        tasks = [
            _make_task(1, 'FASTQC (sample1)', 'COMPLETED', exit_code=0),
            _make_task(2, 'TRIMGALORE (sample1)', 'FAILED', exit_code=1),
        ]
        result = find_primary_failed_task(tasks, "")
        self.assertEqual(result.name, 'TRIMGALORE (sample1)')

    def test_multiple_failed_picks_earliest(self):
        tasks = [
            _make_task(1, 'FASTQC (sample1)', 'FAILED', exit_code=1),
            _make_task(2, 'TRIMGALORE (sample1)', 'FAILED', exit_code=1),
            _make_task(3, 'ALIGN (sample1)', 'FAILED', exit_code=1),
        ]
        result = find_primary_failed_task(tasks, "")
        self.assertEqual(result.name, 'FASTQC (sample1)')

    def test_log_cross_reference_exact_match(self):
        tasks = [
            _make_task(1, 'FASTQC (sample1)', 'FAILED', exit_code=1),
            _make_task(2, 'TRIMGALORE (sample1)', 'FAILED', exit_code=1),
        ]
        log = "Error executing process > 'TRIMGALORE (sample1)'"
        result = find_primary_failed_task(tasks, log)
        self.assertEqual(result.name, 'TRIMGALORE (sample1)')

    def test_log_cross_reference_partial_match(self):
        tasks = [
            _make_task(1, 'NFCORE:RNASEQ:FASTQC (sample1)', 'FAILED', exit_code=1),
            _make_task(2, 'NFCORE:RNASEQ:TRIMGALORE (sample1)', 'FAILED', exit_code=1),
        ]
        # Log mentions just "TRIMGALORE (sample1)" — partial match
        log = "Error executing process > 'TRIMGALORE (sample1)'"
        result = find_primary_failed_task(tasks, log)
        self.assertEqual(result.name, 'NFCORE:RNASEQ:TRIMGALORE (sample1)')

    def test_fallback_to_earliest_when_log_no_match(self):
        tasks = [
            _make_task(3, 'ALIGN (sample1)', 'FAILED', exit_code=1),
            _make_task(1, 'FASTQC (sample1)', 'FAILED', exit_code=1),
            _make_task(2, 'TRIMGALORE (sample1)', 'FAILED', exit_code=1),
        ]
        log = "Error executing process > 'UNKNOWN_PROCESS'"
        result = find_primary_failed_task(tasks, log)
        self.assertEqual(result.name, 'FASTQC (sample1)')

    def test_prefers_nonzero_exit_over_zero_exit(self):
        # A task with exit_code=None (aborted) should not be chosen over one
        # with exit_code=1 (actually failed)
        tasks = [
            _make_task(1, 'FASTQC (sample1)', 'FAILED', exit_code=None),
            _make_task(2, 'TRIMGALORE (sample1)', 'FAILED', exit_code=1),
        ]
        result = find_primary_failed_task(tasks, "")
        self.assertEqual(result.name, 'TRIMGALORE (sample1)')

    def test_falls_back_to_null_exit_when_no_nonzero(self):
        # All failed tasks have exit_code=None — should still return one
        tasks = [
            _make_task(1, 'FASTQC (sample1)', 'FAILED', exit_code=None),
            _make_task(2, 'TRIMGALORE (sample1)', 'FAILED', exit_code=None),
        ]
        result = find_primary_failed_task(tasks, "")
        self.assertIsNotNone(result)
        self.assertEqual(result.name, 'FASTQC (sample1)')


if __name__ == '__main__':
    unittest.main()
