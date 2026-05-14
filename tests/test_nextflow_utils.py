import unittest
from unittest.mock import MagicMock

from cirro.sdk.nextflow_utils import parse_inputs_from_command_run, find_primary_failed_task


def _make_task(task_id, name, status, exit_code=None):
    """Build a minimal DataPortalTask-like mock."""
    task = MagicMock()
    task.task_id = task_id
    task.name = name
    task.status = status
    task.exit_code = exit_code
    return task


class TestParseInputsFromCommandRun(unittest.TestCase):

    def test_basic_s3_copy(self):
        content = "aws s3 cp s3://my-bucket/path/to/file.bam ./file.bam\n"
        result = parse_inputs_from_command_run(content)
        self.assertEqual(result, ['s3://my-bucket/path/to/file.bam'])

    def test_with_only_show_errors_flag(self):
        content = "aws s3 cp --only-show-errors s3://my-bucket/data/sample.fastq.gz ./sample.fastq.gz\n"
        result = parse_inputs_from_command_run(content)
        self.assertEqual(result, ['s3://my-bucket/data/sample.fastq.gz'])

    def test_multiple_flags(self):
        content = "aws s3 cp --quiet --no-progress s3://bucket/work/ab/cdef/reads.bam ./reads.bam\n"
        result = parse_inputs_from_command_run(content)
        self.assertEqual(result, ['s3://bucket/work/ab/cdef/reads.bam'])

    def test_multiple_files(self):
        content = (
            "aws s3 cp --only-show-errors s3://bucket/data/r1.fastq.gz ./r1.fastq.gz\n"
            "aws s3 cp --only-show-errors s3://bucket/data/r2.fastq.gz ./r2.fastq.gz\n"
        )
        result = parse_inputs_from_command_run(content)
        self.assertEqual(result, [
            's3://bucket/data/r1.fastq.gz',
            's3://bucket/data/r2.fastq.gz',
        ])

    def test_no_s3_lines(self):
        content = "#!/bin/bash\nset -e\necho hello\n"
        result = parse_inputs_from_command_run(content)
        self.assertEqual(result, [])

    def test_empty_string(self):
        result = parse_inputs_from_command_run('')
        self.assertEqual(result, [])

    def test_ignores_upload_lines(self):
        # aws s3 cp in the other direction (local → s3) should not be captured
        content = "aws s3 cp ./output.bam s3://bucket/results/output.bam\n"
        result = parse_inputs_from_command_run(content)
        self.assertEqual(result, [])


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
