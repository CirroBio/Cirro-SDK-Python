import tempfile
import threading
import time
import unittest
from pathlib import Path
from unittest.mock import ANY, Mock, call

from boto3.exceptions import S3UploadFailedError

from cirro.file_utils import upload_directory, download_directory, get_files_in_directory, get_files_stats
from cirro.models.file import File, FileAccessContext
from cirro.models.s3_path import S3Path


class TestFileUtils(unittest.TestCase):
    def setUp(self):
        self.mock_s3_client = Mock()
        self.mock_s3_client.upload_file = Mock()
        self.test_bucket = 'project-1a1a'
        self.test_prefix = 'datasets/1a1a/data'

    def _make_files(self, directory: Path, relative_paths):
        """Create each relative path under the directory so it can be stat'd."""
        for relative_path in relative_paths:
            file_path = Path(directory, relative_path)
            file_path.parent.mkdir(parents=True, exist_ok=True)
            file_path.write_text(relative_path)

    def test_get_file_stats(self):
        directory = Path(__file__).parent / 'data'
        files = get_files_in_directory(directory)
        files = [Path(directory, f) for f in files]
        stats = get_files_stats(files=files)
        self.assertGreater(stats.size, 0)
        self.assertGreater(stats.number_of_files, 0)
        self.assertIn('KB', stats.size_friendly)

    def test_upload_directory_pathlike(self):
        with tempfile.TemporaryDirectory() as directory:
            test_path = Path(directory)
            self._make_files(test_path, ['test_file.fastq', 'folder1/test_file.fastq'])
            test_files = [
                test_path / 'test_file.fastq',
                test_path / 'folder1' / 'test_file.fastq',
            ]
            upload_directory(directory=test_path,
                             files=test_files,
                             file_path_map={},
                             s3_client=self.mock_s3_client,
                             bucket=self.test_bucket,
                             prefix=self.test_prefix)

        # The function should upload files relative to the directory path.
        self.mock_s3_client.upload_file.assert_has_calls([
            call(file_path=test_files[0], bucket=self.test_bucket,
                 key=f'{self.test_prefix}/test_file.fastq', callback=ANY),
            call(file_path=test_files[1], bucket=self.test_bucket,
                 key=f'{self.test_prefix}/folder1/test_file.fastq', callback=ANY)
        ], any_order=True)

    def test_upload_directory_string(self):
        with tempfile.TemporaryDirectory() as test_path:
            test_files = [
                'file1.txt',
                'folder1/file2.txt'
            ]
            self._make_files(Path(test_path), test_files)
            upload_directory(directory=test_path,
                             files=test_files,
                             file_path_map={},
                             s3_client=self.mock_s3_client,
                             bucket=self.test_bucket,
                             prefix=self.test_prefix)

            # The function should upload files relative to the directory path,
            # but also format file_path into the Path object.
            self.mock_s3_client.upload_file.assert_has_calls([
                call(file_path=Path(test_path, test_files[0]),
                     bucket=self.test_bucket,
                     key=f'{self.test_prefix}/file1.txt',
                     callback=ANY),
                call(file_path=Path(test_path, test_files[1]),
                     bucket=self.test_bucket,
                     key=f'{self.test_prefix}/folder1/file2.txt',
                     callback=ANY)
            ], any_order=True)

    def test_upload_directory_different_types(self):
        test_path = Path('s3://bucket/dataset1')
        test_files = [
            's3://bucket/dataset1/file2.txt'
        ]
        with self.assertRaises(ValueError):
            upload_directory(directory=test_path,
                             files=test_files,
                             file_path_map={},
                             s3_client=self.mock_s3_client,
                             bucket=self.test_bucket,
                             prefix=self.test_prefix)

    def test_upload_directory_file_map_included(self):
        test_files = [
            'file1.txt',
            'folder1/file2.txt',
            'folder1/unmapped.txt'
        ]

        file_path_map = {
            'file1.txt': 'mapped_file1.txt',
            'folder1/file2.txt': 'mapped_file2.txt'
            # unmapped file3
        }

        with tempfile.TemporaryDirectory() as test_path:
            self._make_files(Path(test_path), test_files)
            upload_directory(directory=test_path,
                             files=test_files,
                             file_path_map=file_path_map,
                             s3_client=self.mock_s3_client,
                             bucket=self.test_bucket,
                             prefix=self.test_prefix)

            # Check that upload file was called with the mapped key
            self.mock_s3_client.upload_file.assert_has_calls([
                call(file_path=Path(test_path, test_files[0]),
                     bucket=self.test_bucket,
                     key=f'{self.test_prefix}/mapped_file1.txt',
                     callback=ANY),
                call(file_path=Path(test_path, test_files[1]),
                     bucket=self.test_bucket,
                     key=f'{self.test_prefix}/mapped_file2.txt',
                     callback=ANY),
                call(file_path=Path(test_path, test_files[2]),
                     bucket=self.test_bucket,
                     key=f'{self.test_prefix}/folder1/unmapped.txt',
                     callback=ANY)
            ], any_order=True)

    def test_upload_directory_duplicate_destination(self):
        test_files = ['folder1/file.txt', 'folder2/file.txt']

        with tempfile.TemporaryDirectory() as test_path:
            self._make_files(Path(test_path), test_files)

            # Flattening both files onto one destination path cannot be honored
            with self.assertRaises(ValueError):
                upload_directory(directory=test_path,
                                 files=test_files,
                                 file_path_map={f: 'file.txt' for f in test_files},
                                 s3_client=self.mock_s3_client,
                                 bucket=self.test_bucket,
                                 prefix=self.test_prefix)

    def test_upload_directory_runs_files_concurrently(self):
        test_files = [f'file{i}.txt' for i in range(4)]
        in_flight = []
        peak_in_flight = 0
        lock = threading.Lock()

        def slow_upload(file_path, bucket, key, callback):
            nonlocal peak_in_flight
            with lock:
                in_flight.append(key)
                peak_in_flight = max(peak_in_flight, len(in_flight))
            time.sleep(0.05)
            with lock:
                in_flight.remove(key)

        self.mock_s3_client.upload_file = Mock(side_effect=slow_upload)

        with tempfile.TemporaryDirectory() as test_path:
            self._make_files(Path(test_path), test_files)
            upload_directory(directory=test_path,
                             files=test_files,
                             file_path_map={},
                             s3_client=self.mock_s3_client,
                             bucket=self.test_bucket,
                             prefix=self.test_prefix,
                             threads=4)

        self.assertGreater(peak_in_flight, 1)
        self.assertEqual(self.mock_s3_client.upload_file.call_count, 4)

    def test_upload_directory_single_thread_spawns_no_threads(self):
        test_files = [f'file{i}.txt' for i in range(4)]
        thread_names = set()

        def record_thread(file_path, bucket, key, callback):
            thread_names.add(threading.current_thread().name)

        self.mock_s3_client.upload_file = Mock(side_effect=record_thread)

        with tempfile.TemporaryDirectory() as test_path:
            self._make_files(Path(test_path), test_files)
            upload_directory(directory=test_path,
                             files=test_files,
                             file_path_map={},
                             s3_client=self.mock_s3_client,
                             bucket=self.test_bucket,
                             prefix=self.test_prefix,
                             threads=1)

        # Everything must run in the calling thread, for environments without threads
        self.assertEqual(thread_names, {threading.current_thread().name})
        self.assertEqual([t.name for t in threading.enumerate() if t.name.startswith('cirro-')], [])
        self.assertEqual(self.mock_s3_client.upload_file.call_count, 4)

    def test_upload_directory_rejects_zero_threads(self):
        with tempfile.TemporaryDirectory() as test_path:
            self._make_files(Path(test_path), ['file1.txt'])
            with self.assertRaises(ValueError):
                upload_directory(directory=test_path,
                                 files=['file1.txt'],
                                 file_path_map={},
                                 s3_client=self.mock_s3_client,
                                 bucket=self.test_bucket,
                                 prefix=self.test_prefix,
                                 threads=0)

    def test_download_directory_single_thread_spawns_no_threads(self):
        thread_names = set()

        def record_thread(local_path, bucket, key, callback):
            thread_names.add(threading.current_thread().name)

        self.mock_s3_client.download_file = Mock(side_effect=record_thread)

        with tempfile.TemporaryDirectory() as directory:
            download_directory(directory=directory,
                               files=['a.txt', 'b.txt'],
                               s3_client=self.mock_s3_client,
                               bucket=self.test_bucket,
                               prefix=self.test_prefix,
                               threads=1)

        self.assertEqual(thread_names, {threading.current_thread().name})
        self.assertEqual([t.name for t in threading.enumerate() if t.name.startswith('cirro-')], [])

    def test_upload_directory_reports_failures(self):
        test_files = ['file1.txt', 'file2.txt']
        self.mock_s3_client.upload_file = Mock(side_effect=S3UploadFailedError('denied'))

        with tempfile.TemporaryDirectory() as test_path:
            self._make_files(Path(test_path), test_files)

            with self.assertRaises(RuntimeError) as raised:
                upload_directory(directory=test_path,
                                 files=test_files,
                                 file_path_map={},
                                 s3_client=self.mock_s3_client,
                                 bucket=self.test_bucket,
                                 prefix=self.test_prefix,
                                 max_retries=1)

        # Every failed file should be named, not just the first
        self.assertIn('file1.txt', str(raised.exception))
        self.assertIn('file2.txt', str(raised.exception))

    def test_upload_directory_resume_skips_already_uploaded(self):
        directory = Path(__file__).parent / 'data' / 'example_data_1'
        uploaded = directory / 'samplesheet.csv'
        pending = directory / 'files.csv'

        self.mock_s3_client.get_file_sizes = Mock(return_value={
            S3Path(f's3://{self.test_bucket}/{self.test_prefix}/samplesheet.csv'): uploaded.stat().st_size
        })

        upload_directory(directory=directory,
                         files=[uploaded, pending],
                         file_path_map={},
                         s3_client=self.mock_s3_client,
                         bucket=self.test_bucket,
                         prefix=self.test_prefix,
                         resume=True)

        # Only the file not already present should be uploaded
        self.mock_s3_client.upload_file.assert_called_once_with(
            file_path=pending,
            bucket=self.test_bucket,
            key=f'{self.test_prefix}/files.csv',
            callback=ANY)

    def test_upload_directory_resume_reuploads_size_mismatch(self):
        directory = Path(__file__).parent / 'data' / 'example_data_1'
        partial = directory / 'samplesheet.csv'

        self.mock_s3_client.get_file_sizes = Mock(return_value={
            S3Path(f's3://{self.test_bucket}/{self.test_prefix}/samplesheet.csv'): partial.stat().st_size - 1  # remote is incomplete
        })

        upload_directory(directory=directory,
                         files=[partial],
                         file_path_map={},
                         s3_client=self.mock_s3_client,
                         bucket=self.test_bucket,
                         prefix=self.test_prefix,
                         resume=True)

        # A modified file (different size) should be reuploaded
        self.mock_s3_client.upload_file.assert_called_once_with(
            file_path=partial,
            bucket=self.test_bucket,
            key=f'{self.test_prefix}/samplesheet.csv',
            callback=ANY)

    def test_upload_directory_resume_disabled_ignores_remote(self):
        directory = Path(__file__).parent / 'data' / 'example_data_1'
        file1 = directory / 'samplesheet.csv'
        file2 = directory / 'files.csv'

        upload_directory(directory=directory,
                         files=[file1, file2],
                         file_path_map={},
                         s3_client=self.mock_s3_client,
                         bucket=self.test_bucket,
                         prefix=self.test_prefix)

        # Should not get file sizes and should upload all files.
        self.mock_s3_client.get_file_sizes.assert_not_called()
        self.mock_s3_client.upload_file.assert_has_calls([
            call(file_path=file1, bucket=self.test_bucket,
                 key=f'{self.test_prefix}/samplesheet.csv', callback=ANY),
            call(file_path=file2, bucket=self.test_bucket,
                 key=f'{self.test_prefix}/files.csv', callback=ANY),
        ], any_order=True)

    def _make_dataset_file(self, relative_path: str, size: int) -> File:
        access_context = FileAccessContext.download(project_id='project-1a1a',
                                                    base_url=f's3://{self.test_bucket}/{self.test_prefix}')
        return File(relative_path=relative_path,
                    size=size,
                    access_context=access_context,
                    metadata={})

    def test_download_directory_uses_known_sizes(self):
        files = [self._make_dataset_file('file1.txt', 10),
                 self._make_dataset_file('folder1/file2.txt', 20)]

        with tempfile.TemporaryDirectory() as directory:
            local_paths = download_directory(directory=directory,
                                             files=files,
                                             s3_client=self.mock_s3_client,
                                             bucket=self.test_bucket,
                                             prefix=self.test_prefix,
                                             threads=2)

            # Sizes come from the dataset, so no request is needed to look them up
            self.mock_s3_client.get_file_stats.assert_not_called()

            # Returned paths keep the order they were requested in
            self.assertEqual(local_paths, [Path(directory, 'file1.txt'),
                                           Path(directory, 'folder1/file2.txt')])

            # Parent directories are created before the transfer
            self.assertTrue(Path(directory, 'folder1').is_dir())

        self.mock_s3_client.download_file.assert_has_calls([
            call(local_path=local_paths[0], bucket=self.test_bucket,
                 key=f'{self.test_prefix}/file1.txt', callback=ANY),
            call(local_path=local_paths[1], bucket=self.test_bucket,
                 key=f'{self.test_prefix}/folder1/file2.txt', callback=ANY),
        ], any_order=True)
