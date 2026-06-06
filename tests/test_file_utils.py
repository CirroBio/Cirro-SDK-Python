import unittest
from pathlib import Path
from unittest.mock import Mock, call

from cirro.file_utils import upload_directory, get_files_in_directory, get_files_stats


class TestFileUtils(unittest.TestCase):
    def setUp(self):
        self.mock_s3_client = Mock()
        self.mock_s3_client.upload_file = Mock()
        self.test_bucket = 'project-1a1a'
        self.test_prefix = 'datasets/1a1a/data'

    def test_get_file_stats(self):
        directory = Path(__file__).parent / 'data'
        files = get_files_in_directory(directory)
        files = [Path(directory, f) for f in files]
        stats = get_files_stats(files=files)
        self.assertGreater(stats.size, 0)
        self.assertGreater(stats.number_of_files, 0)
        self.assertIn('KB', stats.size_friendly)

    def test_upload_directory_pathlike(self):
        test_path = Path('/Users/test/Documents/dataset1')
        test_files = [
            Path('/Users/test/Documents/dataset1/test_file.fastq'),
            Path('/Users/test/Documents/dataset1/folder1/test_file.fastq'),
        ]
        upload_directory(directory=test_path,
                         files=test_files,
                         file_path_map={},
                         s3_client=self.mock_s3_client,
                         bucket=self.test_bucket,
                         prefix=self.test_prefix)

        # The function should upload files relative to the directory path.
        self.mock_s3_client.upload_file.assert_has_calls([
            call(file_path=test_files[0], bucket=self.test_bucket, key=f'{self.test_prefix}/test_file.fastq'),
            call(file_path=test_files[1], bucket=self.test_bucket, key=f'{self.test_prefix}/folder1/test_file.fastq')
        ], any_order=True)

    def test_upload_directory_string(self):
        test_path = 'data'
        test_files = [
            'file1.txt',
            'folder1/file2.txt'
        ]
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
                 key=f'{self.test_prefix}/file1.txt'),
            call(file_path=Path(test_path, test_files[1]),
                 bucket=self.test_bucket,
                 key=f'{self.test_prefix}/folder1/file2.txt')
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
        test_path = 'data'
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
                 key=f'{self.test_prefix}/mapped_file1.txt'),
            call(file_path=Path(test_path, test_files[1]),
                 bucket=self.test_bucket,
                 key=f'{self.test_prefix}/mapped_file2.txt'),
            call(file_path=Path(test_path, test_files[2]),
                 bucket=self.test_bucket,
                 key=f'{self.test_prefix}/folder1/unmapped.txt')
        ], any_order=True)

    def test_upload_directory_resume_skips_already_uploaded(self):
        directory = Path(__file__).parent / 'data' / 'example_data_1'
        uploaded = directory / 'samplesheet.csv'
        pending = directory / 'files.csv'

        self.mock_s3_client.get_file_sizes = Mock(return_value={
            f'{self.test_prefix}/samplesheet.csv': uploaded.stat().st_size
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
            key=f'{self.test_prefix}/files.csv')

    def test_upload_directory_resume_reuploads_size_mismatch(self):
        directory = Path(__file__).parent / 'data' / 'example_data_1'
        partial = directory / 'samplesheet.csv'

        self.mock_s3_client.get_file_sizes = Mock(return_value={
            f'{self.test_prefix}/samplesheet.csv': partial.stat().st_size - 1  # remote is incomplete
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
            key=f'{self.test_prefix}/samplesheet.csv')

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
            call(file_path=file1, bucket=self.test_bucket, key=f'{self.test_prefix}/samplesheet.csv'),
            call(file_path=file2, bucket=self.test_bucket, key=f'{self.test_prefix}/files.csv'),
        ], any_order=True)

