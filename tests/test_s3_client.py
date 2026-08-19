import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import Mock, patch

from cirro.clients.s3 import S3Client


class TestS3Client(unittest.TestCase):
    def setUp(self):
        # Without a creds getter a standard client is built, which needs no credentials
        self.s3_client = S3Client()

    def test_connection_pool_covers_request_concurrency(self):
        creds = Mock(access_key_id='key', secret_access_key='secret', session_token='token',
                     region='us-west-2',
                     expiration=datetime.now(tz=timezone.utc) + timedelta(hours=1))

        # A pool smaller than boto3's 10 concurrent requests per transfer would make
        # urllib3 discard connections and serialize the transfers
        config = S3Client(creds_getter=lambda: creds).get_aws_client().meta.config
        self.assertGreater(config.max_pool_connections, 10)

    @patch('cirro.clients.s3.S3Transfer')
    def test_transfer_manager_is_reused(self, mock_transfer_class):
        with tempfile.TemporaryDirectory() as directory:
            file_path = Path(directory, 'a.txt')
            file_path.write_text('hello')

            self.s3_client.upload_file(file_path, 'bucket', 'key-1', callback=Mock())
            self.s3_client.upload_file(file_path, 'bucket', 'key-2', callback=Mock())
            self.s3_client.download_file(Path(directory, 'b.txt'), 'bucket', 'key-1', callback=Mock())

        # One manager, and the thread pools it owns, for every transfer on this client
        self.assertEqual(mock_transfer_class.call_count, 1)

    @patch('cirro.clients.s3.S3Transfer')
    def test_upload_passes_a_filename(self, mock_transfer_class):
        with tempfile.TemporaryDirectory() as directory:
            file_path = Path(directory, 'a.txt')
            file_path.write_text('hello')

            self.s3_client.upload_file(file_path, 'bucket', 'key', callback=Mock())

        # Passing an open file object instead would make s3transfer read the parts
        # serially rather than in parallel
        upload_file = mock_transfer_class.return_value.upload_file
        self.assertEqual(upload_file.call_args.kwargs['filename'], str(file_path))

    @patch('cirro.clients.s3.S3Transfer')
    def test_download_with_callback_skips_head_object(self, _mock_transfer_class):
        self.s3_client.get_file_stats = Mock()

        self.s3_client.download_file(Path('/tmp/a.txt'), 'bucket', 'key', callback=Mock())

        # The caller already knows the size, so no request is needed to look it up
        self.s3_client.get_file_stats.assert_not_called()

    @patch('cirro.clients.s3.S3Transfer')
    def test_close_shuts_down_the_transfer_manager(self, mock_transfer_class):
        with tempfile.TemporaryDirectory() as directory:
            file_path = Path(directory, 'a.txt')
            file_path.write_text('hello')
            self.s3_client.upload_file(file_path, 'bucket', 'key', callback=Mock())

            self.s3_client.close()

            # The pools use non-daemon threads that garbage collection only partly
            # reclaims, so they have to be shut down explicitly
            mock_transfer_class.return_value.__exit__.assert_called_once()

            # A later transfer must not reuse the shut-down manager
            self.s3_client.upload_file(file_path, 'bucket', 'key', callback=Mock())
        self.assertEqual(mock_transfer_class.call_count, 2)

    @patch('cirro.clients.s3.S3Transfer')
    def test_close_without_any_transfer_is_a_no_op(self, mock_transfer_class):
        self.s3_client.close()
        self.s3_client.close()
        mock_transfer_class.assert_not_called()

    def test_single_thread_disables_boto3_threading(self):
        # boto3 runs the whole transfer in the calling thread when use_threads is False
        self.assertFalse(S3Client(threads=1)._transfer_config.use_threads)
        self.assertTrue(S3Client(threads=2)._transfer_config.use_threads)
