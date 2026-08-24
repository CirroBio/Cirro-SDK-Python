import io
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import Mock, patch

from cirro.clients.s3 import S3Client, local_filename


class RemotePath:
    """
    Stands in for a Path-like object backed by something other than the local
    filesystem, such as an s3fs path (see #115).
    """

    def __init__(self, uri: str, contents: bytes = b'remote'):
        self.uri = uri
        self.contents = contents

    def __fspath__(self):
        return self.uri

    def open(self, mode='rb'):
        return io.BytesIO(self.contents)


class StreamOnlyPath:
    """A Path-like object that only supports open(), with no filesystem path."""

    def open(self, mode='rb'):
        return io.BytesIO(b'stream')


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

    @patch('cirro.clients.s3.create_transfer_manager')
    def test_transfer_manager_is_reused(self, mock_create_manager):
        with tempfile.TemporaryDirectory() as directory:
            file_path = Path(directory, 'a.txt')
            file_path.write_text('hello')

            self.s3_client.upload_file(file_path, 'bucket', 'key-1', callback=Mock())
            self.s3_client.upload_file(file_path, 'bucket', 'key-2', callback=Mock())
            self.s3_client.download_file(Path(directory, 'b.txt'), 'bucket', 'key-1', callback=Mock())

        # One manager, and the thread pools it owns, for every transfer on this client
        self.assertEqual(mock_create_manager.call_count, 1)

    @patch('cirro.clients.s3.create_transfer_manager')
    def test_upload_passes_a_filename(self, mock_create_manager):
        with tempfile.TemporaryDirectory() as directory:
            file_path = Path(directory, 'a.txt')
            file_path.write_text('hello')

            self.s3_client.upload_file(file_path, 'bucket', 'key', callback=Mock())

        # Passing an open file object instead would make s3transfer read the parts
        # serially rather than in parallel
        upload = mock_create_manager.return_value.upload
        self.assertEqual(upload.call_args.args[0], str(file_path))

    @patch('cirro.clients.s3.create_transfer_manager')
    def test_download_with_callback_skips_head_object(self, _mock_create_manager):
        self.s3_client.get_file_stats = Mock()

        self.s3_client.download_file(Path('/tmp/a.txt'), 'bucket', 'key', callback=Mock())

        # The caller already knows the size, so no request is needed to look it up
        self.s3_client.get_file_stats.assert_not_called()

    @patch('cirro.clients.s3.create_transfer_manager')
    def test_close_shuts_down_the_transfer_manager(self, mock_create_manager):
        with tempfile.TemporaryDirectory() as directory:
            file_path = Path(directory, 'a.txt')
            file_path.write_text('hello')
            self.s3_client.upload_file(file_path, 'bucket', 'key', callback=Mock())

            self.s3_client.close()

            # The pools use non-daemon threads that garbage collection only partly
            # reclaims, so they have to be shut down explicitly
            mock_create_manager.return_value.shutdown.assert_called_once()

            # A later transfer must not reuse the shut-down manager
            self.s3_client.upload_file(file_path, 'bucket', 'key', callback=Mock())
        self.assertEqual(mock_create_manager.call_count, 2)

    @patch('cirro.clients.s3.create_transfer_manager')
    def test_close_without_any_transfer_is_a_no_op(self, mock_create_manager):
        self.s3_client.close()
        self.s3_client.close()
        mock_create_manager.assert_not_called()

    def test_single_thread_disables_boto3_threading(self):
        # boto3 runs the whole transfer in the calling thread when use_threads is False
        self.assertFalse(S3Client(threads=1)._transfer_config.use_threads)
        self.assertTrue(S3Client(threads=2)._transfer_config.use_threads)

    def test_local_filename_only_matches_real_local_files(self):
        with tempfile.TemporaryDirectory() as directory:
            existing = Path(directory, 'a.txt')
            existing.write_text('hello')

            self.assertIsNotNone(local_filename(existing))
            self.assertIsNotNone(local_filename(str(existing)))
            self.assertIsNone(local_filename(Path(directory, 'missing.txt')))
            self.assertIsNone(local_filename(Path(directory)))
            self.assertIsNone(local_filename(RemotePath('s3://bucket/key')))
            self.assertIsNone(local_filename(StreamOnlyPath()))

    @patch('cirro.clients.s3.create_transfer_manager')
    def test_local_file_uses_the_transfer_manager(self, mock_create_manager):
        with tempfile.TemporaryDirectory() as directory:
            file_path = Path(directory, 'a.txt')
            file_path.write_text('hello')
            self.s3_client.upload_file(file_path, 'bucket', 'key', callback=Mock())

        # A filename goes through S3Transfer, which reads the parts in parallel
        upload = mock_create_manager.return_value.upload
        upload.assert_called_once()
        self.assertEqual(upload.call_args.args[0], str(file_path))

    @patch('cirro.clients.s3.create_transfer_manager')
    def test_remote_path_is_streamed_through_its_own_open(self, mock_create_manager):
        # The transfer manager can only open real files, so a Path-like object backed
        # by another filesystem has to be streamed instead (#115)
        uploaded = []

        def capture(stream, *args, **kwargs):
            # Read while the stream is still open; upload_file closes it on the way out
            uploaded.append(stream.read())
            return Mock()

        mock_create_manager.return_value.upload = Mock(side_effect=capture)

        self.s3_client.upload_file(RemotePath('s3://bucket/source.txt'), 'bucket', 'key',
                                   callback=Mock())

        # Streamed through the same shared manager, not a per-file one
        self.assertEqual(uploaded, [b'remote'])
        self.assertEqual(mock_create_manager.call_count, 1)

    @patch('cirro.clients.s3.create_transfer_manager')
    def test_stream_only_path_is_supported(self, mock_create_manager):
        self.s3_client.upload_file(StreamOnlyPath(), 'bucket', 'key', callback=Mock())

        mock_create_manager.return_value.upload.assert_called_once()
