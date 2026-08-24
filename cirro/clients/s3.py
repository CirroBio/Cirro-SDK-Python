import os
import threading
from pathlib import Path
from typing import Callable, Optional

from boto3 import Session
from boto3.exceptions import S3UploadFailedError
from boto3.s3.transfer import ProgressCallbackInvoker, S3Transfer, TransferConfig, create_transfer_manager
from botocore.config import Config
from botocore.exceptions import ClientError
from botocore.credentials import RefreshableCredentials
from botocore.session import get_session
from cirro_api_client.v1.models import AWSCredentials
from tqdm import tqdm

from cirro.config import Constants
from cirro.models.s3_path import S3Path

# boto3 defaults to 10 concurrent requests per transfer; the pool needs headroom
# above that for the transfer manager's submission threads and credential refresh.
# Undersizing it makes urllib3 discard connections and serializes the transfers.
_MAX_POOL_CONNECTIONS = 20


def local_filename(file_path) -> Optional[str]:
    """
    Returns the local filesystem path named by `file_path`, or None if it does not
    name a local file.

    boto3's managed transfer has to open the file itself, so it only works for real
    filesystem paths. Path-like objects backed by something else, such as an s3fs
    path, have to be streamed through their own open() instead.
    """
    try:
        filename = os.fspath(file_path)
    except TypeError:
        return None
    return filename if os.path.isfile(filename) else None


def format_creds_for_session(creds: AWSCredentials):
    return {
        'access_key': creds.access_key_id,
        'secret_key': creds.secret_access_key,
        'token': creds.session_token,
        'expiry_time': creds.expiration.isoformat()
    }


class ProgressPercentage:
    def __init__(self, progress: tqdm):
        self._lock = threading.Lock()
        self.progress = progress

    def __call__(self, bytes_amount):
        with self._lock:
            self.progress.update(bytes_amount)


class S3Client:
    def __init__(self, creds_getter: Callable[[], AWSCredentials] = None, checksum_method: str = None,
                 threads: int = Constants.default_transfer_threads):
        self._creds_getter = creds_getter
        # A single thread means no threading anywhere, so boto3 runs the transfer
        # inline rather than handing parts to its own worker pool
        self._transfer_config = TransferConfig(use_threads=threads > 1)
        self._client = self._build_session_client()
        self._manager = None
        self._transfer_lock = threading.Lock()
        self._upload_args = dict(ChecksumAlgorithm=checksum_method)
        self._download_args = dict(ChecksumMode='ENABLED') if checksum_method else dict()

    def get_aws_client(self):
        return self._client

    def upload_file(self, file_path: Path, bucket: str, key: str,
                    callback: Callable[[int], None] = None):
        """
        Uploads a file to S3, reporting transferred bytes to `callback`.

        Local files are handed to the shared transfer manager by name, which lets
        s3transfer read their parts in parallel. Any other Path-like object is
        streamed through its own open().
        """
        local_file_path = local_filename(file_path)

        if local_file_path is not None:
            self._get_transfer().upload_file(
                filename=local_file_path,
                bucket=bucket,
                key=key,
                callback=callback,
                extra_args=self._upload_args
            )
            return

        with file_path.open('rb') as file_obj:
            self._upload_fileobj(file_obj, bucket, key, callback)

    def _upload_fileobj(self, file_obj, bucket: str, key: str,
                        callback: Callable[[int], None] = None):
        """
        S3Transfer only accepts filenames, so a file object goes to the shared manager
        directly, mirroring the error translation S3Transfer would have applied.
        """
        subscribers = [ProgressCallbackInvoker(callback)] if callback else None
        future = self._get_manager().upload(file_obj, bucket, key, self._upload_args, subscribers)
        try:
            future.result()
        except ClientError as e:
            raise S3UploadFailedError(f"Failed to upload {key} to {bucket}: {e}")

    def download_file(self, local_path: Path, bucket: str, key: str,
                      callback: Callable[[int], None] = None):
        """
        Downloads a file from S3, reporting transferred bytes to `callback`.
        """
        self._get_transfer().download_file(
            bucket=bucket,
            key=key,
            filename=str(local_path.absolute()),
            callback=callback,
            extra_args=self._download_args
        )

    def _get_manager(self):
        """
        A single transfer manager, and the thread pools it owns, is shared by every
        transfer on this client rather than rebuilt for each file. It accepts both
        filenames and file objects, so both upload paths share these pools.
        """
        with self._transfer_lock:
            if self._manager is None:
                self._manager = create_transfer_manager(self._client, self._transfer_config)
            return self._manager

    def _get_transfer(self) -> S3Transfer:
        # Wrapping the shared manager costs nothing and adds boto3's error translation
        return S3Transfer(manager=self._get_manager())

    def close(self):
        """
        Shuts down the transfer manager's thread pools. Their threads are not
        daemons and are only partly reclaimed by garbage collection, so a
        long-lived process needs this to avoid accumulating them.
        """
        with self._transfer_lock:
            if self._manager is not None:
                self._manager.shutdown()
                self._manager = None

    def create_object(self, bucket: str, key: str, contents: str, content_type: str):
        self._client.put_object(
            Bucket=bucket,
            Key=key,
            ContentType=content_type,
            ContentEncoding='utf-8',
            Body=bytes(contents, 'UTF-8'),
            **self._upload_args
        )

    def get_file(self, bucket: str, key: str) -> bytes:
        resp = self._client.get_object(Bucket=bucket, Key=key, **self._download_args)
        file_body = resp['Body']
        return file_body.read()

    def get_file_stats(self, bucket: str, key: str):
        """
        https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3/client/head_object.html
        """
        return self._client.head_object(Bucket=bucket, Key=key, ChecksumMode='ENABLED')

    def get_file_listing(self, bucket: str, prefix: str) -> list[str]:
        """
        Retrieves a list of files given a prefix
        """
        return [str(f) for f in self.get_file_sizes(bucket, prefix)]

    def get_file_sizes(self, bucket: str, prefix: str) -> dict[S3Path, int]:
        """
        Retrieves a mapping of object key to size (in bytes) for all files under a prefix.
        """
        sizes = {}
        paginator = self._client.get_paginator('list_objects_v2')
        s3_pages = paginator.paginate(Bucket=bucket, Prefix=prefix)
        for page in s3_pages:
            for obj in page.get('Contents') or []:
                if obj['Size'] == 0:  # Is directory
                    continue
                sizes[S3Path(f"s3://{bucket}/{obj['Key']}")] = obj["Size"]
        return sizes

    def _build_session_client(self):
        # Use standard client if creds are not provided
        if self._creds_getter is None:
            s3_config = Config(use_dualstack_endpoint=True)
            return Session().client('s3', config=s3_config)

        creds = self._creds_getter()

        if creds.expiration:
            session = get_session()
            session._credentials = RefreshableCredentials.create_from_metadata(
                metadata=format_creds_for_session(creds),
                refresh_using=self._refresh_credentials,
                method='sts'
            )
            session = Session(botocore_session=session)
        else:
            session = Session(
                aws_access_key_id=creds.access_key_id,
                aws_secret_access_key=creds.secret_access_key,
                aws_session_token=creds.session_token
            )
        s3_config = Config(
            use_dualstack_endpoint=True,
            max_pool_connections=_MAX_POOL_CONNECTIONS
        )
        return session.client('s3', region_name=creds.region, config=s3_config)

    def _refresh_credentials(self):
        new_creds = self._creds_getter()
        return format_creds_for_session(new_creds)
