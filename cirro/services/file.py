import logging
import threading
from datetime import datetime, timezone
from functools import partial
from pathlib import Path
from typing import List, Dict

from botocore.client import BaseClient
from cirro_api_client import CirroApiClient
from cirro_api_client.v1.api.file import generate_project_file_access_token
from cirro_api_client.v1.models import AWSCredentials, ProjectAccessType

from cirro.clients.s3 import S3Client
from cirro.file_utils import upload_directory, download_directory, get_checksum
from cirro.models.file import FileAccessContext, File, PathLike
from cirro.services.base import BaseService

logger = logging.getLogger(__name__)


class FileService(BaseService):
    """
    Service for interacting with files
    """
    checksum_method: str
    transfer_retries: int
    _get_token_lock = threading.Lock()
    _read_token_cache: Dict[str, AWSCredentials] = {}

    def __init__(self, api_client, checksum_method, transfer_retries):
        """
        Instantiates the file service class
        """
        self._api_client = api_client
        self.checksum_method = checksum_method
        self.transfer_retries = transfer_retries

    def get_access_credentials(self, access_context: FileAccessContext) -> AWSCredentials:
        """
        Retrieves credentials to access files

        Args:
            access_context (cirro.models.file.FileAccessContext): File access context, use class methods to generate
        """
        access_request = access_context.file_access_request

        if access_request.access_type == ProjectAccessType.PROJECT_DOWNLOAD:
            return self._get_project_read_credentials(access_context)

        else:
            return generate_project_file_access_token.sync(
                client=self._api_client,
                project_id=access_context.project_id,
                body=access_context.file_access_request
            )

    def _get_project_read_credentials(self, access_context: FileAccessContext):
        """
        Retrieves credentials to read project data, this can be cached
        """
        access_request = access_context.file_access_request
        project_id = access_context.project_id
        with self._get_token_lock:
            cached_token = self._read_token_cache.get(project_id)

            if not cached_token or datetime.now(tz=timezone.utc) > cached_token.expiration:
                new_token = generate_project_file_access_token.sync(
                    client=self._api_client,
                    project_id=project_id,
                    body=access_request
                )

                self._read_token_cache[project_id] = new_token

        return self._read_token_cache[project_id]

    def get_aws_s3_client(self, access_context: FileAccessContext) -> BaseClient:
        """
        Gets the underlying AWS S3 client to perform operations on files

        This is seeded with refreshable credentials from the access_context parameter

        This may be used to perform advanced operations, such as CopyObject, S3 Select, etc.
        """
        s3_client = self._generate_s3_client(access_context)
        return s3_client.get_aws_client()

    def get_file(self, file: File) -> bytes:
        """
        Gets the contents of a file

        Args:
            file (cirro.models.file.File):

        Returns:
            The raw bytes of the file
        """
        return self.get_file_from_path(file.access_context, file.relative_path)

    def get_file_from_path(self, access_context: FileAccessContext, file_path: str) -> bytes:
        """
        Gets the contents of a file by providing the path, used internally

        Args:
            access_context (cirro.models.file.FileAccessContext): File access context, use class methods to generate
            file_path (str): Relative path to file within dataset

        Returns:
            The raw bytes of the file
        """
        s3_client = self._generate_s3_client(access_context)

        full_path = f'{access_context.prefix}/{file_path}'.lstrip('/')

        return s3_client.get_file(access_context.bucket, full_path)

    def create_file(self, access_context: FileAccessContext, key: str,
                    contents: str, content_type: str) -> None:
        """
        Creates a file at the specified path

        Args:
            access_context (cirro.models.file.FileAccessContext): File access context, use class methods to generate
            key (str): Key of object to create
            contents (str): Content of object
            content_type (str):
        """
        s3_client = self._generate_s3_client(access_context)

        s3_client.create_object(
            key=key,
            contents=contents,
            content_type=content_type,
            bucket=access_context.bucket
        )

    def upload_files(self,
                     access_context: FileAccessContext,
                     directory: PathLike,
                     files: List[PathLike],
                     file_path_map: Dict[PathLike, str]) -> None:
        """
        Uploads a list of files from the specified directory

        Args:
            access_context (cirro.models.file.FileAccessContext): File access context, use class methods to generate
            directory (str|Path): Path to directory
            files (typing.List[str|Path]): List of paths to files within the directory
                must be the same type as directory.
            file_path_map (typing.Dict[str|Path, str]): Optional mapping of file paths to upload
             from source path to destination path, used to "re-write" paths within the dataset.
        """
        s3_client = self._generate_s3_client(access_context)

        upload_directory(
            directory=directory,
            files=files,
            file_path_map=file_path_map,
            s3_client=s3_client,
            bucket=access_context.bucket,
            prefix=access_context.prefix,
            max_retries=self.transfer_retries
        )

    def download_files(self, access_context: FileAccessContext, directory: str, files: List[str]) -> List[Path]:
        """
        Download a list of files to the specified directory

        Args:
            access_context (cirro.models.file.FileAccessContext): File access context, use class methods to generate
            directory (str): download location
            files (List[str]): relative path of files to download
        Returns:
            List of paths to downloaded files
        """
        s3_client = self._generate_s3_client(access_context)

        return download_directory(
            directory,
            files,
            s3_client,
            access_context.bucket,
            access_context.prefix
        )

    def is_valid_file(self, file: File, local_file: Path) -> bool:
        """
        Validates the checksum of a file against a local file
        See ``validate_file`` method for details.

        Args:
            file (File): Cirro file to validate
            local_file (PathLike): Local file path to compare against

        Returns:
            bool: True if file integrity matches, False otherwise

        Raises:
            RuntimeWarning: If the remote checksum is not available or not supported
        """
        try:
            self.validate_file(file, local_file)
            return True
        except ValueError:
            return False

    def validate_file(self, file: File, local_file: PathLike):
        """
        Validates the checksum of a file against a local file
        This is used to ensure file integrity after download or upload

        Checksums might not be available if the file was uploaded without checksum support

        https://docs.aws.amazon.com/AmazonS3/latest/userguide/checking-object-integrity.html
        Args:
            file (File): Cirro file to validate
            local_file (PathLike): Local file path to compare against

        Raises:
            ValueError: If checksums do not match
            RuntimeWarning: If the remote checksum is not available or not supported
        """
        local_file = Path(local_file).expanduser() if isinstance(local_file, str) else local_file.expanduser()

        stats = self.get_file_stats(file)

        remote_checksum_key = next((prop for prop in stats.keys()
                                    if 'Checksum' in prop and prop != 'ChecksumType'), None)

        if 'ChecksumType' in stats and stats['ChecksumType'] != 'FULL_OBJECT':
            raise RuntimeWarning(f"Only 'FULL_OBJECT' checksums are supported, not {stats['ChecksumType']}")

        if remote_checksum_key is None:
            raise RuntimeWarning(f"File {file.relative_path} does not have a checksum available for validation.")

        remote_checksum = stats[remote_checksum_key]
        remote_checksum_name = remote_checksum_key.replace('Checksum', '')
        logger.debug(f"Checksum for file {file.relative_path} is {remote_checksum} using {remote_checksum_name}")

        local_checksum = get_checksum(local_file, remote_checksum_name)
        logger.debug(f"Local checksum for file {local_file} is {local_checksum} using {remote_checksum_name}")

        if local_checksum != remote_checksum:
            raise ValueError(f"Checksum mismatch for file {file.relative_path}: "
                             f"local {local_checksum}, remote {remote_checksum}")

    def get_file_stats(self, file: File) -> dict:
        """
        Gets the file stats for a file, such as size, checksum, etc.
        Equivalent to the `head_object` operation in S3
        """
        s3_client = self._generate_s3_client(file.access_context)

        full_path = f'{file.access_context.prefix}/{file.relative_path}'

        stats = s3_client.get_file_stats(
            bucket=file.access_context.bucket,
            key=full_path
        )
        logger.debug(f"File stats for file {file.relative_path} is {stats}")
        return stats

    def _generate_s3_client(self, access_context: FileAccessContext):
        """
        Generates the Cirro-S3 client to perform operations on files
        """
        return S3Client(
            partial(self.get_access_credentials, access_context),
            self.checksum_method
        )


class FileEnabledService(BaseService):
    """
    Not to be instantiated directly
    """
    _file_service: FileService

    def __init__(self, api_client: CirroApiClient, file_service: FileService):
        super().__init__(api_client)
        self._file_service = file_service
