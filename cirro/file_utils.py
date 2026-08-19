import base64
import os
import random
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from functools import partial
from pathlib import Path, PurePath
from typing import Callable, List, Union, Dict

from boto3.exceptions import RetriesExceededError, S3UploadFailedError
from botocore.exceptions import ConnectionError
from tqdm import tqdm

from cirro.clients import S3Client
from cirro.clients.s3 import ProgressPercentage
from cirro.config import Constants
from cirro.models.file import DirectoryStatistics, File, PathLike
from cirro.models.s3_path import S3Path

if os.name == 'nt':
    import win32api
    import win32con

# Transient failures worth another attempt. Anything else (a missing object,
# denied access) will not succeed on a retry and is surfaced immediately.
_RETRYABLE_ERRORS = (S3UploadFailedError, RetriesExceededError, ConnectionError)

# Ceiling on the wait between attempts. botocore already retries throttling and
# transient 5xx internally, so this outer loop only needs to cover longer outages.
_MAX_RETRY_DELAY = 60

_PROGRESS_BAR_FORMAT = "{desc} | {percentage:.1f}%|{bar:25} | {rate_fmt}"


def filter_files_by_pattern(files: Union[List[File], List[str]], pattern: str) -> Union[List[File], List[str]]:
    """
    Filters a list of files by a glob pattern

    Args:
        files (Union[List[File], List[str]]): List of Files or file paths
        pattern (str): Glob pattern (i.e., *.fastq)

    Returns:
        The filtered list of files
    """
    def matches_glob(file: Union[File, str]):
        return PurePath(file if isinstance(file, str) else file.relative_path).match(pattern)

    return [
        file for file in files
        if matches_glob(file)
    ]


def generate_flattened_file_map(files: List[PathLike]) -> Dict[PathLike, str]:
    """
    Generates a mapping of file paths "flattened" to their base name.

    Example:  data1/sample1.fastq.gz -> sample1.fastq.gz

    Args:
        files: List[PathLike]: List of file paths

    Returns:
        Dict[PathLike, str]: Mapping of file paths to their base name
    """
    return {
        file: Path(file).name for file in files
    }


def is_hidden_file(file_path: Path):
    """
    Check if a file path is hidden
    Such as desktop.ini, .DS_Store, etc.
    """
    if os.name == 'nt':
        attributes = win32api.GetFileAttributes(str(file_path))
        return attributes & (win32con.FILE_ATTRIBUTE_HIDDEN | win32con.FILE_ATTRIBUTE_SYSTEM)
    else:
        return file_path.name.startswith('.')


def get_files_in_directory(
        directory: Union[str, Path],
        include_hidden=False
) -> List[str]:
    """
    Returns a list of strings containing the relative path of
    each file within the indicated directory.

    Args:
        directory (Union[str, Path]): The path to the directory
        include_hidden (bool): include hidden files in the returned list

    Returns:
        List of files in the directory
    """
    path = Path(directory).expanduser()
    path_posix = str(path.as_posix())

    paths = []

    for file_path in path.rglob("*"):
        if file_path.is_dir():
            continue

        if not include_hidden and is_hidden_file(file_path):
            continue

        if not file_path.exists():
            continue

        str_file_path = str(file_path.as_posix())
        str_file_path = str_file_path.replace(f'{path_posix}/', "")
        paths.append(str_file_path)

    paths.sort()
    return paths


def bytes_to_human_readable(num_bytes: int) -> str:
    for unit in ['B', 'KB', 'MB', 'GB', 'TB', 'PB']:
        if num_bytes < 1000.0 or unit == 'PB':
            break
        num_bytes /= 1000.0
    return f"{num_bytes:,.2f} {unit}"


def get_files_stats(files: List[PathLike]) -> DirectoryStatistics:
    """
    Returns information about the list of files provided, such as the total size and number of files.
    """
    sizes = [f.stat().st_size for f in files]
    total_size = sum(sizes)
    return DirectoryStatistics(
        size_friendly=bytes_to_human_readable(total_size),
        size=total_size,
        number_of_files=len(sizes)
    )


def _run_transfers(transfers: Dict[str, Callable[..., None]],
                   progress: tqdm,
                   threads: int,
                   max_retries: int,
                   action: str):
    """
    @private

    Runs the given transfers, retrying transient failures and reporting every failure
    rather than letting the first one hide the rest. A single thread runs them one at
    a time in the calling thread, spawning nothing.
    """
    if threads < 1:
        raise ValueError(f"threads must be at least 1, got {threads}")

    # One callback shared by every worker, so its lock serializes bar updates
    callback = ProgressPercentage(progress)
    attempts = max(max_retries, 1)

    def transfer_with_retry(transfer: Callable[..., None], description: str):
        # Waiting between attempts only occupies this worker, leaving any workers
        # transferring other files unaffected
        for attempt in range(attempts):
            try:
                return transfer(callback=callback)
            except _RETRYABLE_ERRORS as e:
                if attempt == attempts - 1:
                    raise
                delay = min(2 ** attempt, _MAX_RETRY_DELAY) + random.uniform(0, 1)
                progress.write(f"Encountered error transferring {description}:\n{str(e)}\n"
                               f"Retrying in {delay:.0f} seconds "
                               f"({attempts - (attempt + 1)} attempts remaining)")
                time.sleep(delay)

    errors: Dict[str, Exception] = {}

    if threads == 1:
        for description, transfer in transfers.items():
            try:
                transfer_with_retry(transfer, description)
            except Exception as e:
                errors[description] = e
    else:
        with ThreadPoolExecutor(max_workers=threads, thread_name_prefix=f'cirro-{action}') as executor:
            futures = {executor.submit(transfer_with_retry, transfer, description): description
                       for description, transfer in transfers.items()}
            for future in as_completed(futures):
                try:
                    future.result()
                except Exception as e:
                    # Collected so one bad file does not hide the outcome of the others
                    errors[futures[future]] = e

    if errors:
        detail = '\n'.join(f'  {description}: {error}' for description, error in errors.items())
        raise RuntimeError(f"Failed to {action} {len(errors)} of {len(transfers)} files:\n{detail}")


def upload_directory(directory: PathLike,
                     files: List[PathLike],
                     file_path_map: Dict[PathLike, str],
                     s3_client: S3Client,
                     bucket: str,
                     prefix: str,
                     max_retries=10,
                     resume=False,
                     threads=Constants.default_transfer_threads):
    """
    @private

    Uploads a list of files from the specified directory
    Args:
        directory (str|Path): Path to directory
        files (typing.List[str|Path]): List of paths to files within the directory
            must be the same type as directory.
        file_path_map (typing.Dict[str|Path, str]): Map of file paths from source to destination
        s3_client (cirro.clients.S3Client): S3 client
        bucket (str): S3 bucket
        prefix (str): S3 prefix
        max_retries (int): Number of retries
        resume (bool): Skip files that are already present in S3 under the prefix
        threads (int): Number of files to upload at once. 1 disables threading.

    Raises:
        RuntimeError: If any file could not be uploaded
    """
    # Ensure all files are of the same type as the directory
    if not all(isinstance(file, type(directory)) for file in files):
        raise ValueError("All files must be of the same type as the directory (str or Path)")

    # When resuming, map the files already uploaded to their size so completed
    # uploads can be skipped while half-uploaded files are re-sent.
    # List with a trailing slash so the request prefix falls within the
    # ListBucket s3:prefix condition on the vended upload credentials.
    already_uploaded = s3_client.get_file_sizes(bucket, f'{prefix}/') if resume else {}

    transfers: Dict[str, Callable[..., None]] = {}
    seen_keys = set()
    total_size = 0

    for file in files:
        if isinstance(file, str):
            file_path = Path(directory, file)
        else:
            file_path = file

        # Check if is present in the file_path_map
        # if it is, use the mapped value as the destination path
        if file in file_path_map:
            file_relative = file_path_map[file]
        else:
            file_relative = file_path.relative_to(directory).as_posix()

        key = f'{prefix}/{file_relative}'
        file_size = file_path.stat().st_size

        # Two sources mapping to one destination cannot both survive, and uploading
        # them concurrently would make the winner arbitrary.
        if key in seen_keys:
            raise ValueError(f"Multiple files map to the same destination path: {file_relative}")
        seen_keys.add(key)

        # When resuming, skip files already uploaded with a matching size.
        # A size mismatch implies a modified file, so re-upload the file.
        expected_path = S3Path(f"s3://{bucket}/{key}")
        if resume and already_uploaded.get(expected_path) == file_size:
            print(f"Uploading {file_relative} skipped as it has already been uploaded.")
            continue

        transfers[key] = partial(s3_client.upload_file, file_path=file_path, bucket=bucket, key=key)
        total_size += file_size

    with tqdm(total=total_size,
              desc=f'Uploading {len(transfers)} files ({bytes_to_human_readable(total_size)})',
              bar_format=_PROGRESS_BAR_FORMAT,
              unit='B', unit_scale=True,
              unit_divisor=1024) as progress:
        _run_transfers(transfers, progress, threads, max_retries, 'upload')


def download_directory(directory: str,
                       files: Union[List[File], List[str]],
                       s3_client: S3Client,
                       bucket: str,
                       prefix: str,
                       max_retries=10,
                       threads=Constants.default_transfer_threads) -> List[Path]:
    """
    @private

    Raises:
        RuntimeError: If any file could not be downloaded
    """
    relative_paths = [file if isinstance(file, str) else file.relative_path for file in files]
    # File objects carry their size from the dataset listing; plain paths do not
    total_size = None if any(isinstance(f, str) for f in files) else sum(f.size for f in files)

    # Resolved up front so the returned order always matches the order requested
    local_paths = [Path(directory, relative_path).expanduser() for relative_path in relative_paths]
    for parent in {local_path.parent for local_path in local_paths}:
        parent.mkdir(parents=True, exist_ok=True)

    transfers: Dict[str, Callable[..., None]] = {
        relative_path: partial(s3_client.download_file,
                               local_path=local_path,
                               bucket=bucket,
                               key=f'{prefix}/{relative_path}'.lstrip('/'))
        for relative_path, local_path in zip(relative_paths, local_paths)
    }

    # Without a total there is no percentage to render
    bar_format = _PROGRESS_BAR_FORMAT if total_size is not None else "{desc} | {n_fmt} | {rate_fmt}"
    total_described = f' ({bytes_to_human_readable(total_size)})' if total_size is not None else ''

    with tqdm(total=total_size,
              desc=f'Downloading {len(files)} files{total_described}',
              bar_format=bar_format,
              unit='B', unit_scale=True, unit_divisor=1024) as progress:
        _run_transfers(transfers, progress, threads, max_retries, 'download')

    return local_paths


def get_checksum(file: Path, checksum_name: str, chunk_size=1024 * 1024) -> str:
    from awscrt import checksums
    checksum_func_map = {
        'CRC32': checksums.crc32,
        'CRC32C': checksums.crc32c,
        'CRC64NVME': checksums.crc64nvme
    }

    checksum_func = checksum_func_map.get(checksum_name)
    if checksum_func is None:
        raise RuntimeWarning(f"Unsupported checksum type: {checksum_name}")

    crc = 0
    with file.open("rb") as f:
        while True:
            chunk = f.read(chunk_size)
            if not chunk:
                break
            crc = checksum_func(chunk, crc)

    byte_length = 8 if checksum_name == 'CRC64NVME' else 4
    checksum_bytes = crc.to_bytes(byte_length, byteorder='big')
    return base64.b64encode(checksum_bytes).decode('utf-8')
