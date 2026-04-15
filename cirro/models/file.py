from dataclasses import dataclass
from pathlib import PurePath, Path
from typing import Dict, Optional, TypeVar, NamedTuple

from cirro_api_client.v1.models import ProjectFileAccessRequest, ProjectAccessType, FileEntry, DatasetDetail

from cirro.models.s3_path import S3Path

PathLike = TypeVar('PathLike', str, Path)


class DirectoryStatistics(NamedTuple):
    size: float
    " Size in bytes"
    size_friendly: str
    " Size in user friendly format (e.g. 1.2 KB)"
    number_of_files: int
    " Number of files"


class FileAccessContext:
    """
    Context holder for accessing various files in Cirro and abstracting out their location.
    Prefer to use the class methods to instantiate.
    """
    def __init__(self,
                 file_access_request: ProjectFileAccessRequest,
                 project_id: str,
                 base_url: str):
        self.file_access_request = file_access_request
        self.base_url = base_url
        self.project_id = project_id
        self._s3_path = S3Path(base_url)

    @classmethod
    def download(cls, project_id: str, base_url: str, token_lifetime_override: int = None):
        return cls(
            file_access_request=ProjectFileAccessRequest(
                access_type=ProjectAccessType.PROJECT_DOWNLOAD,
                token_lifetime_hours=token_lifetime_override
            ),
            base_url=base_url,
            project_id=project_id
        )

    @classmethod
    def download_shared_dataset(cls, project_id: str, dataset_id: str,
                                base_url: str, token_lifetime_override: int = None):
        return cls(
            file_access_request=ProjectFileAccessRequest(
                access_type=ProjectAccessType.SHARED_DATASET_DOWNLOAD,
                dataset_id=dataset_id,
                token_lifetime_hours=token_lifetime_override
            ),
            base_url=base_url,
            project_id=project_id
        )

    @classmethod
    def upload_dataset(cls, project_id: str, dataset_id: str, base_url: str, token_lifetime_override: int = None):
        return cls(
            file_access_request=ProjectFileAccessRequest(
                access_type=ProjectAccessType.DATASET_UPLOAD,
                dataset_id=dataset_id,
                token_lifetime_hours=token_lifetime_override
            ),
            base_url=f'{base_url}/data',
            project_id=project_id
        )

    @classmethod
    def upload_reference(cls, project_id: str, base_url: str):
        return cls(
            file_access_request=ProjectFileAccessRequest(
                access_type=ProjectAccessType.REFERENCE_UPLOAD
            ),
            base_url=base_url,
            project_id=project_id
        )

    @classmethod
    def upload_sample_sheet(cls, project_id: str, dataset_id: str, base_url: str):
        return cls(
            file_access_request=ProjectFileAccessRequest(
                access_type=ProjectAccessType.SAMPLESHEET_UPLOAD,
                dataset_id=dataset_id
            ),
            base_url=f'{base_url}/data',
            project_id=project_id
        )

    @property
    def bucket(self) -> str:
        """ S3 Bucket """
        return self._s3_path.bucket

    @property
    def prefix(self) -> str:
        """ S3 Prefix """
        return self._s3_path.key

    def __repr__(self):
        return f'{self.__class__.__name__}({self.file_access_request.access_type}@base_url={self.base_url})'


@dataclass(frozen=True)
class File:
    relative_path: str
    size: int
    access_context: FileAccessContext
    metadata: Optional[Dict] = None

    @classmethod
    def from_file_entry(cls, file: FileEntry, project_id: str, dataset: DatasetDetail = None, domain: str = None):
        # Path is absolute rather than relative
        if 's3://' in file.path:
            parts = S3Path(file.path)
            domain = parts.base
            path = parts.key
        else:
            path = file.path

        if dataset and dataset.share:
            access_context = FileAccessContext.download_shared_dataset(
                project_id=project_id,
                dataset_id=dataset.id,
                base_url=domain
            )
        else:
            access_context = FileAccessContext.download(
                project_id=project_id,
                base_url=domain
            )

        return cls(
            relative_path=path,
            metadata=file.metadata.additional_properties if file.metadata else {},
            size=file.size,
            access_context=access_context
        )

    @property
    def normalized_path(self) -> str:
        """ Without the data prefix """
        return self.relative_path[len("data/"):]

    @property
    def absolute_path(self):
        return f'{self.access_context.base_url}/{self.relative_path.strip("/")}'

    @property
    def name(self):
        return PurePath(self.absolute_path).name

    def __repr__(self):
        return f'{self.__class__.__name__}(path={self.relative_path})'
