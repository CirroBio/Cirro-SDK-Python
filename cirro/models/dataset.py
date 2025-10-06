from attrs import define as _attrs_define
from cirro_api_client.v1.models import Share, Dataset


@_attrs_define
class DatasetWithShare(Dataset):
    share: Share

    @classmethod
    def from_dataset(cls, dataset: Dataset, share: Share) -> 'DatasetWithShare':
        return cls(
            id=dataset.id,
            name=dataset.name,
            description=dataset.description,
            project_id=dataset.project_id,
            process_id=dataset.process_id,
            source_dataset_ids=dataset.source_dataset_ids,
            status=dataset.status,
            tags=dataset.tags,
            created_by=dataset.created_by,
            created_at=dataset.created_at,
            updated_at=dataset.updated_at,
            share=share
        )


@_attrs_define
class DatasetValidationResponse:
    files_matching: list[str]
    files_not_matching: list[str]
    files_missing: list[str]
    local_only_files: list[str]
    validate_errors: list[str]
