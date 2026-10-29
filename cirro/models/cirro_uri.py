from typing import Any, List, NamedTuple, Optional

_SCHEME = 'cirro:'
_DATASET_TYPE = 'dataset'


class CirroUri(NamedTuple):
    """
    A parsed `cirro:<tenant_id>:<project_id>:<type>:<id>/<path>` reference
    """
    tenant_id: str
    project_id: str
    type: str
    id: str
    path: Optional[str]
    """Relative to the dataset's `data/` directory, `''` for the whole directory, or None for the dataset itself"""


def format_cirro_uri(project_id: str, dataset_id: str, path: Optional[str] = None) -> str:
    """
    Builds a Cirro URI referencing a dataset, or a file or folder within it.

    This is the value file and folder columns of a structured input
    (and path parameters) expect, rather than a local or S3 path.

    Args:
        project_id (str): ID of the project the dataset is in
        dataset_id (str): ID of the dataset
        path (str): Path of a file or folder, relative to the dataset's `data/` directory.
         Use `''` for the dataset's whole data directory, or omit it to reference the dataset itself.

    ```python
    format_cirro_uri("project-id", "dataset-id", "sample1_R1.fastq.gz")
    # 'cirro::project-id:dataset:dataset-id/sample1_R1.fastq.gz'
    ```
    """
    head = f'{_SCHEME}:{project_id}:{_DATASET_TYPE}:{dataset_id}'
    if path is None:
        return head
    return f'{head}/{path.removeprefix("data/")}'


def parse_cirro_uri(value: Any) -> Optional[CirroUri]:
    """
    Parses a Cirro URI referencing a dataset, or a file or folder within it,
    returning None for anything else.
    """
    if not isinstance(value, str) or not value.startswith(_SCHEME):
        return None
    head, slash, path = value[len(_SCHEME):].partition('/')
    parts = head.split(':')
    if len(parts) != 4:
        return None
    tenant_id, project_id, uri_type, resource_id = parts
    if uri_type not in _DATASET_TYPE or not resource_id:
        return None
    return CirroUri(tenant_id, project_id, uri_type, resource_id, path if slash else None)


def find_dataset_ids(value: Any, project_id: Optional[str] = None) -> List[str]:
    """
    The datasets referenced by any Cirro URI within a value (e.g. analysis parameters,
    or structured input rows), in the order they first appear.

    Args:
        value: String, list, or dict to search
        project_id (str): If given, only count URIs from this project
    """
    ids = {}

    def collect(item: Any):
        if isinstance(item, str):
            uri = parse_cirro_uri(item.strip())
            if uri and (project_id is None or uri.project_id == project_id):
                ids[uri.id] = None
        elif isinstance(item, dict):
            for v in item.values():
                collect(v)
        elif isinstance(item, (list, tuple)):
            for v in item:
                collect(v)

    collect(value)
    return list(ids)
