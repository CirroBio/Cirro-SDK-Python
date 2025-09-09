from typing import TypedDict, List, Optional


class DownloadArguments(TypedDict):
    project: str
    dataset: str
    data_directory: str
    interactive: bool


class UploadArguments(TypedDict):
    name: str
    description: str
    project: str
    data_type: str
    data_directory: str
    include_hidden: bool
    interactive: bool
    files: Optional[List[str]]


class ListArguments(TypedDict):
    project: str
    interactive: bool


class CreatePipelineConfigArguments(TypedDict):
    pipeline_dir: str
    output_dir: str
    entrypoint: Optional[str]
    interactive: bool


class UploadReferenceArguments(TypedDict):
    name: str
    reference_type: str
    project: str
    reference_file: list[str]
    interactive: bool
