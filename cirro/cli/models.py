from typing import TypedDict, Optional


class DownloadArguments(TypedDict):
    project: str
    dataset: str
    data_directory: str
    interactive: bool
    file: Optional[list[str]]


class UploadArguments(TypedDict):
    name: str
    description: str
    project: str
    data_type: str
    data_directory: str
    include_hidden: bool
    interactive: bool
    file: Optional[list[str]]


class ValidateArguments(TypedDict):
    dataset: str
    project: str
    data_directory: str
    interactive: bool


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
