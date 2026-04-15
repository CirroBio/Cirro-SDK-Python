from typing import List

from cirro_api_client.v1.models import Reference

from cirro.cirro_client import CirroApi
from cirro.models.file import File
from cirro.sdk.asset import DataPortalAssets, DataPortalAsset
from cirro.sdk.file import DataPortalFile


class DataPortalReference(DataPortalAsset):
    """
    Reference data object containing files which can be used for analysis in a particular project.
    """
    def __init__(self, ref: Reference, project_id: str, client: CirroApi):
        """
        Instantiate by listing the references which have been added to a particular project
        ```python
        from cirro import DataPortal()
        portal = DataPortal()
        project = portal.get_project_by_name("Project Name")
        references = project.list_references()
        ```
        """
        self._data = ref
        self._files = [
            DataPortalFile(File.from_file_entry(f, project_id), client) for f in ref.files
        ]

    @property
    def files(self) -> List[DataPortalFile]:
        """File(s) contained in the reference"""
        return self._files

    @property
    def name(self) -> str:
        """Reference name"""
        return self._data.name

    @property
    def type(self) -> str:
        """Type of reference data (e.g. genome_fasta)"""
        return self._data.type_

    @property
    def absolute_path(self):
        if len(self._files) == 0:
            return None
        return self._files[0].absolute_path

    def __str__(self):
        return self.name


class DataPortalReferences(DataPortalAssets[DataPortalReference]):
    """Collection of DataPortalReference objects."""
    asset_name = "reference"

    def get_by_id(self, _id: str) -> DataPortalReference:
        raise NotImplementedError("Filtering by ID is not supported, use get_by_name")
