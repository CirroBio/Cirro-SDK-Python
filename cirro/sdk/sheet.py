from typing import Dict, List, Optional, TYPE_CHECKING, Union

if TYPE_CHECKING:
    from pandas import DataFrame

from cirro_api_client.v1.models import ColumnDef, RowInsert, RowInsertValues, RowUpdate, RowUpdateValues, Sheet, \
    SheetDetail, SheetJob, SheetType, SqlSortOrder, Status
from cirro_api_client.v1.types import Unset

from cirro.cirro_client import CirroApi
from cirro.sdk.asset import DataPortalAssets, DataPortalAsset
from cirro.sdk.exceptions import DataPortalInputError

# The API limits the size of a response rather than its row count, so a page
# much wider than this can come back as a 502.
ROWS_PER_PAGE = 1000


class DataPortalSheet(DataPortalAsset):
    """
    Sheets are tables of structured data held alongside a project, either a
    TABLE holding rows directly or a VIEW materialized from a query.
    """

    def __init__(self, sheet: Union[Sheet, SheetDetail], client: CirroApi):
        """
        Instantiate by listing the sheets in a project

        ```python
        from cirro import DataPortal
        portal = DataPortal()
        project = portal.get_project_by_name("Project Name")
        sheets = project.list_sheets()
        ```
        """
        self._data = sheet
        self._client = client

    def _get_detail(self) -> SheetDetail:
        if not isinstance(self._data, SheetDetail):
            self._data = self._client.sheets.get(project_id=self.project_id, sheet_id=self.id)
        return self._data

    @property
    def id(self) -> str:
        """Unique identifier"""
        return self._data.id

    @property
    def name(self) -> str:
        """Editable name for the sheet"""
        return self._data.name

    @property
    def description(self) -> str:
        """Longer description of the sheet"""
        return self._data.description

    @property
    def project_id(self) -> str:
        """ID of the project which contains the sheet"""
        return self._data.project_id

    @property
    def sheet_type(self) -> SheetType:
        """Whether the sheet is a TABLE or a VIEW"""
        return self._data.sheet_type

    @property
    def status(self) -> Status:
        """Status of the sheet"""
        return self._data.status

    @property
    def total_row_count(self) -> Optional[int]:
        """
        Number of rows the sheet holds, or `None` for a virtual view whose rows
        are computed on read rather than stored.
        """
        count = self._data.total_row_count
        return None if isinstance(count, Unset) else count

    @property
    def virtual(self) -> bool:
        """Whether a VIEW is computed on read rather than materialized"""
        return self._data.virtual

    @property
    def namespace_name(self) -> str:
        """Namespace containing the sheet's underlying table"""
        return self._data.namespace_name

    @property
    def table_name(self) -> str:
        """Name of the sheet's underlying table"""
        return self._data.table_name

    @property
    def columns(self) -> List[ColumnDef]:
        """
        Column definitions for the sheet, or `None` for a VIEW.

        Fetches the sheet's full detail on first access.
        """
        return self._get_detail().columns

    def __str__(self):
        return '\n'.join([
            f"{i.replace('_', ' ').title()}: {self.__getattribute__(i)}"
            for i in ['name', 'id', 'description', 'sheet_type', 'total_row_count']
        ])

    def to_dataframe(self,
                     max_rows: int = 10000,
                     order_by: str = None,
                     order: SqlSortOrder = None) -> 'DataFrame':
        """
        Read the sheet's rows into a Pandas DataFrame.

        Pages through the sheet until it has every row or `max_rows`,
        whichever comes first. The `_row_id` column identifies each row for
        `update_rows` and `delete_rows`.

        Args:
            max_rows (int): Maximum number of rows to read (default 10,000)
            order_by (str): Column to sort by
            order (SqlSortOrder): Sort direction

        Returns:
            `pandas.DataFrame`
        """
        import pandas

        # Held constant across requests: the API pages by number, so a page
        # size that changed mid-read would skip or repeat rows.
        page_size = min(ROWS_PER_PAGE, max_rows)
        rows = []
        columns = []
        page = 1

        while len(rows) < max_rows:
            results = self._client.sheets.get_data(
                project_id=self.project_id,
                sheet_id=self.id,
                limit=page_size,
                page=page,
                order_by=order_by,
                order=order
            )
            columns = results.columns
            if not results.rows:
                break

            rows.extend(results.rows)
            if len(rows) >= results.total_row_count:
                break
            page += 1

        return pandas.DataFrame(
            rows[:max_rows],
            columns=[column.name for column in columns]
        )

    def insert_rows(self, rows: List[dict]) -> int:
        """
        Add rows to the sheet.

        Args:
            rows (List[dict]): Each row as a mapping of column name to value.
                Columns left out are null.

        Returns:
            Number of rows inserted

        Example:
        ```python
        sheet.insert_rows([{"icd_code": "G65"}, {"icd_code": "A01"}])
        ```
        """
        if not rows:
            raise DataPortalInputError("Must provide at least one row to insert")

        response = self._client.sheets.insert_rows(
            project_id=self.project_id,
            sheet_id=self.id,
            rows=[RowInsert(values=RowInsertValues.from_dict(row)) for row in rows]
        )
        return response.rows_affected

    def update_rows(self, rows: Dict[int, dict]) -> int:
        """
        Modify rows in the sheet, keyed by `_row_id`.

        Only the columns named in each mapping are written; the rest of the row
        is left alone.

        Args:
            rows (Dict[int, dict]): `_row_id` mapped to the column values to set

        Returns:
            Number of rows updated

        Example:
        ```python
        sheet.update_rows({42: {"icd_code": "G65"}})
        ```
        """
        if not rows:
            raise DataPortalInputError("Must provide at least one row to update")

        response = self._client.sheets.update_rows(
            project_id=self.project_id,
            sheet_id=self.id,
            rows=[
                RowUpdate(row_id=row_id, values=RowUpdateValues.from_dict(values))
                for row_id, values in rows.items()
            ]
        )
        return response.rows_affected

    def delete_rows(self, row_ids: List[int]) -> int:
        """
        Remove rows from the sheet, identified by `_row_id`.

        Args:
            row_ids (List[int]): `_row_id` of each row to remove (at most 10,000)

        Returns:
            Number of rows deleted
        """
        if not row_ids:
            raise DataPortalInputError("Must provide at least one row ID to delete")

        response = self._client.sheets.delete_rows(
            project_id=self.project_id,
            sheet_id=self.id,
            row_ids=row_ids
        )
        return response.rows_affected

    def refresh(self) -> None:
        """
        Re-materialize a VIEW sheet from its query.

        Raises:
            DataPortalInputError: if the sheet is a TABLE rather than a VIEW.
        """
        if self.sheet_type != SheetType.VIEW:
            raise DataPortalInputError(f"Only VIEW sheets can be refreshed, '{self.name}' is a {self.sheet_type}")

        self._client.sheets.refresh_view(project_id=self.project_id, sheet_id=self.id)

    def list_jobs(self) -> List[SheetJob]:
        """
        List the jobs run against the sheet, such as ingests and view
        materializations.

        Returns:
            `List[cirro_api_client.v1.models.SheetJob]`
        """
        return self._client.sheets.list_jobs(project_id=self.project_id, sheet_id=self.id)


class DataPortalSheets(DataPortalAssets[DataPortalSheet]):
    """Collection of DataPortalSheet objects."""
    asset_name = "sheet"
