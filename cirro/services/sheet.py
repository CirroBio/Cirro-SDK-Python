from typing import List

from cirro_api_client.v1.api.sheets import get_sheets, get_sheet, create_sheet, update_sheet, delete_sheet, \
    get_sheet_data, query_sheet_data, query_data, insert_sheet_data, update_sheet_data, delete_sheet_data, \
    refresh_view, trigger_ingest, get_jobs
from cirro_api_client.v1.models import Sheet, SheetDetail, TableSheetInput, ViewSheetInput, CreateResponse, \
    SheetUpdateResponse, SheetDataRequest, SheetQueryRequest, SheetQueryResponse, SheetDataUpdateResponse, \
    RowInsert, RowUpdate, InsertRowsRequest, UpdateRowsRequest, DeleteRowsRequest, SheetIngestRequest, SheetJob, \
    SqlSortOrder
from cirro_api_client.v1.types import UNSET

from cirro.services.base import BaseService


class SheetService(BaseService):
    """
    Service for interacting with the Sheet endpoints
    """

    def list(self, project_id: str) -> List[Sheet]:
        """
        Retrieves a list of sheets for a given project

        Args:
            project_id (str): ID of the Project

        ```python
        from cirro.cirro_client import CirroApi

        cirro = CirroApi()
        cirro.sheets.list(project_id="project-id")
        ```
        """
        return get_sheets.sync(project_id=project_id, client=self._api_client)

    def get(self, project_id: str, sheet_id: str) -> SheetDetail:
        """
        Get details of a sheet, including its column definitions or view definition

        Args:
            project_id (str): ID of the Project
            sheet_id (str): ID of the Sheet
        """
        return get_sheet.sync(project_id=project_id, sheet_id=sheet_id, client=self._api_client)

    def create(self, project_id: str, sheet: TableSheetInput | ViewSheetInput) -> CreateResponse:
        """
        Create a sheet (table or view) in the given project

        Args:
            project_id (str): ID of the Project
            sheet (TableSheetInput | ViewSheetInput): Sheet to create
        """
        return create_sheet.sync(project_id=project_id, body=sheet, client=self._api_client)

    def update(self,
               project_id: str,
               sheet_id: str,
               sheet: TableSheetInput | ViewSheetInput,
               dry_run: bool = False) -> SheetUpdateResponse:
        """
        Update a sheet by sending its full target state; the server applies the diff of the mutable fields

        Args:
            project_id (str): ID of the Project
            sheet_id (str): ID of the Sheet
            sheet (TableSheetInput | ViewSheetInput): Target state of the sheet
            dry_run (bool): Report the changes that would be applied without applying them
        """
        return update_sheet.sync(project_id=project_id,
                                 sheet_id=sheet_id,
                                 body=sheet,
                                 dry_run=dry_run,
                                 client=self._api_client)

    def delete(self, project_id: str, sheet_id: str) -> None:
        """
        Delete a sheet

        Args:
            project_id (str): ID of the Project
            sheet_id (str): ID of the Sheet
        """
        delete_sheet.sync_detailed(project_id=project_id, sheet_id=sheet_id, client=self._api_client)

    def get_data(self,
                 project_id: str,
                 sheet_id: str,
                 limit: int = 1000,
                 page: int = 1,
                 order_by: str = None,
                 order: SqlSortOrder = None) -> SheetQueryResponse:
        """
        Retrieve a page of rows from a sheet

        The first column is always `_row_id`, which identifies the row for `update_rows` and `delete_rows`.

        Args:
            project_id (str): ID of the Project
            sheet_id (str): ID of the Sheet
            limit (int): Maximum number of rows to return (default 1,000)
            page (int): Page to return (default 1)
            order_by (str): Column to sort by
            order (SqlSortOrder): Sort direction
        """
        return get_sheet_data.sync(project_id=project_id,
                                   sheet_id=sheet_id,
                                   limit=limit,
                                   page=page,
                                   order_by=order_by if order_by is not None else UNSET,
                                   order=order if order is not None else UNSET,
                                   client=self._api_client)

    def query_data(self, project_id: str, sheet_id: str, request: SheetDataRequest) -> SheetQueryResponse:
        """
        Retrieve a page of rows from a sheet, with an optional sort and filter

        Args:
            project_id (str): ID of the Project
            sheet_id (str): ID of the Sheet
            request (cirro_api_client.v1.models.SheetDataRequest): Page, sort, and filter to apply
        """
        return query_sheet_data.sync(project_id=project_id,
                                     sheet_id=sheet_id,
                                     body=request,
                                     client=self._api_client)

    def raw_query(self,
                  project_id: str,
                  query: str,
                  limit: int = 1000,
                  page: int = 1) -> SheetQueryResponse:
        """
        Run a raw SQL query against the project's sheets

        Every namespace is on the engine's search path, so sheets are reached
        by name without qualifying them.

        Args:
            project_id (str): ID of the Project
            query (str): Raw SQL query to run
            limit (int): Maximum number of rows to return (default 1,000)
            page (int): Page to return (default 1)

        ```python
        from cirro.cirro_client import CirroApi

        cirro = CirroApi()
        results = cirro.sheets.raw_query(
            project_id="project-id",
            query="SELECT icd_code, COUNT(*) FROM diagnoses GROUP BY icd_code"
        )
        ```
        """
        request = SheetQueryRequest(
            query=query,
            limit=limit,
            page=page
        )
        return query_data.sync(project_id=project_id, body=request, client=self._api_client)

    def insert_rows(self, project_id: str, sheet_id: str, rows: List[RowInsert]) -> SheetDataUpdateResponse:
        """
        Insert rows into a sheet

        Args:
            project_id (str): ID of the Project
            sheet_id (str): ID of the Sheet
            rows (list[cirro_api_client.v1.models.RowInsert]): Rows to insert

        ```python
        from cirro_api_client.v1.models import RowInsert, RowInsertValues
        from cirro.cirro_client import CirroApi

        cirro = CirroApi()
        cirro.sheets.insert_rows(
            project_id="project-id",
            sheet_id="sheet-id",
            rows=[RowInsert(values=RowInsertValues.from_dict({"icd_code": "G65"}))]
        )
        ```
        """
        return insert_sheet_data.sync(project_id=project_id,
                                      sheet_id=sheet_id,
                                      body=InsertRowsRequest(inserts=rows),
                                      client=self._api_client)

    def update_rows(self, project_id: str, sheet_id: str, rows: List[RowUpdate]) -> SheetDataUpdateResponse:
        """
        Update rows in a sheet by `_row_id`

        Only the columns included in each row are modified; all others are left unchanged.

        Args:
            project_id (str): ID of the Project
            sheet_id (str): ID of the Sheet
            rows (list[cirro_api_client.v1.models.RowUpdate]): Rows to update
        """
        return update_sheet_data.sync(project_id=project_id,
                                      sheet_id=sheet_id,
                                      body=UpdateRowsRequest(updates=rows),
                                      client=self._api_client)

    def delete_rows(self, project_id: str, sheet_id: str, row_ids: List[int]) -> SheetDataUpdateResponse:
        """
        Delete rows from a sheet by `_row_id`

        Args:
            project_id (str): ID of the Project
            sheet_id (str): ID of the Sheet
            row_ids (list[int]): `_row_id` of each row to delete (at most 10,000)
        """
        return delete_sheet_data.sync(project_id=project_id,
                                      sheet_id=sheet_id,
                                      body=DeleteRowsRequest(row_ids=row_ids),
                                      client=self._api_client)

    def refresh_view(self, project_id: str, sheet_id: str) -> None:
        """
        Re-materialize a view sheet

        Args:
            project_id (str): ID of the Project
            sheet_id (str): ID of the Sheet
        """
        refresh_view.sync_detailed(project_id=project_id, sheet_id=sheet_id, client=self._api_client)

    def trigger_ingest(self, project_id: str, sheet_id: str, request: SheetIngestRequest) -> None:
        """
        Ingest a file that has been staged to the sheet's upload path

        The file must already sit under `s3://project-<project id>/sheets/`,
        which is the only prefix the sheets IAM role can read. `staging_upload_path`
        on the sheet's detail gives the location to write to; put the file there
        with `cirro.file.upload_files` before calling this.

        Args:
            project_id (str): ID of the Project
            sheet_id (str): ID of the Sheet
            request (cirro_api_client.v1.models.SheetIngestRequest): File to ingest and its column mapping

        ```python
        from cirro.cirro_client import CirroApi

        cirro = CirroApi()
        sheet = cirro.sheets.get(project_id="project-id", sheet_id="sheet-id")
        print(sheet.staging_upload_path)  # where the file has to land
        ```
        """
        trigger_ingest.sync_detailed(project_id=project_id,
                                     sheet_id=sheet_id,
                                     body=request,
                                     client=self._api_client)

    def list_jobs(self, project_id: str, sheet_id: str) -> List[SheetJob]:
        """
        Retrieve the jobs run against a sheet, such as ingests and view materializations

        Args:
            project_id (str): ID of the Project
            sheet_id (str): ID of the Sheet
        """
        return get_jobs.sync(project_id=project_id, sheet_id=sheet_id, client=self._api_client)
