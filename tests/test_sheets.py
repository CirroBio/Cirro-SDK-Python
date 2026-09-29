import unittest
from unittest.mock import Mock

from cirro_api_client.v1.models import ColumnDataType, ColumnDef, CreateResponse, Project, QueryColumn, Sheet, \
    SheetCreationMode, SheetDataUpdateResponse, SheetDetail, SheetQueryResponse, SheetUpdateResponse, \
    SqlSortOrder, TableSheetInput, ViewSheetInput

from cirro.sdk.exceptions import DataPortalInputError
from cirro.sdk.project import DataPortalProject
from cirro.sdk.sheet import ROWS_PER_PAGE, DataPortalSheet

_PROJECT = {
    "id": "project-1",
    "name": "Test Project",
    "description": "",
    "status": "COMPLETED",
    "tags": [],
    "organization": "org-1",
    "classificationIds": [],
    "billingAccountId": "billing-1"
}

_SHEET = {
    "id": "sheet-1",
    "name": "Diagnoses",
    "namespaceName": "default",
    "tableName": "diagnoses",
    "description": "ICD codes",
    "projectId": "project-1",
    "sheetType": "TABLE",
    "sheetCreationMode": "STANDARD",
    "virtual": False,
    "status": "COMPLETED",
    "createdBy": "user-1",
    "createdAt": "2026-01-01T00:00:00Z",
    "updatedAt": "2026-01-01T00:00:00Z",
    "totalRowCount": 0,
    "tags": [],
    "columnRelationships": []
}

_COLUMNS = [
    QueryColumn(name="_row_id", data_type=ColumnDataType.INTEGER),
    QueryColumn(name="icd_code", data_type=ColumnDataType.STRING)
]


def _make_sheet(client: Mock, **overrides) -> DataPortalSheet:
    """Create a DataPortalSheet backed by the given mock client."""
    return DataPortalSheet(Sheet.from_dict({**_SHEET, **overrides}), client)


def _paging_client(total_rows: int):
    """
    Mock client whose sheets.get_data pages the way the API does, reading the
    slice at offset (page - 1) * limit.

    Returns the client alongside the list of (page, limit) pairs it was asked
    for, so a test can assert how the read was split up.
    """
    requests = []

    def get_data(project_id, sheet_id, limit, page, order_by, order):
        requests.append((page, limit))
        start = (page - 1) * limit
        rows = [[i, f"code-{i}"] for i in range(start, min(start + limit, total_rows))]
        return SheetQueryResponse(columns=_COLUMNS, rows=rows, total_row_count=total_rows)

    client = Mock()
    client.sheets.get_data.side_effect = get_data
    return client, requests


class SheetPagingTest(unittest.TestCase):
    """to_dataframe reads a sheet across as many pages as it takes."""

    def test_single_partial_page(self):
        client, requests = _paging_client(250)
        df = _make_sheet(client).to_dataframe()

        self.assertEqual(250, len(df))
        self.assertEqual(["_row_id", "icd_code"], list(df.columns))
        self.assertEqual([(1, ROWS_PER_PAGE)], requests)

    def test_full_page_does_not_fetch_another(self):
        # total_row_count says the sheet is exhausted, so there is no empty second read
        client, requests = _paging_client(ROWS_PER_PAGE)
        df = _make_sheet(client).to_dataframe()

        self.assertEqual(ROWS_PER_PAGE, len(df))
        self.assertEqual(1, len(requests))

    def test_reads_every_page(self):
        client, requests = _paging_client(2500)
        df = _make_sheet(client).to_dataframe()

        self.assertEqual(2500, len(df))
        self.assertEqual(list(range(2500)), list(df["_row_id"]))
        self.assertEqual([(1, ROWS_PER_PAGE), (2, ROWS_PER_PAGE), (3, ROWS_PER_PAGE)], requests)

    def test_reads_the_whole_sheet_when_uncapped(self):
        # No max_rows means the whole sheet, however many pages that takes
        client, requests = _paging_client(25000)
        df = _make_sheet(client).to_dataframe()

        self.assertEqual(25000, len(df))
        self.assertEqual(25, len(requests))

    def test_page_size_is_constant_across_requests(self):
        # The API pages by number, so a page size that changed mid-read would
        # move the offset under us and skip or repeat rows.
        client, requests = _paging_client(2500)
        _make_sheet(client).to_dataframe(max_rows=2200)

        self.assertEqual({ROWS_PER_PAGE}, {limit for _, limit in requests})

    def test_max_rows_truncates_and_stops_early(self):
        client, requests = _paging_client(5000)
        df = _make_sheet(client).to_dataframe(max_rows=1500)

        self.assertEqual(1500, len(df))
        self.assertEqual(list(range(1500)), list(df["_row_id"]))
        self.assertEqual(2, len(requests))

    def test_max_rows_below_page_size_shrinks_the_page(self):
        client, requests = _paging_client(5000)
        df = _make_sheet(client).to_dataframe(max_rows=10)

        self.assertEqual(10, len(df))
        self.assertEqual([(1, 10)], requests)

    def test_empty_sheet_keeps_its_columns(self):
        client, requests = _paging_client(0)
        df = _make_sheet(client).to_dataframe()

        self.assertTrue(df.empty)
        self.assertEqual(["_row_id", "icd_code"], list(df.columns))
        self.assertEqual(1, len(requests))

    def test_sort_is_passed_through(self):
        client, _ = _paging_client(10)
        _make_sheet(client).to_dataframe(order_by="icd_code", order=SqlSortOrder.DESC)

        _, kwargs = client.sheets.get_data.call_args
        self.assertEqual("icd_code", kwargs["order_by"])
        self.assertEqual(SqlSortOrder.DESC, kwargs["order"])


class NamespaceQueryTest(unittest.TestCase):
    """project.query_sheets pages a raw SQL result the same way."""

    @staticmethod
    def _project(total_rows: int):
        requests = []

        def raw_query(project_id, query, limit, page):
            requests.append((page, limit))
            start = (page - 1) * limit
            rows = [[i, f"code-{i}"] for i in range(start, min(start + limit, total_rows))]
            return SheetQueryResponse(columns=_COLUMNS, rows=rows, total_row_count=total_rows)

        client = Mock()
        client.sheets.raw_query.side_effect = raw_query
        project = DataPortalProject(Project.from_dict(_PROJECT), client)
        return project, requests

    def test_reads_every_page(self):
        project, requests = self._project(2500)
        df = project.query_sheets("SELECT 1")

        self.assertEqual(2500, len(df))
        self.assertEqual(["_row_id", "icd_code"], list(df.columns))
        self.assertEqual(3, len(requests))

    def test_max_rows_truncates(self):
        project, requests = self._project(5000)
        df = project.query_sheets("SELECT 1", max_rows=1500)

        self.assertEqual(1500, len(df))
        self.assertEqual(2, len(requests))

    def test_query_is_passed_through(self):
        project, _ = self._project(10)
        project.query_sheets("SELECT icd_code FROM diagnoses")

        _, kwargs = project._client.sheets.raw_query.call_args
        self.assertEqual("project-1", kwargs["project_id"])
        self.assertEqual("SELECT icd_code FROM diagnoses", kwargs["query"])


class CreateSheetTest(unittest.TestCase):
    def test_create_sheet_returns_the_new_sheet(self):
        client = Mock()
        client.sheets.create.return_value = CreateResponse(id="sheet-1", message="created")
        client.sheets.get.return_value = SheetDetail.from_dict({**_SHEET, "auditReadAccess": False})
        project = DataPortalProject(Project.from_dict(_PROJECT), client)

        sheet_input = TableSheetInput(
            name="Diagnoses",
            namespace_name="default",
            table_name="diagnoses",
            sheet_creation_mode=SheetCreationMode.STANDARD,
            columns=[ColumnDef(name="icd_code", data_type=ColumnDataType.STRING)]
        )
        sheet = project.create_sheet(sheet_input)

        self.assertIsInstance(sheet, DataPortalSheet)
        self.assertEqual("sheet-1", sheet.id)
        client.sheets.create.assert_called_once_with(project_id="project-1", sheet=sheet_input)
        client.sheets.get.assert_called_once_with(project_id="project-1", sheet_id="sheet-1")


class SheetRowWriteTest(unittest.TestCase):
    """Row writes take plain dicts and report how many rows changed."""

    def setUp(self):
        self.client = Mock()
        for method in ("insert_rows", "update_rows", "delete_rows"):
            getattr(self.client.sheets, method).return_value = SheetDataUpdateResponse(rows_affected=2)
        self.sheet = _make_sheet(self.client)

    def test_insert_rows_wraps_plain_dicts(self):
        affected = self.sheet.insert_rows([{"icd_code": "G65"}, {"icd_code": "A01"}])

        self.assertEqual(2, affected)
        _, kwargs = self.client.sheets.insert_rows.call_args
        self.assertEqual([{"icd_code": "G65"}, {"icd_code": "A01"}],
                         [row.values.to_dict() for row in kwargs["rows"]])

    def test_update_rows_is_keyed_by_row_id(self):
        affected = self.sheet.update_rows({42: {"icd_code": "G65"}})

        self.assertEqual(2, affected)
        _, kwargs = self.client.sheets.update_rows.call_args
        self.assertEqual([42], [row.row_id for row in kwargs["rows"]])
        self.assertEqual([{"icd_code": "G65"}], [row.values.to_dict() for row in kwargs["rows"]])

    def test_delete_rows_passes_row_ids(self):
        affected = self.sheet.delete_rows([42, 43])

        self.assertEqual(2, affected)
        _, kwargs = self.client.sheets.delete_rows.call_args
        self.assertEqual([42, 43], kwargs["row_ids"])

    def test_empty_writes_are_rejected(self):
        for write in (lambda: self.sheet.insert_rows([]),
                      lambda: self.sheet.update_rows({}),
                      lambda: self.sheet.delete_rows([])):
            with self.assertRaises(DataPortalInputError):
                write()


class SheetLifecycleTest(unittest.TestCase):
    """update sends the full target state; delete removes the sheet."""

    @staticmethod
    def _sheet_with_detail(**detail_overrides):
        client = Mock()
        client.sheets.get.return_value = SheetDetail.from_dict({
            **_SHEET,
            "auditReadAccess": False,
            "columns": [ColumnDef(name="icd_code", data_type=ColumnDataType.STRING).to_dict()],
            **detail_overrides
        })
        return _make_sheet(client), client

    def test_update_sends_the_whole_table_state(self):
        sheet, client = self._sheet_with_detail()
        client.sheets.update.return_value = SheetUpdateResponse()

        sheet.update(name="Renamed")

        _, kwargs = client.sheets.update.call_args
        target = kwargs["sheet"]
        self.assertIsInstance(target, TableSheetInput)
        self.assertEqual("Renamed", target.name)
        # untouched fields are carried over rather than dropped
        self.assertEqual("ICD codes", target.description)
        self.assertEqual("default", target.namespace_name)
        self.assertEqual(["icd_code"], [column.name for column in target.columns])

    def test_update_sends_the_whole_view_state(self):
        sheet, client = self._sheet_with_detail(sheetType="VIEW", columns=None,
                                                viewDefinition={"viewType": "RAW", "query": "SELECT 1"})
        client.sheets.update.return_value = SheetUpdateResponse()

        sheet.update(description="Rolled up")

        _, kwargs = client.sheets.update.call_args
        self.assertIsInstance(kwargs["sheet"], ViewSheetInput)
        self.assertEqual("Rolled up", kwargs["sheet"].description)
        self.assertEqual("Diagnoses", kwargs["sheet"].name)

    def test_update_reflects_the_rename_locally(self):
        sheet, client = self._sheet_with_detail()
        client.sheets.update.return_value = SheetUpdateResponse(
            sheet=SheetDetail.from_dict({**_SHEET, "name": "Renamed", "auditReadAccess": False})
        )

        sheet.update(name="Renamed")

        self.assertEqual("Renamed", sheet.name)

    def test_delete_removes_the_sheet(self):
        client = Mock()
        sheet = _make_sheet(client)

        sheet.delete()

        client.sheets.delete.assert_called_once_with(project_id="project-1", sheet_id="sheet-1")


class SheetPropertyTest(unittest.TestCase):
    def test_refresh_rejects_a_table(self):
        sheet = _make_sheet(Mock())

        with self.assertRaises(DataPortalInputError):
            sheet.refresh()

    def test_refresh_materializes_a_view(self):
        client = Mock()
        sheet = _make_sheet(client, id="sheet-2", sheetType="VIEW")

        sheet.refresh()

        client.sheets.refresh_view.assert_called_once_with(project_id="project-1", sheet_id="sheet-2")

    def test_virtual_view_reports_no_row_count(self):
        # A virtual view computes its rows on read, so the API sends no count
        sheet = _make_sheet(Mock(), sheetType="VIEW", virtual=True, totalRowCount=None)

        self.assertIsNone(sheet.total_row_count)
        self.assertTrue(sheet.virtual)

    def test_columns_fetches_the_sheet_detail(self):
        client = Mock()
        client.sheets.get.return_value = SheetDetail.from_dict({
            **_SHEET,
            "auditReadAccess": False,
            "columns": [ColumnDef(name="icd_code", data_type=ColumnDataType.STRING).to_dict()]
        })
        sheet = _make_sheet(client)

        self.assertEqual(["icd_code"], [column.name for column in sheet.columns])
        # the detail is kept, so a second read does not go back to the API
        self.assertEqual(["icd_code"], [column.name for column in sheet.columns])
        client.sheets.get.assert_called_once_with(project_id="project-1", sheet_id="sheet-1")


if __name__ == '__main__':
    unittest.main()
