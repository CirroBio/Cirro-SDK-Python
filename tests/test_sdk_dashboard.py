import copy
import json
import unittest
from types import SimpleNamespace

import httpx
from cirro_api_client import CirroApiClient

from cirro.sdk.dashboard import DATA_STUDIO_TYPE
from cirro.sdk.exceptions import DataPortalAssetNotFound, DataPortalConflictError
from cirro.sdk.project import DataPortalProject
from cirro.services import DashboardService


def record(dashboard_id, name, updated, criteria_type=DATA_STUDIO_TYPE, revision="r1"):
    return {"id": dashboard_id, "name": name, "description": "", "tags": [],
            "criteria": {"type": criteria_type, "scope": "project", "revision": revision},
            "dashboardData": {"version": 1,
                              "ir": {"nodes": [{"id": "src1", "type": "dataset-file", "config": {}}],
                                     "edges": []},
                              "positions": {}, "activeDashboardId": "default",
                              "dashboards": [{"id": "default", "name": "Default", "tiles": []}]},
            "createdBy": "me", "createdAt": "2026-09-01T00:00:00+00:00",
            "updatedAt": updated, "schemaVersion": 1}


class FakeDashboards:
    """The Dashboards API over an in-memory table, as an httpx transport."""

    def __init__(self):
        self.store = {
            "d-old": record("d-old", "Older", "2026-09-02T00:00:00+00:00"),
            "d-new": record("d-new", "Newer", "2026-09-03T00:00:00+00:00"),
            "d-other": record("d-other", "Other feature", "2026-09-04T00:00:00+00:00",
                              criteria_type="something-else"),
        }
        self.paths = []

    def __call__(self, request: httpx.Request) -> httpx.Response:
        self.paths.append(request.url.path)
        parts = request.url.path.strip("/").split("/")
        if request.method == "GET" and len(parts) == 3:
            return httpx.Response(200, json=[{k: v for k, v in r.items() if k != "dashboardData"}
                                             for r in self.store.values()])
        if request.method == "POST":
            body = json.loads(request.content)
            self.store["d-created"] = {**body, "id": "d-created", "createdBy": "me",
                                       "createdAt": "2026-09-05T00:00:00+00:00",
                                       "updatedAt": "2026-09-05T00:00:00+00:00"}
            return httpx.Response(201, json={"id": "d-created", "message": "created"})
        dashboard_id = parts[3]
        if dashboard_id not in self.store:
            return httpx.Response(404, json={"statusCode": 404, "errorCode": "NOT_FOUND",
                                             "errorDetail": "not found", "errors": []})
        if request.method == "GET":
            return httpx.Response(200, json=self.store[dashboard_id])
        if request.method == "PUT":
            self.store[dashboard_id].update(json.loads(request.content))
            self.store[dashboard_id]["updatedAt"] = "2026-09-06T00:00:00+00:00"
            return httpx.Response(200, json=self.store[dashboard_id])
        del self.store[dashboard_id]
        return httpx.Response(204)


class TestDataPortalDashboard(unittest.TestCase):
    def setUp(self):
        self.api = FakeDashboards()
        client = SimpleNamespace(dashboards=DashboardService(CirroApiClient(
            base_url="https://api.test", auth_method=None,
            httpx_args={"transport": httpx.MockTransport(self.api)})))
        self.project = DataPortalProject(SimpleNamespace(id="p1", name="Project"), client)

    def test_lists_data_studio_dashboards_newest_first(self):
        self.assertEqual([d.name for d in self.project.list_dashboards()], ["Newer", "Older"])
        self.assertEqual(len(self.project.list_dashboards(include_other=True)), 3)

    def test_get_by_name_or_id_loads_the_document(self):
        by_name = self.project.get_dashboard("Older")
        self.assertEqual(by_name.id, "d-old")
        self.assertEqual([n["id"] for n in by_name.nodes], ["src1"])
        self.assertEqual(self.project.get_dashboard("d-new").name, "Newer")
        with self.assertRaises(DataPortalAssetNotFound):
            self.project.get_dashboard("nope")

    def test_get_by_id_fetches_the_one_record(self):
        dashboard = self.project.get_dashboard_by_id("d-old")
        self.assertEqual([n["id"] for n in dashboard.nodes], ["src1"])
        self.assertEqual(self.api.paths, ["/projects/p1/dashboards/d-old"])
        with self.assertRaises(DataPortalAssetNotFound):
            self.project.get_dashboard_by_id("gone")

    def test_create_makes_a_data_studio_dashboard(self):
        document = copy.deepcopy(self.api.store["d-old"]["dashboardData"])
        created = self.project.create_dashboard("Fresh", document, description="d")
        self.assertTrue(created.in_data_studio)
        self.assertEqual(created.document, document)
        self.assertTrue(self.api.store["d-created"]["criteria"]["revision"])

    def test_save_writes_the_document_and_a_new_revision(self):
        dashboard = self.project.get_dashboard("Older")
        document = copy.deepcopy(dashboard.document)
        document["ir"]["nodes"].append({"id": "view1", "type": "chart", "config": {"viewType": "table"}})
        dashboard.save(document)
        stored = self.api.store["d-old"]
        self.assertEqual([n["id"] for n in stored["dashboardData"]["ir"]["nodes"]], ["src1", "view1"])
        self.assertNotEqual(stored["criteria"]["revision"], "r1")
        self.assertEqual(stored["criteria"]["type"], DATA_STUDIO_TYPE)
        self.assertEqual([n["id"] for n in dashboard.nodes], ["src1", "view1"], "refreshed after save")

    def test_save_refuses_to_overwrite_a_save_made_elsewhere(self):
        dashboard = self.project.get_dashboard("Older")
        self.api.store["d-old"]["criteria"]["revision"] = "saved-in-the-portal"
        with self.assertRaises(DataPortalConflictError):
            dashboard.save(dashboard.document)
        self.assertEqual(self.api.store["d-old"]["criteria"]["revision"], "saved-in-the-portal")
        dashboard.refresh()
        dashboard.save(dashboard.document)

    def test_rename_keeps_the_document_and_its_revision(self):
        dashboard = self.project.list_dashboards().get_by_name("Older")
        dashboard.rename("Renamed")
        stored = self.api.store["d-old"]
        self.assertEqual((stored["name"], stored["criteria"]["revision"]), ("Renamed", "r1"))
        self.assertEqual(stored["dashboardData"]["ir"]["nodes"][0]["id"], "src1")

    def test_delete(self):
        self.project.get_dashboard("Older").delete()
        self.assertNotIn("d-old", self.api.store)


if __name__ == '__main__':
    unittest.main()
