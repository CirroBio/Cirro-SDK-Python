import json
import unittest

import httpx
from cirro_api_client import CirroApiClient
from cirro_api_client.v1.errors import NotFoundException
from cirro_api_client.v1.models import DashboardInput

from cirro.services import DashboardService

RECORD = {
    "id": "dash-1", "name": "Expression", "description": "", "tags": [],
    "criteria": {"type": "sql-room", "scope": "project", "revision": "r1"},
    "dashboardData": {"version": 1, "ir": {"nodes": [], "edges": []}},
    "createdBy": "me", "createdAt": "2026-09-29T00:00:00Z", "updatedAt": "2026-09-29T00:00:00Z",
    "schemaVersion": 1,
}


class TestDashboardService(unittest.TestCase):
    def setUp(self):
        self.requests = []

        def handler(request: httpx.Request) -> httpx.Response:
            self.requests.append(request)
            path, method = request.url.path, request.method
            if method == "GET" and path == "/projects/p1/dashboards":
                return httpx.Response(200, json=[{k: v for k, v in RECORD.items() if k != "dashboardData"}])
            if method == "GET" and path == "/projects/p1/dashboards/dash-1":
                return httpx.Response(200, json=RECORD)
            if method == "POST" and path == "/projects/p1/dashboards":
                return httpx.Response(201, json={"id": "dash-2", "message": "created"})
            if method == "PUT" and path == "/projects/p1/dashboards/dash-1":
                return httpx.Response(200, json={**RECORD, **json.loads(request.content)})
            if method == "DELETE" and path == "/projects/p1/dashboards/dash-1":
                return httpx.Response(204)
            return httpx.Response(404, json={"statusCode": 404, "errorCode": "NOT_FOUND",
                                             "errorDetail": "Dashboard not found", "errors": []})

        client = CirroApiClient(base_url="https://api.test", auth_method=None,
                                httpx_args={"transport": httpx.MockTransport(handler)})
        self.service = DashboardService(client)

    def test_list_and_get(self):
        listed = self.service.list(project_id="p1")
        self.assertEqual([d.id for d in listed], ["dash-1"])
        record = self.service.get(project_id="p1", dashboard_id="dash-1")
        self.assertEqual(record.dashboard_data.to_dict()["version"], 1)
        self.assertEqual(record.criteria.to_dict()["revision"], "r1")

    def test_create_and_update_send_the_whole_record(self):
        body = DashboardInput.from_dict({
            "name": "New", "description": "d", "dashboardData": RECORD["dashboardData"],
            "criteria": {"type": "sql-room", "scope": "project"}, "tags": [], "schemaVersion": 1})
        self.assertEqual(self.service.create(project_id="p1", dashboard=body).id, "dash-2")
        updated = self.service.update(project_id="p1", dashboard_id="dash-1", dashboard=body)
        self.assertEqual(updated.name, "New")
        sent = json.loads(self.requests[-1].content)
        self.assertEqual(sorted(sent), ["criteria", "dashboardData", "description", "name",
                                        "schemaVersion", "tags"])

    def test_delete_reads_an_empty_204(self):
        # The generated delete route parses the 204 body as a Dashboard and raises.
        self.service.delete(project_id="p1", dashboard_id="dash-1")
        self.assertEqual(self.requests[-1].method, "DELETE")

    def test_delete_raises_for_a_missing_dashboard(self):
        with self.assertRaises(NotFoundException):
            self.service.delete(project_id="p1", dashboard_id="gone")


if __name__ == '__main__':
    unittest.main()
