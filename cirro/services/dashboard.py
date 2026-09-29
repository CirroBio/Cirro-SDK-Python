from typing import List

from cirro_api_client.v1 import errors
from cirro_api_client.v1.api.dashboards import get_dashboards, get_dashboard, create_dashboard, \
    update_dashboard, delete_dashboard, get_dashboard_templates, get_dashboard_template
from cirro_api_client.v1.models import Dashboard, DashboardInput, CreateResponse

from cirro.services.base import BaseService


class DashboardService(BaseService):
    """
    Service for interacting with the Dashboard endpoints (the portal's Data Studio)

    A dashboard's `dashboard_data` and `criteria` are stored as-is by the API; their shape
    is defined by the portal. The Data Studio lists only records whose criteria carry
    `type: "sql-room"`, and `dashboard_data` is the portal's persisted dashboard document.
    """

    def list(self, project_id: str) -> List[Dashboard]:
        """
        Retrieves the dashboards in a project. `dashboard_data` is not included;
        use `get` for a single dashboard's contents.

        Args:
            project_id (str): ID of the Project

        ```python
        from cirro.cirro_client import CirroApi

        cirro = CirroApi()
        cirro.dashboards.list(project_id="project-id")
        ```
        """
        return get_dashboards.sync(project_id=project_id, client=self._api_client)

    def get(self, project_id: str, dashboard_id: str) -> Dashboard:
        """
        Get a dashboard, including its `dashboard_data`

        Args:
            project_id (str): ID of the Project
            dashboard_id (str): ID of the Dashboard
        """
        return get_dashboard.sync(project_id=project_id, dashboard_id=dashboard_id, client=self._api_client)

    def create(self, project_id: str, dashboard: DashboardInput) -> CreateResponse:
        """
        Create a dashboard in the given project

        Args:
            project_id (str): ID of the Project
            dashboard (`cirro_api_client.v1.models.DashboardInput`): Dashboard to create
        """
        return create_dashboard.sync(project_id=project_id, body=dashboard, client=self._api_client)

    def update(self, project_id: str, dashboard_id: str, dashboard: DashboardInput) -> Dashboard:
        """
        Replace a dashboard. Every field of `dashboard` is written, so read the current
        record first and carry over anything that should not change.

        The endpoint has no revision check: a write here replaces whatever was saved
        last, including an edit made in the portal since the record was read.

        Args:
            project_id (str): ID of the Project
            dashboard_id (str): ID of the Dashboard
            dashboard (`cirro_api_client.v1.models.DashboardInput`): The full new record
        """
        return update_dashboard.sync(project_id=project_id, dashboard_id=dashboard_id,
                                     body=dashboard, client=self._api_client)

    def delete(self, project_id: str, dashboard_id: str) -> None:
        """
        Delete a dashboard

        Args:
            project_id (str): ID of the Project
            dashboard_id (str): ID of the Dashboard
        """
        # The generated route parses the empty 204 body as a Dashboard and raises, in
        # sync_detailed too, so the request is sent here and only its status is read.
        response = self._api_client.get_httpx_client().request(
            auth=self._api_client.get_auth(),
            **delete_dashboard._get_kwargs(project_id=project_id, dashboard_id=dashboard_id))
        if response.status_code >= 400:
            errors.handle_error_response(response, True)

    def list_templates(self) -> List[Dashboard]:
        """
        Retrieves the dashboard templates available across the tenant
        """
        return get_dashboard_templates.sync(client=self._api_client)

    def get_template(self, dashboard_id: str) -> Dashboard:
        """
        Get a dashboard template, including its `dashboard_data`

        Args:
            dashboard_id (str): ID of the template
        """
        return get_dashboard_template.sync(dashboard_id=dashboard_id, client=self._api_client)
