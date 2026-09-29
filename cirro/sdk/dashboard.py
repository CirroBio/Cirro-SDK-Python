import uuid
from datetime import datetime
from typing import List, Optional

from cirro_api_client.v1.models import Dashboard, DashboardInput

from cirro.cirro_client import CirroApi
from cirro.sdk.asset import DataPortalAssets, DataPortalAsset
from cirro.sdk.exceptions import DataPortalConflictError

# The portal's record envelope: the Data Studio lists only dashboards whose criteria
# carry this type, and bumps `revision` on every save of the document.
DATA_STUDIO_TYPE = "sql-room"
RECORD_SCHEMA_VERSION = 1


class DataPortalDashboard(DataPortalAsset):
    """
    A dashboard in a project's Data Studio: charts, tables and filter controls over
    the project's dataset files and sheets.

    The layout and contents are the portal's document (`document`), a graph of data
    sources, SQL queries and views plus the tiles that place them. The portal owns its
    format; `save` writes one back, refusing if someone has saved the dashboard since
    it was read.
    """

    def __init__(self, dashboard: Dashboard, project_id: str, client: CirroApi):
        """
        Instantiate by listing the dashboards in a project

        ```python
        from cirro import DataPortal
        portal = DataPortal()
        project = portal.get_project_by_name("Project Name")
        dashboards = project.list_dashboards()
        ```
        """
        self._record = dashboard.to_dict()
        self._project_id = project_id
        self._client = client

    @property
    def id(self) -> str:
        """Unique identifier"""
        return self._record["id"]

    @property
    def name(self) -> str:
        """Name shown in the Data Studio"""
        return self._record["name"]

    @property
    def description(self) -> str:
        """Longer description of the dashboard"""
        return self._record.get("description") or ""

    @property
    def project_id(self) -> str:
        """ID of the project which contains the dashboard"""
        return self._project_id

    @property
    def created_by(self) -> str:
        """User who created the dashboard"""
        return self._record.get("createdBy")

    @property
    def created_at(self) -> datetime:
        """When the dashboard was created"""
        return datetime.fromisoformat(self._record["createdAt"])

    @property
    def updated_at(self) -> datetime:
        """When the dashboard was last saved"""
        return datetime.fromisoformat(self._record["updatedAt"])

    @property
    def in_data_studio(self) -> bool:
        """Whether the Data Studio shows this record (other features share the table)"""
        return (self._record.get("criteria") or {}).get("type") == DATA_STUDIO_TYPE

    @property
    def document(self) -> dict:
        """
        The dashboard's contents as the portal stores them: `ir` (its nodes and the
        edges between them), and `dashboards` (pages of tiles placing its views).

        Fetches the full record on first access if this object came from a listing,
        which leaves the document out. Empty for a dashboard never saved in the portal.
        """
        if "dashboardData" not in self._record:
            self.refresh()
        data = self._record.get("dashboardData") or {}
        return data if isinstance(data.get("version"), int) else {}

    @property
    def nodes(self) -> List[dict]:
        """The dashboard's sources, queries, views and controls, as `{id, type, config}`"""
        return list((self.document.get("ir") or {}).get("nodes") or [])

    @property
    def tiles(self) -> List[dict]:
        """Where each view sits on the first page, as `{componentId, x, y, w, h}`"""
        pages = self.document.get("dashboards") or []
        return list(pages[0].get("tiles") or []) if pages else []

    def __str__(self):
        return '\n'.join([
            f"Name: {self.name}",
            f"Id: {self.id}",
            f"Description: {self.description}",
            f"Updated At: {self._record.get('updatedAt')}",
            f"Nodes: {len(self.nodes)}",
            f"Tiles: {len(self.tiles)}",
        ])

    def refresh(self) -> None:
        """Re-read the dashboard from Cirro, picking up any saves made since"""
        self._record = self._client.dashboards.get(project_id=self._project_id,
                                                   dashboard_id=self.id).to_dict()

    def save(self, document: dict) -> None:
        """
        Replace the dashboard's contents with `document`.

        Refused if the dashboard was saved elsewhere -- in the portal, or by another
        script -- since this object last read it, so nobody's work is overwritten
        unseen. Call `refresh`, re-apply the change to the new `document`, and save
        again. The portal does not validate the document: one it cannot read is shown
        as unreadable, and a node whose config has a wrong-typed field is reset.

        Args:
            document (dict): The full new document, typically `document` edited.

        Raises:
            DataPortalConflictError: if the dashboard was saved since it was read.
        """
        current = self._current()
        criteria = {**(current.get("criteria") or {}), "revision": str(uuid.uuid4())}
        self._write(current, document=document, criteria=criteria)

    def rename(self, name: str, description: Optional[str] = None) -> None:
        """
        Rename the dashboard, and optionally change its description.

        Args:
            name (str): New name.
            description (str): New description; the current one is kept if omitted.
        """
        current = self._client.dashboards.get(project_id=self._project_id,
                                              dashboard_id=self.id).to_dict()
        self._write(current, name=name,
                    description=description if description is not None
                    else current.get("description") or "")

    def delete(self) -> None:
        """Delete the dashboard for everyone. The data it reads is not touched."""
        self._client.dashboards.delete(project_id=self._project_id, dashboard_id=self.id)

    def _current(self) -> dict:
        """The record as Cirro holds it now, refused if its document moved since read."""
        current = self._client.dashboards.get(project_id=self._project_id,
                                              dashboard_id=self.id).to_dict()
        # A listing carries the revision but not the document, so the document is
        # compared only once it has been read.
        seen = (self._record.get("criteria") or {}).get("revision")
        moved = (current.get("criteria") or {}).get("revision") != seen or (
            "dashboardData" in self._record
            and current.get("dashboardData") != self._record["dashboardData"])
        if moved:
            raise DataPortalConflictError(
                f"Dashboard '{self.name}' was saved elsewhere since it was read. "
                "Call refresh(), re-apply your change, and save again.")
        return current

    def _write(self, current: dict, **changes) -> None:
        # The endpoint replaces the whole record, so every field is carried over.
        record = {
            "name": current["name"], "description": current.get("description") or "",
            "document": current.get("dashboardData") or {},
            "criteria": current.get("criteria") or {},
            "tags": current.get("tags") or [],
            "schemaVersion": current.get("schemaVersion") or RECORD_SCHEMA_VERSION,
            **changes,
        }
        self._client.dashboards.update(
            project_id=self._project_id, dashboard_id=self.id,
            dashboard=DashboardInput.from_dict({
                "name": record["name"], "description": record["description"],
                "dashboardData": record["document"], "criteria": record["criteria"],
                "tags": record["tags"], "schemaVersion": record["schemaVersion"]}))
        self.refresh()


class DataPortalDashboards(DataPortalAssets[DataPortalDashboard]):
    """Collection of DataPortalDashboard objects"""
    asset_name = "dashboard"
