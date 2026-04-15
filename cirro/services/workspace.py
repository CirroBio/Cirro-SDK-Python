from cirro_api_client.v1.api.workspaces import get_workspaces, get_workspace, get_workspace_environments, \
    create_workspace, delete_workspace, start_workspace, stop_workspace
from cirro_api_client.v1.models import Workspace, WorkspaceInput, CreateResponse, WorkspaceEnvironment

from cirro.services.base import BaseService


class WorkspaceService(BaseService):
    """
    Service for interacting with the Workspace endpoints
    """
    def list_environments(self) -> list[WorkspaceEnvironment]:
        """
        List available workspace environments
        """
        return get_workspace_environments.sync(client=self._api_client)

    def list(self, project_id: str) -> list[Workspace]:
        """
        Retrieves a list of workspaces that the user has access to

        Args:
            project_id (str): ID of the Project
        """
        return get_workspaces.sync(project_id, client=self._api_client)

    def get(self, project_id: str, workspace_id: str) -> Workspace:
        """
        Get details of a workspace

        Args:
            project_id (str): ID of the Project
            workspace_id (str): ID of the Workspace
        """
        return get_workspace.sync(project_id=project_id, workspace_id=workspace_id, client=self._api_client)

    def create(self, project_id: str, workspace: WorkspaceInput) -> CreateResponse:
        """
        Create a new workspace in the given project

        Args:
            project_id (str): ID of the Project
            workspace (WorkspaceInput): Workspace object to create
        """
        return create_workspace.sync(project_id=project_id, client=self._api_client, body=workspace)

    def delete(self, project_id: str, workspace_id: str) -> None:
        """
        Delete a workspace in the given project

        Args:
            project_id (str): ID of the Project
            workspace_id (str): ID of the Workspace
        """
        delete_workspace.sync_detailed(project_id=project_id, workspace_id=workspace_id, client=self._api_client)

    def start(self, project_id: str, workspace_id: str) -> None:
        """
        Start a workspace environment

        Args:
            project_id (str): ID of the Project
            workspace_id (str): ID of the Workspace
        """
        start_workspace.sync_detailed(project_id=project_id, workspace_id=workspace_id, client=self._api_client)

    def stop(self, project_id: str, workspace_id: str) -> None:
        """
        Stop a workspace environment

        Args:
            project_id (str): ID of the Project
            workspace_id (str): ID of the Workspace
        """
        stop_workspace.sync_detailed(project_id=project_id, workspace_id=workspace_id, client=self._api_client)
