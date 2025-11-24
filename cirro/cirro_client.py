from cirro_api_client import CirroApiClient

from cirro.auth import get_auth_info_from_config
from cirro.auth.base import AuthInfo
from cirro.config import AppConfig
from cirro.services import FileService, DatasetService, ProjectService, ProcessService, ExecutionService, \
    MetricsService, MetadataService, BillingService, ReferenceService, UserService, ComputeEnvironmentService, \
    ShareService, WorkspaceService


class CirroApi:
    """
    Client for interacting directly with the Cirro API
    """
    def __init__(self, auth_info: AuthInfo = None, base_url: str = None, user_agent: str = 'Cirro SDK'):
        """
        Instantiates the Cirro API object

        Args:
            auth_info (cirro.auth.base.AuthInfo):
            base_url (str): Optional base URL of the Cirro instance
             (if not provided, it uses the `CIRRO_BASE_URL` environment variable, or the config file)

        Returns:
            Authenticated Cirro API object, which can be used to call endpoint functions.

        Example:
        ```python
        from cirro.cirro_client import CirroApi

        cirro = CirroApi(base_url="app.cirro.bio")
        print(cirro.projects.list())
        ```
        """

        self._configuration = AppConfig(base_url=base_url)
        if not auth_info:
            auth_info = get_auth_info_from_config(self._configuration, auth_io=None)

        self._api_client = CirroApiClient(
            base_url=self._configuration.rest_endpoint,
            auth_method=auth_info.get_auth_method(),
            client_name=user_agent,
            package_name='cirro'
        )

        # Init services
        self._file_service = FileService(self._api_client,
                                         checksum_method=self._configuration.checksum_method,
                                         transfer_retries=self._configuration.transfer_max_retries)
        self._dataset_service = DatasetService(self._api_client, file_service=self._file_service)
        self._project_service = ProjectService(self._api_client)
        self._process_service = ProcessService(self._api_client)
        self._execution_service = ExecutionService(self._api_client)
        self._compute_environment_service = ComputeEnvironmentService(self._api_client)
        self._metrics_service = MetricsService(self._api_client)
        self._metadata_service = MetadataService(self._api_client)
        self._billing_service = BillingService(self._api_client)
        self._references_service = ReferenceService(self._api_client, file_service=self._file_service)
        self._shares_service = ShareService(self._api_client)
        self._users_service = UserService(self._api_client)
        self._workspace_service = WorkspaceService(self._api_client)

    @property
    def datasets(self) -> DatasetService:
        """
        Create, list, delete, and modify Datasets
        """
        return self._dataset_service

    @property
    def projects(self) -> ProjectService:
        """
        Create, list, delete, and modify Projects
        """
        return self._project_service

    @property
    def processes(self) -> ProcessService:
        """
        List and retrieve detailed information about Processes
        """
        return self._process_service

    @property
    def execution(self) -> ExecutionService:
        """
        List, run, stop, and describe the analysis jobs (executing Processes to create new Datasets)
        """
        return self._execution_service

    @property
    def compute_environments(self) -> ComputeEnvironmentService:
        """
        List and update compute environments
        """
        return self._compute_environment_service

    @property
    def metrics(self) -> MetricsService:
        """
        Project-level summary metrics
        """
        return self._metrics_service

    @property
    def metadata(self) -> MetadataService:
        """
        List and modify Sample metadata or metadata schemas
        """
        return self._metadata_service

    @property
    def billing(self) -> BillingService:
        """
        List and update billing accounts
        """
        return self._billing_service

    @property
    def references(self) -> ReferenceService:
        """
        List References and Reference types
        """
        return self._references_service

    @property
    def shares(self) -> ShareService:
        """
        List, create, update, delete, and subscribe to shares
        """
        return self._shares_service

    @property
    def users(self) -> UserService:
        """
        List and update user information
        """
        return self._users_service

    @property
    def workspaces(self) -> WorkspaceService:
        """
        Manage workspaces
        """
        return self._workspace_service

    @property
    def file(self) -> FileService:
        """
        Read, download, and create file objects
        """
        return self._file_service

    @property
    def api_client(self) -> CirroApiClient:
        """
        Gets the underlying API client
        """
        return self._api_client

    @property
    def configuration(self) -> AppConfig:
        """
        Gets the configuration of the instance
        """
        return self._configuration
