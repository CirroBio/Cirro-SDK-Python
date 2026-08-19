from cirro.auth import DeviceCodeAuth
from cirro.cirro_client import CirroApi
from cirro.config import AppConfig
from cirro.sdk.portal import DataPortal


class DataPortalLogin:
    """
    Start the login process, obtaining the authorization message from Cirro
    needed to confirm the user identity.

    Use this when a person is available to complete the login but you need to
    control when your code blocks -- for example to render the authorization
    message in a web page or notebook before waiting. Constructing the object
    does not block; only `await_completion` does.

    For automation with no person in the loop, use OAuth client credentials
    instead -- see `cirro.sdk.portal.DataPortal`.

    Usage:

    ```python
    # Replace app.cirro.bio as appropriate
    login = DataPortalLogin(base_url="app.cirro.bio")

    # Present the user with the authorization message
    print(login.auth_message)

    # Generate the authenticated DataPortal object,
    # blocking until the user completes the login process in their browser
    portal = login.await_completion()
    ```
    """
    base_url: str
    auth_info: DeviceCodeAuth

    def __init__(self, base_url: str = None, enable_cache=False):
        """
        Begin a device-code login without waiting for it to complete.

        Args:
            base_url (str): Base URL of the Cirro instance, e.g. `app.cirro.bio`.
             If omitted, falls back to the `CIRRO_BASE_URL` environment variable,
             then to the saved configuration.
            enable_cache (bool): If True, save the resulting token to the system
             keychain so later sessions can reuse it.
        """
        app_config = AppConfig(base_url=base_url)

        self.base_url = base_url

        self.auth_info = DeviceCodeAuth(
            region=app_config.region,
            client_id=app_config.client_id,
            auth_endpoint=app_config.auth_endpoint,
            enable_cache=enable_cache,
            await_completion=False
        )

    @property
    def auth_message(self) -> str:
        """Authorization message provided by Cirro."""
        return self.auth_info.auth_message

    @property
    def auth_message_markdown(self) -> str:
        """Authorization message provided by Cirro (Markdown format)."""
        return self.auth_info.auth_message_markdown

    def await_completion(self) -> DataPortal:
        """
        Block until the user completes the login in their browser.

        Returns:
            `cirro.sdk.portal.DataPortal`: an authenticated portal object.

        Raises:
            RuntimeError: if the device code expires before the login completes.
        """

        # Block until the user completes the login flow
        self.auth_info.await_completion()

        # Set up the client object
        cirro_client = CirroApi(
            auth_info=self.auth_info,
            base_url=self.base_url
        )

        # Return the Data Portal object
        return DataPortal(client=cirro_client)
