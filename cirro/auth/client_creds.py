import base64
import logging
import threading
import time
from typing import Optional

import jwt
import requests
from cirro_api_client import RefreshableTokenAuth
from cirro_api_client.cirro_auth import AuthMethod

from cirro.auth.base import AuthInfo
from cirro.auth.oauth_models import OAuthTokenResponse

logger = logging.getLogger()


class ClientCredentialsAuth(AuthInfo):
    """
    Authenticates to Cirro with OAuth client credentials

    Args:
        client_id (str): Client ID
        client_secret (str): Client Secret
        auth_endpoint (str): Auth Endpoint

    ```python
    import os
    from cirro import CirroApi
    from cirro.auth.client_creds import ClientCredentialsAuth
    from cirro.config import AppConfig

    client_id = os.getenv('CIRRO_CLIENT_ID')
    client_secret = os.getenv('CIRRO_CLIENT_SECRET')

    config = AppConfig(base_url="app.cirro.bio")
    auth_info = ClientCredentialsAuth(client_id, client_secret, auth_endpoint=config.auth_endpoint)
    cirro = CirroApi(auth_info=auth_info)
    ```
    """

    def __init__(
            self,
            client_id: str,
            client_secret: str,
            auth_endpoint: str
    ):
        self._client_id = client_id
        self._client_secret = client_secret
        self._auth_endpoint = auth_endpoint
        self._token_info: Optional[OAuthTokenResponse] = None
        self._token_expiry = None
        self._username = None
        self._get_token_lock = threading.Lock()

    def get_current_user(self) -> str:
        return self._username

    def get_auth_method(self) -> AuthMethod:
        return RefreshableTokenAuth(token_getter=lambda: self._get_token()["access_token"])

    def _get_token(self) -> OAuthTokenResponse:
        with self._get_token_lock:
            # Refresh access token if expired
            if not self._token_expiry or time.time() > self._token_expiry:
                self._refresh_token()

            return self._token_info

    def _refresh_token(self):
        logger.debug("Refreshing token")
        basic_auth = base64.b64encode(
            f"{self._client_id}:{self._client_secret}".encode()
        ).decode()

        headers = {
            "Authorization": f"Basic {basic_auth}",
            "Content-Type": "application/x-www-form-urlencoded",
        }

        data = {
            "grant_type": "client_credentials",
        }

        response = requests.post(
            f"{self._auth_endpoint}/token",
            headers=headers,
            data=data,
        )
        token_info: OAuthTokenResponse = response.json()

        self._token_info = token_info

        if "access_token" not in token_info:
            raise RuntimeError(f"Error authenticating {token_info}")

        self._update_token_metadata()

    def _update_token_metadata(self):
        decoded_access_token = jwt.decode(self._token_info["access_token"],
                                          options={"verify_signature": False})
        expires_in = self._token_info.get("expires_in", 3600)
        self._token_expiry = time.time() + expires_in - 30
        self._username = decoded_access_token["appUsername"]
