import base64
import time
from typing import Optional

import httpx
from cirro_api_client import RefreshableTokenAuth
from cirro_api_client.cirro_auth import AuthMethod

from cirro.auth.base import AuthInfo
from cirro.auth.oauth_models import OAuthTokenResponse


class ClientCredentialsAuth(AuthInfo):
    """
    Authenticates to Cirro with the oauth client credentials

    :param client_id: Client ID
    :param client_secret: Client Secret
    :param auth_endpoint: Auth Endpoint
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

    def get_current_user(self) -> str:
        return ''

    def get_auth_method(self) -> AuthMethod:
        return RefreshableTokenAuth(token_getter=lambda: self._get_token()['access_token'])

    def _get_token(self) -> OAuthTokenResponse:
        # Return cached token if still valid
        if (
                self._token_info is not None
                and self._token_expiry is not None
                and time.time() < self._token_expiry
        ):
            return self._token_info

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
        response = httpx.post(
            f'{self._auth_endpoint}/oauth2/token',
            headers=headers,
            data=data,
            timeout=10.0,
        )
        response.raise_for_status()

        token: OAuthTokenResponse = response.json()

        expires_in = token.get("expires_in", 3600)
        self._token_info = token
        self._token_expiry = time.time() + expires_in - 30

        return token
