from io import StringIO
from typing import Optional

from cirro.auth.access_token import AccessTokenAuth
from cirro.auth.base import AuthInfo
from cirro.auth.client_creds import ClientCredentialsAuth
from cirro.auth.device_code import DeviceCodeAuth

__all__ = [
    'get_auth_info_from_config',
    'AuthInfo',
    'DeviceCodeAuth',
    'AccessTokenAuth',
    'ClientCredentialsAuth'
]

from cirro.config import AppConfig


def get_auth_info_from_config(app_config: AppConfig, auth_io: Optional[StringIO] = None):
    """
    Generates the AuthInfo object from the user's saved configuration
    """
    user_config = app_config.user_config
    if not user_config or not user_config.auth_method:
        return DeviceCodeAuth(region=app_config.region,
                              client_id=app_config.client_id,
                              auth_endpoint=app_config.auth_endpoint,
                              auth_io=auth_io)

    auth_methods = [
        DeviceCodeAuth,
        ClientCredentialsAuth
    ]
    matched_auth_method = next((m for m in auth_methods if m.__name__ == user_config.auth_method), None)
    if not matched_auth_method:
        # Backwards compatibility
        if user_config.auth_method == 'ClientAuth':
            matched_auth_method = DeviceCodeAuth
        else:
            raise RuntimeError(f'{user_config.auth_method} not found, please re-run configuration')

    auth_config = user_config.auth_method_config

    if matched_auth_method == DeviceCodeAuth:
        return DeviceCodeAuth(region=app_config.region,
                              client_id=app_config.client_id,
                              auth_endpoint=app_config.auth_endpoint,
                              enable_cache=auth_config.get('enable_cache') == 'True',
                              auth_io=auth_io)

    if matched_auth_method == ClientCredentialsAuth:
        return ClientCredentialsAuth(
            auth_config.get('client_id'),
            auth_config.get('client_secret'),
            auth_endpoint=app_config.auth_endpoint
        )

    return None
