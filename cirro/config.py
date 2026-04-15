import configparser
import os
import re
from pathlib import Path
from typing import NamedTuple, Dict, Optional

import requests
from requests import RequestException


class Constants:
    home = os.environ.get('CIRRO_HOME', '~/.cirro')
    config_path = Path(home, 'config.ini').expanduser()
    default_base_url = 'cirro.bio'
    default_max_retries = 10


class UserConfig(NamedTuple):
    auth_method: str
    auth_method_config: Dict  # This needs to match the init params of the auth method
    base_url: Optional[str]
    transfer_max_retries: Optional[int]


def extract_base_url(base_url: str):
    # remove http(s):// if it's there
    base_url = re.compile(r'https?://').sub('', base_url).strip()
    # remove anything after the first slash if it's there
    base_url = base_url.split('/')[0]
    return base_url


def save_user_config(user_config: UserConfig):
    original_user_config = load_user_config()
    ini_config = configparser.ConfigParser()
    ini_config['General'] = {
        'auth_method': user_config.auth_method,
        'base_url': user_config.base_url,
        'transfer_max_retries': Constants.default_max_retries
    }
    if original_user_config:
        ini_config['General']['transfer_max_retries'] = str(original_user_config.transfer_max_retries)

    ini_config[user_config.auth_method] = user_config.auth_method_config
    Constants.config_path.parent.mkdir(exist_ok=True)
    with Constants.config_path.open('w') as configfile:
        ini_config.write(configfile)


def load_user_config() -> Optional[UserConfig]:
    if not Constants.config_path.exists():
        return None

    try:
        ini_config = configparser.ConfigParser()
        ini_config.read(str(Constants.config_path.absolute()))
        main_config = ini_config['General']
        auth_method = main_config.get('auth_method')
        base_url = main_config.get('base_url')
        transfer_max_retries = main_config.getint('transfer_max_retries', Constants.default_max_retries)

        if auth_method and ini_config.has_section(auth_method):
            auth_method_config = dict(ini_config[auth_method])
        else:
            auth_method_config = {}

        return UserConfig(
            auth_method=auth_method,
            auth_method_config=auth_method_config,
            base_url=base_url,
            transfer_max_retries=transfer_max_retries
        )
    except Exception:
        raise RuntimeError('Configuration load error, please re-run configuration')


class AppConfig:
    def __init__(self, base_url: str = None):
        self.user_config = load_user_config()
        self.base_url = (base_url or
                         os.environ.get('CIRRO_BASE_URL') or
                         (self.user_config.base_url if self.user_config else None))
        if not self.base_url:
            raise RuntimeError('No base URL provided, please run cirro configure,'
                               ' set the CIRRO_BASE_URL environment variable, or provide the base_url parameter.')
        # remove http(s):// if it's there
        self.base_url = re.compile(r'https?://').sub('', self.base_url).strip()
        self.transfer_max_retries = self.user_config.transfer_max_retries\
            if self.user_config else Constants.default_max_retries
        self._init_config()

    @property
    def checksum_method_display(self):
        return self.checksum_method.replace('NVME', '')

    @property
    def checksum_method(self):
        return 'CRC64NVME'

    def _init_config(self):
        self.rest_endpoint = f'https://{self.base_url}/api'
        self.auth_endpoint = f'https://{self.base_url}/api/auth'

        try:
            info_resp = requests.get(f'{self.rest_endpoint}/info/system')
            info_resp.raise_for_status()

            info = info_resp.json()
            self.client_id = info['auth']['sdkAppId']
            self.user_pool_id = info['auth']['userPoolId']
            self.references_bucket = info['referencesBucket']
            self.resources_bucket = info['resourcesBucket']
            self.region = info['region']
        except RequestException:
            raise RuntimeError(f'Failed connecting to {self.base_url}, please check your configuration')
