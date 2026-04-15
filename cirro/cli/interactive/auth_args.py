import logging
from typing import Tuple, Dict

from cirro.cli.interactive.utils import ask_yes_no, ask
from cirro.config import extract_base_url

logger = logging.getLogger()


def gather_auth_config() -> Tuple[str, str, Dict]:
    base_url = ask(
        'text',
        'Enter the URL of the Cirro instance you\'d like to connect to:'
    )
    # Fix user-provided base URL, if necessary
    base_url = extract_base_url(base_url)

    auth_method_config = {
        'enable_cache': ask_yes_no('Would you like to save your login? (do not use this on shared devices)')
    }

    return 'ClientAuth', base_url, auth_method_config
