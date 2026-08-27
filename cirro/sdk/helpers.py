from typing import Union

from cirro_api_client.v1.errors import UnexpectedStatus
from cirro_api_client.v1.models import ProcessDetail

from cirro.cirro_client import CirroApi
from cirro.sdk.exceptions import DataPortalInputError
from cirro.sdk.process import DataPortalProcess


def parse_process_name_or_id(process: Union[DataPortalProcess, str], client: CirroApi):
    """
    Resolve a process given as a name, an ID, or an already-built object.

    Args:
        process (str | `cirro.sdk.process.DataPortalProcess`): Name or ID of the
            process, or the process object itself (returned unchanged).
        client (`cirro.cirro_client.CirroApi`): Client to look the process up with.

    Returns:
        `cirro.sdk.process.DataPortalProcess`

    Raises:
        DataPortalInputError: if `process` is not a string, or if no process
            matches it by either ID or name.
    """

    # If the process object is already a DataPortalProcess object
    if isinstance(process, DataPortalProcess):
        return process

    # Otherwise, it should be a string
    if not isinstance(process, str):
        raise DataPortalInputError(f"Process name or ID should be a string: '{process}'")

    # Try to get the process by ID
    try:
        process_by_id = client.processes.get(process)
        if isinstance(process_by_id, ProcessDetail):
            return DataPortalProcess(process_by_id, client)

    # Catch the error if no process is found
    except UnexpectedStatus:
        pass

    # If that didn't work, try to parse it as a name
    try:
        process_by_name = client.processes.find_by_name(process)
        if isinstance(process_by_name, ProcessDetail):
            return DataPortalProcess(process_by_name, client)

    # Catch the error if no process is found
    except UnexpectedStatus:
        pass

    # If that didn't work, raise an error indicating that the process couldn't be parsed
    raise DataPortalInputError(f"Could not parse process name or id: '{process}'")
