class DataPortalAssetNotFound(Exception):
    """Exception raised when a Data Portal Asset cannot be found."""
    pass


class DataPortalInputError(Exception):
    """Exception raised when invalid inputs are provided to the Data Portal."""
    pass
