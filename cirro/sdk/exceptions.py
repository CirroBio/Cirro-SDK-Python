class DataPortalAssetNotFound(Exception):
    """Exception raised when a Data Portal Asset cannot be found."""
    pass


class DataPortalInputError(Exception):
    """Exception raised when invalid inputs are provided to the Data Portal."""
    pass


class DataPortalConflictError(Exception):
    """Exception raised when a write would overwrite a change made since the asset was read."""
    pass
