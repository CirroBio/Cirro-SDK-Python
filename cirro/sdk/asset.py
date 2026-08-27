import fnmatch
from abc import abstractmethod
from typing import List, TypeVar

from cirro.sdk.exceptions import DataPortalAssetNotFound, DataPortalInputError


class DataPortalAsset:
    """
    Base class used for all Data Portal Assets.

    Assets are not constructed directly -- each one is obtained from a method on
    `cirro.sdk.portal.DataPortal` or on another asset.
    """

    @property
    @abstractmethod
    def name(self):
        """Asset name"""
        pass

    def __repr__(self):
        return f'{self.__class__.__name__}(name={self.name})'


T = TypeVar('T', bound=DataPortalAsset)


class DataPortalAssets(List[T]):
    """
    A `list` of assets (projects, datasets, files, ...) with lookup helpers.

    Every `list_*` method in the SDK returns one of these rather than a plain
    list, so anything you can do with a list works, plus lookup by name or ID
    and filtering by wildcard:

    ```python
    projects = portal.list_projects()

    for project in projects:                   # ordinary list iteration
        print(project.name)

    project = projects.get_by_name("My Project")
    subset = projects.filter_by_pattern("RNA-seq*")
    print(projects.description())              # printable summary of them all
    ```
    """

    # Overridden by child classes
    asset_name = 'asset'

    def __init__(self, input_list: List[T]):
        super().__init__(input_list)

    def __str__(self):
        return "\n".join([str(i) for i in self])

    def description(self) -> str:
        """
        Render a text summary of the assets, one block per asset.
        """

        return '\n\n---\n\n'.join([
            str(i)
            for i in self
        ])

    def get_by_name(self, name: str) -> T:
        """
        Return the single item whose `name` attribute matches exactly.

        Args:
            name (str): Name to match. Matching is exact and case-sensitive;
                use `filter_by_pattern` for wildcards.

        Returns:
            The matching item.

        Raises:
            DataPortalInputError: if `name` is None, or if several items share
                the name -- in which case use `get_by_id`.
            DataPortalAssetNotFound: if nothing matches.
        """

        if name is None:
            raise DataPortalInputError(f"Must provide name to identify {self.asset_name}")

        # Get the items which have a matching name
        matching_queries = [i for i in self if i.name == name]

        # Error if no items are found
        msg = '\n'.join([f"No {self.asset_name} found with name '{name}'.", self.description()])
        if len(matching_queries) == 0:
            raise DataPortalAssetNotFound(msg)

        # Error if multiple projects are found
        msg = f"Multiple {self.asset_name} items found with name '{name}', use ID instead.\n{self.description()}"
        if len(matching_queries) > 1:
            raise DataPortalInputError(msg)

        return matching_queries[0]

    def get_by_id(self, _id: str) -> T:
        """
        Return the single item whose `id` attribute matches exactly.

        For files, the `id` is the relative path within the dataset.

        Args:
            _id (str): ID to match.

        Returns:
            The matching item.

        Raises:
            DataPortalInputError: if `_id` is None.
            DataPortalAssetNotFound: if nothing matches.
        """

        if _id is None:
            raise DataPortalInputError(f"Must provide id to identify {self.asset_name}")

        # Get the items which have a matching ID
        matching_queries = [i for i in self if i.id == _id]

        # Error if no items are found
        msg = '\n'.join([f"No {self.asset_name} found with id '{_id}'.", self.description()])
        if len(matching_queries) == 0:
            raise DataPortalAssetNotFound(msg)

        return matching_queries[0]

    def filter_by_pattern(self, pattern: str) -> 'DataPortalAssets[T]':
        """
        Return the items whose `name` matches a shell-style wildcard pattern.

        Args:
            pattern (str): Wildcard pattern, matched with `fnmatch` -- `*` for
                any run of characters, `?` for one, `[seq]` for a character set.

        Returns:
            A new collection of the same type holding the matching items, empty
            if none match.
        """

        # Get a list of the names to search against
        all_names = [i.name for i in self]

        # Filter the names by the pattern
        filtered_names = fnmatch.filter(all_names, pattern)

        # Filter the list to only include those items
        return self.__class__(
            [
                i
                for i in self
                if i.name in filtered_names
            ]
        )
