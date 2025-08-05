import fnmatch
from abc import abstractmethod
from typing import List, TypeVar

from cirro.sdk.exceptions import DataPortalAssetNotFound, DataPortalInputError


class DataPortalAsset:
    """Base class used for all Data Portal Assets"""

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
    Generic class with helper functions for any group of assets (projects, datasets, etc.)
    """

    # Overridden by child classes
    asset_name = 'asset'

    def __init__(self, input_list: List[T]):
        super().__init__(input_list)

    def __str__(self):
        return "\n".join([str(i) for i in self])

    def description(self):
        """Render a text summary of the assets."""

        return '\n\n---\n\n'.join([
            str(i)
            for i in self
        ])

    def get_by_name(self, name: str) -> T:
        """Return the item which matches with name attribute."""

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
            raise DataPortalAssetNotFound(msg)

        return matching_queries[0]

    def get_by_id(self, _id: str) -> T:
        """Return the item which matches by id attribute."""

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
        """Filter the items to just those whose name attribute matches the pattern."""

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
