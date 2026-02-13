"""
Shared mock utilities for test suite.

This module provides base utilities for creating mock objects that can work
with both direct attribute access and Pydantic validation across the test suite.
"""

import re
from unittest.mock import MagicMock


def _camel_to_snake(name: str) -> str:
    """Convert a camelCase string to snake_case."""
    s1 = re.sub("(.)([A-Z][a-z]+)", r"\1_\2", name)
    return re.sub("([a-z0-9])([A-Z])", r"\1_\2", s1).lower()


def _snake_to_camel(name: str) -> str:
    """Convert snake_case to camelCase."""
    components = name.split("_")
    return components[0] + "".join(word.capitalize() for word in components[1:])


class MockVariantCollection(list):
    """A list subclass whose ``__contains__`` always returns a fixed boolean.

    Used to control whether ``mapped_variant.variant in range.variants``
    evaluates to True or False in classification tests.
    """

    def __init__(self, contains_result: bool = True):
        super().__init__()
        self._contains_result = contains_result

    def __contains__(self, item):
        return self._contains_result


class MockObjectWithPydanticFunctionality(dict):
    """Dict subclass with snake_case attribute access for mock objects.

    Stores camelCase dict keys so Pydantic (with ``alias_generator = camelize``)
    can validate the data. Also sets snake_case attributes so source code like
    ``range.functional_classification`` works via normal attribute access.
    """

    def __init__(self, data: dict):
        super().__init__(data)
        # Bypass our custom __setattr__ during initialization
        for key, value in data.items():
            super().__setattr__(key, value)
            snake_key = _camel_to_snake(key)
            if snake_key != key:
                super().__setattr__(snake_key, value)

    def __setitem__(self, key, value):
        super().__setitem__(key, value)
        super().__setattr__(key, value)
        snake_key = _camel_to_snake(key)
        if snake_key != key:
            super().__setattr__(snake_key, value)

    def __setattr__(self, name, value):
        # Avoid recursion - use super().__setattr__ to set the attribute
        super().__setattr__(name, value)

        # Update the dict representation as well for known fields
        camel_key = _snake_to_camel(name)
        if name in self:
            super().__setitem__(name, value)
        elif camel_key in self:
            super().__setitem__(camel_key, value)


def create_sealed_mock(**attributes) -> MagicMock:
    """Create a MagicMock with specified attributes.

    Args:
        **attributes: Key-value pairs to set as attributes on the mock

    Returns:
        A MagicMock with the specified attributes
    """
    mock = MagicMock()
    for key, value in attributes.items():
        setattr(mock, key, value)
    return mock
