"""
Permission system for MaveDB entities.

This module provides a comprehensive permission system for checking user access
to various entity types including ScoreSets, Experiments, Collections, etc.

Main Functions:
    has_permission: Check if a user has permission for an action on an entity
    assert_permission: Assert permission or raise exception

Usage:
    >>> from mavedb.lib.permissions import Action, has_permission, assert_permission
    >>>
    >>> # Check permission and handle response
    >>> result = has_permission(user_data, score_set, Action.READ)
    >>> if result.permitted:
    ...     # User has access
    ...     pass
    >>>
    >>> # Assert permission (raises exception if denied)
    >>> assert_permission(user_data, score_set, Action.UPDATE)
"""

from .actions import Action
from .core import assert_permission, has_permission

__all__ = ["has_permission", "assert_permission", "Action"]
