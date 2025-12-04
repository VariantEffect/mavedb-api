from typing import Any, Callable, Optional, Union

from mavedb.lib.authentication import UserData
from mavedb.lib.logging.context import save_to_logging_context
from mavedb.lib.permissions.actions import Action
from mavedb.lib.permissions.exceptions import PermissionException
from mavedb.lib.permissions.models import PermissionResponse
from mavedb.models.collection import Collection
from mavedb.models.experiment import Experiment
from mavedb.models.experiment_set import ExperimentSet
from mavedb.models.score_calibration import ScoreCalibration
from mavedb.models.score_set import ScoreSet
from mavedb.models.user import User

# Import entity-specific permission modules
from . import (
    collection,
    experiment,
    experiment_set,
    score_calibration,
    score_set,
    user,
)

# Define the supported entity types
EntityType = Union[
    Collection,
    Experiment,
    ExperimentSet,
    ScoreCalibration,
    ScoreSet,
    User,
]


def has_permission(user_data: Optional[UserData], entity: EntityType, action: Action) -> PermissionResponse:
    """
    Main dispatcher function for permission checks across all entity types.

    This function routes permission checks to the appropriate entity-specific
    module based on the type of the entity provided. Each entity type has
    its own permission logic and supported actions.

    Args:
        user_data: The user's authentication data and roles. None for anonymous users.
        entity: The entity to check permissions for. Must be one of the supported types.
        action: The action to be performed on the entity.

    Returns:
        PermissionResponse: Contains permission result, HTTP status code, and message.

    Raises:
        NotImplementedError: If the entity type is not supported.

    Example:
        >>> from mavedb.lib.permissions.core import has_permission
        >>> from mavedb.lib.permissions.actions import Action
        >>> result = has_permission(user_data, score_set, Action.READ)
        >>> if result.permitted:
        ...     # User has permission
        ...     pass

    Note:
        This is the main entry point for all permission checks in the application.
        Each entity type delegates to its own module for specific permission logic.
    """
    # Dictionary mapping entity types to their corresponding permission modules
    entity_handlers: dict[type, Callable[[Optional[UserData], Any, Action], PermissionResponse]] = {
        Collection: collection.has_permission,
        Experiment: experiment.has_permission,
        ExperimentSet: experiment_set.has_permission,
        ScoreCalibration: score_calibration.has_permission,
        ScoreSet: score_set.has_permission,
        User: user.has_permission,
    }

    entity_type = type(entity)

    if entity_type not in entity_handlers:
        supported_types = ", ".join(cls.__name__ for cls in entity_handlers.keys())
        raise NotImplementedError(
            f"Permission checks are not implemented for entity type '{entity_type.__name__}'. "
            f"Supported entity types: {supported_types}"
        )

    handler = entity_handlers[entity_type]
    return handler(user_data, entity, action)


def assert_permission(user_data: Optional[UserData], entity: EntityType, action: Action) -> PermissionResponse:
    """
    Assert that a user has permission to perform an action on an entity.

    This function checks permissions and raises an exception if the user lacks
    the necessary permissions. It's a convenience wrapper around has_permission
    for cases where you want to fail fast on permission denials.

    Args:
        user_data: The user's authentication data and roles. None for anonymous users.
        entity: The entity to check permissions for.
        action: The action to be performed on the entity.

    Returns:
        PermissionResponse: The permission result if access is granted.

    Raises:
        PermissionException: If the user lacks sufficient permissions.

    Example:
        >>> from mavedb.lib.permissions.core import assert_permission
        >>> from mavedb.lib.permissions.actions import Action
        >>> # This will raise PermissionException if user can't update
        >>> assert_permission(user_data, score_set, Action.UPDATE)
    """
    save_to_logging_context({"permission_boundary": action.name})
    permission = has_permission(user_data, entity, action)

    if not permission.permitted:
        http_code = permission.http_code if permission.http_code is not None else 403
        message = permission.message if permission.message is not None else "Permission denied"
        raise PermissionException(http_code=http_code, message=message)

    return permission
