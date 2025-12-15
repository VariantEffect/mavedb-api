from typing import Optional

from mavedb.lib.authentication import UserData
from mavedb.lib.logging.context import save_to_logging_context
from mavedb.lib.permissions.actions import Action
from mavedb.lib.permissions.models import PermissionResponse
from mavedb.lib.permissions.utils import deny_action_for_entity, roles_permitted
from mavedb.models.collection import Collection
from mavedb.models.enums.contribution_role import ContributionRole
from mavedb.models.enums.user_role import UserRole


def has_permission(user_data: Optional[UserData], entity: Collection, action: Action) -> PermissionResponse:
    """
    Check if a user has permission to perform an action on a Collection entity.

    This function evaluates user permissions based on Collection role associations,
    ownership, and user roles. Collections use a special permission model with
    role-based user associations.

    Args:
        user_data: The user's authentication data and roles. None for anonymous users.
        entity: The Collection entity to check permissions for.
        action: The action to be performed (READ, UPDATE, DELETE, ADD_EXPERIMENT, ADD_SCORE_SET, ADD_ROLE, ADD_BADGE).

    Returns:
        PermissionResponse: Contains permission result, HTTP status code, and message.

    Raises:
        ValueError: If the entity's private attribute is not set.
        NotImplementedError: If the action is not supported for Collection entities.

    Note:
        Collections use CollectionUserAssociation objects to define user roles
        (admin, editor, viewer) rather than simple contributor lists.
    """
    if entity.private is None:
        raise ValueError("Collection entity must have 'private' attribute set for permission checks.")

    user_is_owner = False
    collection_roles = []
    active_roles = []

    if user_data is not None:
        user_is_owner = entity.created_by_id == user_data.user.id

        # Find the user's collection roles in this collection through user_associations.
        user_associations = [assoc for assoc in entity.user_associations if assoc.user_id == user_data.user.id]
        if user_associations:
            collection_roles = [assoc.contribution_role for assoc in user_associations]

        active_roles = user_data.active_roles

    save_to_logging_context(
        {
            "resource_is_private": entity.private,
            "user_is_owner": user_is_owner,
            "collection_roles": [role.value for role in collection_roles] if collection_roles else None,
        }
    )

    handlers = {
        Action.READ: _handle_read_action,
        Action.UPDATE: _handle_update_action,
        Action.DELETE: _handle_delete_action,
        Action.PUBLISH: _handle_publish_action,
        Action.ADD_EXPERIMENT: _handle_add_experiment_action,
        Action.ADD_SCORE_SET: _handle_add_score_set_action,
        Action.ADD_ROLE: _handle_add_role_action,
        Action.ADD_BADGE: _handle_add_badge_action,
    }

    if action not in handlers:
        supported_actions = ", ".join(a.value for a in handlers.keys())
        raise NotImplementedError(
            f"Action '{action.value}' is not supported for collection entities. "
            f"Supported actions: {supported_actions}"
        )

    return handlers[action](
        user_data,
        entity,
        entity.private,
        entity.badge_name is not None,
        user_is_owner,
        collection_roles,
        active_roles,
    )


def _handle_read_action(
    user_data: Optional[UserData],
    entity: Collection,
    private: bool,
    official_collection: bool,
    user_is_owner: bool,
    collection_roles: list[ContributionRole],
    active_roles: list[UserRole],
) -> PermissionResponse:
    """
    Handle READ action permission check for Collection entities.

    Public Collections are readable by anyone. Private Collections are only readable
    by users with Collection roles, owners, admins, and mappers.

    Args:
        user_data: The user's authentication data.
        entity: The Collection entity being accessed.
        private: Whether the Collection is private.
        official_collection: Whether the Collection is an official collection.
        user_is_owner: Whether the user owns the Collection.
        collection_roles: The user's roles in this Collection (admin/editor/viewer).
        active_roles: List of the user's active roles.

    Returns:
        PermissionResponse: Permission result with appropriate HTTP status.
    """
    ## Allow read access under the following conditions:
    # Any user may read a non-private collection.
    if not private:
        return PermissionResponse(True)
    # The owner may read a private collection.
    if user_is_owner:
        return PermissionResponse(True)
    # Collection role holders may read a private collection.
    if roles_permitted(collection_roles, [ContributionRole.admin, ContributionRole.editor, ContributionRole.viewer]):
        return PermissionResponse(True)
    # Users with these specific roles may read a private collection.
    if roles_permitted(active_roles, [UserRole.admin]):
        return PermissionResponse(True)

    return deny_action_for_entity(entity, private, user_data, bool(collection_roles) or user_is_owner, "collection")


def _handle_update_action(
    user_data: Optional[UserData],
    entity: Collection,
    private: bool,
    official_collection: bool,
    user_is_owner: bool,
    collection_roles: list[ContributionRole],
    active_roles: list[UserRole],
) -> PermissionResponse:
    """
    Handle UPDATE action permission check for Collection entities.

    Only owners, Collection admins/editors, and system admins can update Collections.

    Args:
        user_data: The user's authentication data.
        entity: The Collection entity being updated.
        private: Whether the Collection is private.
        official_collection: Whether the Collection is an official collection.
        user_is_owner: Whether the user owns the Collection.
        collection_roles: The user's roles in this Collection (admin/editor/viewer).
        active_roles: List of the user's active roles.

    Returns:
        PermissionResponse: Permission result with appropriate HTTP status.
    """
    ## Allow update access under the following conditions:
    # The owner may update the collection.
    if user_is_owner:
        return PermissionResponse(True)
    # Collection admins and editors may update the collection.
    if roles_permitted(collection_roles, [ContributionRole.admin, ContributionRole.editor]):
        return PermissionResponse(True)
    # Users with these specific roles may update the collection.
    if roles_permitted(active_roles, [UserRole.admin]):
        return PermissionResponse(True)

    return deny_action_for_entity(entity, private, user_data, bool(collection_roles) or user_is_owner, "collection")


def _handle_delete_action(
    user_data: Optional[UserData],
    entity: Collection,
    private: bool,
    official_collection: bool,
    user_is_owner: bool,
    collection_roles: list[ContributionRole],
    active_roles: list[UserRole],
) -> PermissionResponse:
    """
    Handle DELETE action permission check for Collection entities.

    System admins can delete any Collection. Owners and Collection admins can only
    delete unpublished Collections.

    Args:
        user_data: The user's authentication data.
        entity: The Collection entity being deleted.
        private: Whether the Collection is private.
        official_collection: Whether the Collection is official.
        user_is_owner: Whether the user owns the Collection.
        collection_roles: The user's roles in this Collection (admin/editor/viewer).
        active_roles: List of the user's active roles.

    Returns:
        PermissionResponse: Permission result with appropriate HTTP status.
    """
    ## Allow delete access under the following conditions:
    # System admins may delete any collection.
    if roles_permitted(active_roles, [UserRole.admin]):
        return PermissionResponse(True)
    # Other users may only delete non-official collections.
    if not official_collection:
        # Owners may delete a collection only if it is still private.
        # Collection admins/editors/viewers may not delete collections.
        if user_is_owner and private:
            return PermissionResponse(True)

    return deny_action_for_entity(entity, private, user_data, bool(collection_roles) or user_is_owner, "collection")


def _handle_publish_action(
    user_data: Optional[UserData],
    entity: Collection,
    private: bool,
    official_collection: bool,
    user_is_owner: bool,
    collection_roles: list[ContributionRole],
    active_roles: list[UserRole],
) -> PermissionResponse:
    """
    Handle PUBLISH action permission check for Collection entities.

    Only owners, Collection admins, and system admins can publish Collections.

    Args:
        user_data: The user's authentication data.
        entity: The Collection entity being published.
        private: Whether the Collection is private.
        official_collection: Whether the Collection is official.
        user_is_owner: Whether the user owns the Collection.
        collection_roles: The user's roles in this Collection (admin/editor/viewer).
        active_roles: List of the user's active roles.
    Returns:
        PermissionResponse: Permission result with appropriate HTTP status.
    """
    ## Allow publish access under the following conditions:
    # The owner may publish a collection.
    if user_is_owner:
        return PermissionResponse(True)
    # Collection admins may publish the collection.
    if roles_permitted(collection_roles, [ContributionRole.admin]):
        return PermissionResponse(True)
    # Users with these specific roles may publish the collection.
    if roles_permitted(active_roles, [UserRole.admin]):
        return PermissionResponse(True)

    return deny_action_for_entity(entity, private, user_data, bool(collection_roles) or user_is_owner, "collection")


def _handle_add_experiment_action(
    user_data: Optional[UserData],
    entity: Collection,
    private: bool,
    official_collection: bool,
    user_is_owner: bool,
    collection_roles: list[ContributionRole],
    active_roles: list[UserRole],
) -> PermissionResponse:
    """
    Handle ADD_EXPERIMENT action permission check for Collection entities.

    Only owners, Collection admins/editors, and system admins can add experiment sets
    to private Collections. Any authenticated user can add to public Collections.

    Args:
        user_data: The user's authentication data.
        entity: The Collection entity to add an experiment to.
        private: Whether the Collection is private.
        official_collection: Whether the Collection is official.
        user_is_owner: Whether the user owns the Collection.
        collection_roles: The user's roles in this Collection (admin/editor/viewer).
        active_roles: List of the user's active roles.

    Returns:
        PermissionResponse: Permission result with appropriate HTTP status.
    """
    ## Allow add experiment add access under the following conditions:
    # The owner may add an experiment to a private collection.
    if user_is_owner:
        return PermissionResponse(True)
    # Collection admins/editors may add an experiment to the collection.
    if roles_permitted(collection_roles, [ContributionRole.admin, ContributionRole.editor]):
        return PermissionResponse(True)
    # Users with these specific roles may add an experiment to the collection.
    if roles_permitted(active_roles, [UserRole.admin]):
        return PermissionResponse(True)

    return deny_action_for_entity(entity, private, user_data, bool(collection_roles) or user_is_owner, "collection")


def _handle_add_score_set_action(
    user_data: Optional[UserData],
    entity: Collection,
    private: bool,
    official_collection: bool,
    user_is_owner: bool,
    collection_roles: list[ContributionRole],
    active_roles: list[UserRole],
) -> PermissionResponse:
    """
    Handle ADD_SCORE_SET action permission check for Collection entities.

    Only owners, Collection admins/editors, and system admins can add score sets
    to private Collections. Any authenticated user can add to public Collections.

    Args:
        user_data: The user's authentication data.
        entity: The Collection entity to add a score set to.
        private: Whether the Collection is private.
        official_collection: Whether the Collection is official.
        user_is_owner: Whether the user owns the Collection.
        collection_roles: The user's roles in this Collection (admin/editor/viewer).
        active_roles: List of the user's active roles.
    Returns:
        PermissionResponse: Permission result with appropriate HTTP status.
    """
    ## Allow add score set access under the following conditions:
    # The owner may add a score set to a private collection.
    if user_is_owner:
        return PermissionResponse(True)
    # Collection admins/editors may add a score set to the collection.
    if roles_permitted(collection_roles, [ContributionRole.admin, ContributionRole.editor]):
        return PermissionResponse(True)
    # Users with these specific roles may add a score set to the collection.
    if roles_permitted(active_roles, [UserRole.admin]):
        return PermissionResponse(True)

    return deny_action_for_entity(entity, private, user_data, bool(collection_roles) or user_is_owner, "collection")


def _handle_add_role_action(
    user_data: Optional[UserData],
    entity: Collection,
    private: bool,
    official_collection: bool,
    user_is_owner: bool,
    collection_roles: list[ContributionRole],
    active_roles: list[UserRole],
) -> PermissionResponse:
    """
    Handle ADD_ROLE action permission check for Collection entities.

    Only owners and Collection admins can add roles to Collections.

    Args:
        user_data: The user's authentication data.
        entity: The Collection entity to add a role to.
        private: Whether the Collection is private.
        official_collection: Whether the Collection is official.
        user_is_owner: Whether the user owns the Collection.
        collection_roles: The user's roles in this Collection (admin/editor/viewer).
        active_roles: List of the user's active roles.
    Returns:
        PermissionResponse: Permission result with appropriate HTTP status.
    """
    ## Allow add role access under the following conditions:
    # The owner may add a role.
    if user_is_owner:
        return PermissionResponse(True)
    # Collection admins may add a role to the collection.
    if roles_permitted(collection_roles, [ContributionRole.admin]):
        return PermissionResponse(True)
    # Users with these specific roles may add a role to the collection.
    if roles_permitted(active_roles, [UserRole.admin]):
        return PermissionResponse(True)

    return deny_action_for_entity(entity, private, user_data, bool(collection_roles) or user_is_owner, "collection")


def _handle_add_badge_action(
    user_data: Optional[UserData],
    entity: Collection,
    private: bool,
    official_collection: bool,
    user_is_owner: bool,
    collection_roles: list[ContributionRole],
    active_roles: list[UserRole],
) -> PermissionResponse:
    """
    Handle ADD_BADGE action permission check for Collection entities.

    Only system admins can add badges to Collections.

    Args:
        user_data: The user's authentication data.
        entity: The Collection entity to add a badge to.
        private: Whether the Collection is private.
        official_collection: Whether the Collection is official.
        user_is_owner: Whether the user owns the Collection.
        collection_roles: The user's roles in this Collection (admin/editor/viewer).
        active_roles: List of the user's active roles.
    Returns:
        PermissionResponse: Permission result with appropriate HTTP status.
    """
    ## Allow add badge access under the following conditions:
    # Users with these specific roles may add a badge to the collection.
    if roles_permitted(active_roles, [UserRole.admin]):
        return PermissionResponse(True)

    return deny_action_for_entity(entity, private, user_data, bool(collection_roles) or user_is_owner, "collection")
