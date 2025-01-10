import logging
from enum import Enum
from typing import Optional

from mavedb.db.base import Base
from mavedb.lib.authentication import UserData
from mavedb.lib.logging.context import logging_context, save_to_logging_context
from mavedb.models.collection import Collection
from mavedb.models.enums.contribution_role import ContributionRole
from mavedb.models.enums.user_role import UserRole
from mavedb.models.experiment import Experiment
from mavedb.models.experiment_set import ExperimentSet
from mavedb.models.score_set import ScoreSet
from mavedb.models.user import User

logger = logging.getLogger(__name__)


class Action(Enum):
    LOOKUP = "lookup"
    READ = "read"
    UPDATE = "update"
    DELETE = "delete"
    ADD_EXPERIMENT = "add_experiment"
    ADD_SCORE_SET = "add_score_set"
    SET_SCORES = "set_scores"
    ADD_ROLE = "add_role"
    PUBLISH = "publish"
    ADD_BADGE = "add_badge"


class PermissionResponse:
    def __init__(self, permitted: bool, http_code: int = 403, message: Optional[str] = None):
        self.permitted = permitted
        self.http_code = http_code if not permitted else None
        self.message = message if not permitted else None

        save_to_logging_context({"permission_message": self.message, "access_permitted": self.permitted})
        if self.permitted:
            logger.debug(
                msg="Access to the requested resource is permitted.",
                extra=logging_context(),
            )
        else:
            logger.debug(
                msg="Access to the requested resource is not permitted.",
                extra=logging_context(),
            )


class PermissionException(Exception):
    def __init__(self, http_code: int, message: str):
        self.http_code = http_code
        self.message = message


def roles_permitted(user_roles: list[UserRole], permitted_roles: list[UserRole]) -> bool:
    save_to_logging_context({"permitted_roles": [role.name for role in permitted_roles]})

    if not user_roles:
        logger.debug(msg="User has no associated roles.", extra=logging_context())
        return False

    return any(role in permitted_roles for role in user_roles)


def has_permission(user_data: Optional[UserData], item: Base, action: Action) -> PermissionResponse:
    private = False
    user_is_owner = False
    user_is_self = False
    user_may_edit = False
    user_may_view_private = False
    active_roles = user_data.active_roles if user_data else []

    if isinstance(item, ExperimentSet) or isinstance(item, Experiment) or isinstance(item, ScoreSet):
        assert item.private is not None
        private = item.private
        published = item.published_date is not None
        user_is_owner = item.created_by_id == user_data.user.id if user_data is not None else False
        user_may_edit = user_is_owner or (
            user_data is not None and user_data.user.username in [c.orcid_id for c in item.contributors]
        )

        save_to_logging_context({"resource_is_published": published})

    if isinstance(item, Collection):
        assert item.private is not None
        private = item.private
        published = item.private is False
        user_is_owner = item.created_by_id == user_data.user.id if user_data is not None else False
        admin_user_ids = set()
        editor_user_ids = set()
        viewer_user_ids = set()
        for user_association in item.user_associations:
            if user_association.contribution_role == ContributionRole.admin:
                admin_user_ids.add(user_association.user_id)
            elif user_association.contribution_role == ContributionRole.editor:
                editor_user_ids.add(user_association.user_id)
            elif user_association.contribution_role == ContributionRole.viewer:
                viewer_user_ids.add(user_association.user_id)
        user_is_admin = user_is_owner or (user_data is not None and user_data.user.id in admin_user_ids)
        user_may_edit = user_is_admin or (user_data is not None and user_data.user.id in editor_user_ids)
        user_may_view_private = user_may_edit or (user_data is not None and (user_data.user.id in viewer_user_ids))

        save_to_logging_context({"resource_is_published": published})

    if isinstance(item, User):
        user_is_self = item.id == user_data.user.id if user_data is not None else False
        user_may_edit = user_is_self

    save_to_logging_context(
        {
            "resource_is_private": private,
            "user_is_owner_of_resource": user_is_owner,
            "user_is_may_edit_resource": user_may_edit,
            "user_is_self": user_is_self,
        }
    )

    if isinstance(item, ExperimentSet):
        if action == Action.READ:
            if user_may_edit or not private:
                return PermissionResponse(True)
            # Roles which may perform this operation.
            elif roles_permitted(active_roles, [UserRole.admin, UserRole.mapper]):
                return PermissionResponse(True)
            elif private:
                # Do not acknowledge the existence of a private entity.
                return PermissionResponse(False, 404, f"experiment set with URN '{item.urn}' not found")
            elif user_data is None or user_data.user is None:
                return PermissionResponse(False, 401, f"insufficient permissions for URN '{item.urn}'")
            else:
                return PermissionResponse(False, 403, f"insufficient permissions for URN '{item.urn}'")
        elif action == Action.UPDATE:
            if user_may_edit:
                return PermissionResponse(True)
            # Roles which may perform this operation.
            elif roles_permitted(active_roles, [UserRole.admin]):
                return PermissionResponse(True)
            elif private:
                # Do not acknowledge the existence of a private entity.
                return PermissionResponse(False, 404, f"experiment set with URN '{item.urn}' not found")
            elif user_data is None or user_data.user is None:
                return PermissionResponse(False, 401, f"insufficient permissions for URN '{item.urn}'")
            else:
                return PermissionResponse(False, 403, f"insufficient permissions for URN '{item.urn}'")
        elif action == Action.DELETE:
            # Owner may only delete an experiment set if it has not already been published.
            if user_may_edit:
                return PermissionResponse(not published, 403, f"insufficient permissions for URN '{item.urn}'")
            # Roles which may perform this operation.
            elif roles_permitted(active_roles, [UserRole.admin]):
                return PermissionResponse(True)
            elif private:
                # Do not acknowledge the existence of a private entity.
                return PermissionResponse(False, 404, f"experiment set with URN '{item.urn}' not found")
            else:
                return PermissionResponse(False)
        elif action == Action.ADD_EXPERIMENT:
            # Only permitted users can add an experiment to an existing experiment set.
            return PermissionResponse(
                user_may_edit or roles_permitted(active_roles, [UserRole.admin]),
                404 if private else 403,
                (
                    f"experiment set with URN '{item.urn}' not found"
                    if private
                    else f"insufficient permissions for URN '{item.urn}'"
                ),
            )
        else:
            raise NotImplementedError(f"has_permission(User, ExperimentSet, {action}, Role)")

    elif isinstance(item, Experiment):
        if action == Action.READ:
            if user_may_edit or not private:
                return PermissionResponse(True)
            # Roles which may perform this operation.
            elif roles_permitted(active_roles, [UserRole.admin, UserRole.mapper]):
                return PermissionResponse(True)
            elif private:
                # Do not acknowledge the existence of a private entity.
                return PermissionResponse(False, 404, f"experiment with URN '{item.urn}' not found")
            elif user_data is None or user_data.user is None:
                return PermissionResponse(False, 401, f"insufficient permissions for URN '{item.urn}'")
            else:
                return PermissionResponse(False, 403, f"insufficient permissions for URN '{item.urn}'")
        elif action == Action.UPDATE:
            if user_may_edit:
                return PermissionResponse(True)
            # Roles which may perform this operation.
            elif roles_permitted(active_roles, [UserRole.admin]):
                return PermissionResponse(True)
            elif private:
                # Do not acknowledge the existence of a private entity.
                return PermissionResponse(False, 404, f"experiment with URN '{item.urn}' not found")
            elif user_data is None or user_data.user is None:
                return PermissionResponse(False, 401, f"insufficient permissions for URN '{item.urn}'")
            else:
                return PermissionResponse(False, 403, f"insufficient permissions for URN '{item.urn}'")
        elif action == Action.DELETE:
            # Owner may only delete an experiment if it has not already been published.
            if user_may_edit:
                return PermissionResponse(not published, 403, f"insufficient permissions for URN '{item.urn}'")
            # Roles which may perform this operation.
            elif roles_permitted(active_roles, [UserRole.admin]):
                return PermissionResponse(True)
            elif private:
                # Do not acknowledge the existence of a private entity.
                return PermissionResponse(False, 404, f"experiment set with URN '{item.urn}' not found")
            else:
                return PermissionResponse(False)
        elif action == Action.ADD_SCORE_SET:
            # Only permitted users can add a score set to a private experiment.
            if user_may_edit or roles_permitted(active_roles, [UserRole.admin]):
                return PermissionResponse(True)
            elif private:
                return PermissionResponse(False, 404, f"experiment with URN '{item.urn}' not found")
            # Any signed in user has permissions to add a score set to a public experiment
            elif user_data is not None:
                return PermissionResponse(True)
            else:
                return PermissionResponse(False, 403, f"insufficient permissions for URN '{item.urn}'")
        else:
            raise NotImplementedError(f"has_permission(User, Experiment, {action}, Role)")

    elif isinstance(item, ScoreSet):
        if action == Action.READ:
            if user_may_edit or not private:
                return PermissionResponse(True)
            # Roles which may perform this operation.
            elif roles_permitted(active_roles, [UserRole.admin, UserRole.mapper]):
                return PermissionResponse(True)
            elif private:
                # Do not acknowledge the existence of a private entity.
                return PermissionResponse(False, 404, f"score set with URN '{item.urn}' not found")
            elif user_data is None or user_data.user is None:
                return PermissionResponse(False, 401, f"insufficient permissions for URN '{item.urn}'")
            else:
                return PermissionResponse(False, 403, f"insufficient permissions for URN '{item.urn}'")
        elif action == Action.UPDATE:
            if user_may_edit:
                return PermissionResponse(True)
            # Roles which may perform this operation.
            elif roles_permitted(active_roles, [UserRole.admin]):
                return PermissionResponse(True)
            elif private:
                # Do not acknowledge the existence of a private entity.
                return PermissionResponse(False, 404, f"score set with URN '{item.urn}' not found")
            elif user_data is None or user_data.user is None:
                return PermissionResponse(False, 401, f"insufficient permissions for URN '{item.urn}'")
            else:
                return PermissionResponse(False, 403, f"insufficient permissions for URN '{item.urn}'")
        elif action == Action.DELETE:
            # Owner may only delete a score set if it has not already been published.
            if user_may_edit:
                return PermissionResponse(not published, 403, f"insufficient permissions for URN '{item.urn}'")
            # Roles which may perform this operation.
            elif roles_permitted(active_roles, [UserRole.admin]):
                return PermissionResponse(True)
            elif private:
                # Do not acknowledge the existence of a private entity.
                return PermissionResponse(False, 404, f"experiment set with URN '{item.urn}' not found")
            else:
                return PermissionResponse(False)
        # Only the owner may publish a private score set.
        elif action == Action.PUBLISH:
            if user_may_edit:
                return PermissionResponse(True)
            elif roles_permitted(active_roles, []):
                return PermissionResponse(True)
            elif private:
                # Do not acknowledge the existence of a private entity.
                return PermissionResponse(False, 404, f"score set with URN '{item.urn}' not found")
            else:
                return PermissionResponse(False)
        elif action == Action.SET_SCORES:
            return PermissionResponse(
                (user_may_edit or roles_permitted(active_roles, [UserRole.admin])),
                404 if private else 403,
                (
                    f"score set with URN '{item.urn}' not found"
                    if private
                    else f"insufficient permissions for URN '{item.urn}'"
                ),
            )
        else:
            raise NotImplementedError(f"has_permission(User, ScoreSet, {action}, Role)")

    elif isinstance(item, Collection):
        if action == Action.READ:
            if user_may_view_private or not private:
                return PermissionResponse(True)
            # Roles which may perform this operation.
            elif roles_permitted(active_roles, [UserRole.admin]):
                return PermissionResponse(True)
            elif private:
                # Do not acknowledge the existence of a private entity.
                return PermissionResponse(False, 404, f"collection with URN '{item.urn}' not found")
            elif user_data is None or user_data.user is None:
                return PermissionResponse(False, 401, f"insufficient permissions for URN '{item.urn}'")
            else:
                return PermissionResponse(False, 403, f"insufficient permissions for URN '{item.urn}'")
        elif action == Action.UPDATE:
            if user_may_edit:
                return PermissionResponse(True)
            # Roles which may perform this operation.
            elif roles_permitted(active_roles, [UserRole.admin]):
                return PermissionResponse(True)
            elif private and not user_may_view_private:
                # Do not acknowledge the existence of a private entity.
                return PermissionResponse(False, 404, f"score set with URN '{item.urn}' not found")
            elif user_data is None or user_data.user is None:
                return PermissionResponse(False, 401, f"insufficient permissions for URN '{item.urn}'")
            else:
                return PermissionResponse(False, 403, f"insufficient permissions for URN '{item.urn}'")
        elif action == Action.DELETE:
            # A collection may be deleted even if it has been published, as long as it is not an official collection.
            if user_is_owner:
                return PermissionResponse(
                    not item.badge_name,
                    403,
                    f"insufficient permissions for URN '{item.urn}'",
                )
            # MaveDB admins may delete official collections.
            elif roles_permitted(active_roles, [UserRole.admin]):
                return PermissionResponse(True)
            elif private and not user_may_view_private:
                # Do not acknowledge the existence of a private entity.
                return PermissionResponse(False, 404, f"collection with URN '{item.urn}' not found")
            else:
                return PermissionResponse(False)
        elif action == Action.PUBLISH:
            if user_is_admin:
                return PermissionResponse(True)
            elif roles_permitted(active_roles, []):
                return PermissionResponse(True)
            elif private and not user_may_view_private:
                # Do not acknowledge the existence of a private entity.
                return PermissionResponse(False, 404, f"score set with URN '{item.urn}' not found")
            else:
                return PermissionResponse(False)
        elif action == Action.ADD_SCORE_SET:
            # Whether the collection is private or public, only permitted users can add a score set to a collection.
            if user_may_edit or roles_permitted(active_roles, [UserRole.admin]):
                return PermissionResponse(True)
            elif private and not user_may_view_private:
                return PermissionResponse(False, 404, f"collection with URN '{item.urn}' not found")
            else:
                return PermissionResponse(False, 403, f"insufficient permissions for URN '{item.urn}'")
        elif action == Action.ADD_EXPERIMENT:
            # Only permitted users can add an experiment to an existing collection.
            return PermissionResponse(
                user_may_edit or roles_permitted(active_roles, [UserRole.admin]),
                404 if private and not user_may_view_private else 403,
                (
                    f"collection with URN '{item.urn}' not found"
                    if private and not user_may_view_private
                    else f"insufficient permissions for URN '{item.urn}'"
                ),
            )
        elif action == Action.ADD_ROLE:
            # Both collection admins and MaveDB admins can add a user to a collection role
            if user_is_admin or roles_permitted(active_roles, [UserRole.admin]):
                return PermissionResponse(True)
            else:
                return PermissionResponse(False, 403, "Insufficient permissions to add user role.")
        # only MaveDB admins may add a badge name to a collection, which makes the collection considered "official"
        elif action == Action.ADD_BADGE:
            # Roles which may perform this operation.
            if roles_permitted(active_roles, [UserRole.admin]):
                return PermissionResponse(True)
            elif private:
                # Do not acknowledge the existence of a private entity.
                return PermissionResponse(False, 404, f"collection with URN '{item.urn}' not found")
            elif user_data is None or user_data.user is None:
                return PermissionResponse(False, 401, f"insufficient permissions for URN '{item.urn}'")
            else:
                return PermissionResponse(False, 403, f"insufficient permissions for URN '{item.urn}'")
        else:
            raise NotImplementedError(f"has_permission(User, ScoreSet, {action}, Role)")

    elif isinstance(item, User):
        if action == Action.LOOKUP:
            # any existing user can look up any mavedb user by Orcid ID
            # lookup differs from read because lookup means getting the first name, last name, and orcid ID of the user,
            # while read means getting an admin view of the user's details
            if user_data is not None and user_data.user is not None:
                return PermissionResponse(True)
            else:
                # TODO is this inappropriately acknowledging the existence of the user?
                return PermissionResponse(False, 401, "Insufficient permissions for user lookup.")
        if action == Action.READ:
            if user_is_self:
                return PermissionResponse(True)
            elif roles_permitted(active_roles, [UserRole.admin]):
                return PermissionResponse(True)
            else:
                return PermissionResponse(False, 403, "Insufficient permissions for user update.")
        elif action == Action.UPDATE:
            if user_is_self:
                return PermissionResponse(True)
            elif roles_permitted(active_roles, [UserRole.admin]):
                return PermissionResponse(True)
            else:
                return PermissionResponse(False, 403, "Insufficient permissions for user update.")
        elif action == Action.ADD_ROLE:
            if roles_permitted(active_roles, [UserRole.admin]):
                return PermissionResponse(True)
            else:
                return PermissionResponse(False, 403, "Insufficient permissions to add user role.")
        elif action == Action.DELETE:
            raise NotImplementedError(f"has_permission(User, ScoreSet, {action}, Role)")
        else:
            raise NotImplementedError(f"has_permission(User, ScoreSet, {action}, Role)")

    else:
        raise NotImplementedError(f"has_permission(User, {item.__class__}, {action}, Role)")


def assert_permission(user_data: Optional[UserData], item: Base, action: Action) -> PermissionResponse:
    save_to_logging_context({"permission_boundary": action.name})
    permission = has_permission(user_data, item, action)

    if not permission.permitted:
        assert permission.http_code and permission.message
        raise PermissionException(http_code=permission.http_code, message=permission.message)

    return permission
