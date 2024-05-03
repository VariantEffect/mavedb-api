from enum import Enum
from typing import Optional

from mavedb.lib.authentication import UserData
from mavedb.db.base import Base
from mavedb.models.enums.user_role import UserRole
from mavedb.models.experiment import Experiment
from mavedb.models.experiment_set import ExperimentSet
from mavedb.models.score_set import ScoreSet
from mavedb.models.user import User


class Action(Enum):
    READ = 1
    UPDATE = 2
    DELETE = 3
    ADD_EXPERIMENT = 4
    ADD_SCORE_SET = 5
    SET_SCORES = 6
    ADD_ROLE = 7
    PUBLISH = 8


class PermissionResponse:
    def __init__(self, permitted: bool, http_code: int = 403, message: Optional[str] = None):
        self.permitted = permitted
        self.http_code = http_code if not permitted else None
        self.message = message if not permitted else None


class PermissionException(Exception):
    def __init__(self, http_code: int, message: str):
        self.http_code = http_code
        self.message = message


def roles_permitted(user_roles: list[UserRole], permitted_roles: list[UserRole]) -> bool:
    if not user_roles:
        return False

    return any(role in permitted_roles for role in user_roles)


def has_permission(user_data: Optional[UserData], item: Base, action: Action) -> PermissionResponse:
    private = False
    user_is_owner = False
    user_is_self = False
    active_roles = user_data.active_roles if user_data else []

    if isinstance(item, ExperimentSet) or isinstance(item, Experiment) or isinstance(item, ScoreSet):
        assert item.private is not None
        private = item.private
        published = item.published_date is not None

        user_is_owner = item.created_by_id == user_data.user.id if user_data is not None else False

    if isinstance(item, User):
        user_is_self = item.id == user_data.user.id if user_data is not None else False

    if isinstance(item, ExperimentSet):
        if action == Action.READ:
            if user_is_owner or not private:
                return PermissionResponse(True)
            # Roles which may perform this operation.
            elif roles_permitted(active_roles, [UserRole.admin]):
                return PermissionResponse(True)
            elif private:
                # Do not acknowledge the existence of a private entity.
                return PermissionResponse(False, 404, f"experiment set with URN '{item.urn}' not found")
            else:
                return PermissionResponse(False)
        elif action == Action.UPDATE:
            if user_is_owner:
                return PermissionResponse(True)
            # Roles which may perform this operation.
            elif roles_permitted(active_roles, [UserRole.admin]):
                return PermissionResponse(True)
            elif private:
                # Do not acknowledge the existence of a private entity.
                return PermissionResponse(False, 404, f"experiment set with URN '{item.urn}' not found")
            else:
                return PermissionResponse(False)
        elif action == Action.DELETE:
            # Owner may only delete an experiment set if it has not already been published.
            if user_is_owner:
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
            return PermissionResponse(
                user_is_owner or roles_permitted(active_roles, [UserRole.admin]),
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
            if user_is_owner or not private:
                return PermissionResponse(True)
            # Roles which may perform this operation.
            elif roles_permitted(active_roles, [UserRole.admin]):
                return PermissionResponse(True)
            elif private:
                # Do not acknowledge the existence of a private entity.
                return PermissionResponse(False, 404, f"experiment with URN '{item.urn}' not found")
            else:
                return PermissionResponse(False)
        elif action == Action.UPDATE:
            if user_is_owner:
                return PermissionResponse(True)
            # Roles which may perform this operation.
            elif roles_permitted(active_roles, [UserRole.admin]):
                return PermissionResponse(True)
            elif private:
                # Do not acknowledge the existence of a private entity.
                return PermissionResponse(False, 404, f"experiment with URN '{item.urn}' not found")
            else:
                return PermissionResponse(False)
        elif action == Action.DELETE:
            # Owner may only delete an experiment if it has not already been published.
            if user_is_owner:
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
            return PermissionResponse(
                (user_is_owner or roles_permitted(active_roles, [UserRole.admin])),
                404 if private else 403,
                (
                    f"experiment with URN '{item.urn}' not found"
                    if private
                    else f"insufficient permissions for URN '{item.urn}'"
                ),
            )
        else:
            raise NotImplementedError(f"has_permission(User, Experiment, {action}, Role)")

    elif isinstance(item, ScoreSet):
        if action == Action.READ:
            if user_is_owner or not private:
                return PermissionResponse(True)
            # Roles which may perform this operation.
            elif roles_permitted(active_roles, [UserRole.admin]):
                return PermissionResponse(True)
            elif private:
                # Do not acknowledge the existence of a private entity.
                return PermissionResponse(False, 404, f"score set with URN '{item.urn}' not found")
            else:
                return PermissionResponse(False)
        elif action == Action.UPDATE:
            if user_is_owner:
                return PermissionResponse(True)
            # Roles which may perform this operation.
            elif roles_permitted(active_roles, [UserRole.admin]):
                return PermissionResponse(True)
            elif private:
                # Do not acknowledge the existence of a private entity.
                return PermissionResponse(False, 404, f"score set with URN '{item.urn}' not found")
            else:
                return PermissionResponse(False)
        elif action == Action.DELETE:
            # Owner may only delete a score set if it has not already been published.
            if user_is_owner:
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
            if user_is_owner:
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
                (user_is_owner or roles_permitted(active_roles, [UserRole.admin])),
                404 if private else 403,
                (
                    f"score set with URN '{item.urn}' not found"
                    if private
                    else f"insufficient permissions for URN '{item.urn}'"
                ),
            )
        else:
            raise NotImplementedError(f"has_permission(User, ScoreSet, {action}, Role)")

    elif isinstance(item, User):
        if action == Action.READ:
            if user_is_self:
                return PermissionResponse(True)
            elif roles_permitted(active_roles, [UserRole.admin]):
                return PermissionResponse(True)
            else:
                return PermissionResponse(False)
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
    permission = has_permission(user_data, item, action)
    if not permission.permitted:
        assert permission.http_code and permission.message
        raise PermissionException(http_code=permission.http_code, message=permission.message)

    return permission
