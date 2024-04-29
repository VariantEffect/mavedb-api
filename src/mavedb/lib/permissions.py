from enum import Enum
from typing import Optional

from mavedb.db.base import Base
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


class PermissionResponse:
    def __init__(self, permitted: bool, http_code: int = 403, message: Optional[str] = None):
        self.permitted = permitted
        self.http_code = http_code if not permitted else None
        self.message = message if not permitted else None


class PermissionException(Exception):
    def __init__(self, http_code: int, message: str):
        self.http_code = http_code
        self.message = message


def has_permission(user: User, item: Base, action: Action) -> PermissionResponse:
    private = False
    user_may_edit = False
    if isinstance(item, ExperimentSet) or isinstance(item, Experiment) or isinstance(item, ScoreSet):
        assert item.private is not None
        private = item.private

        user_is_owner = user is not None and item.created_by_id == user.id
        user_may_edit = user_is_owner or (user is not None and user.username in [c.orcid_id for c in item.contributors])

    if isinstance(item, ExperimentSet):
        if action == Action.READ:
            if user_may_edit or not private:
                return PermissionResponse(True)
            elif private:
                # Do not acknowledge the existence of a private entity.
                return PermissionResponse(False, 404, f"experiment set with URN '{item.urn}' not found")
            else:
                return PermissionResponse(False)
        elif action == Action.UPDATE or action == Action.DELETE:
            if user_may_edit:
                return PermissionResponse(True)
            elif private:
                # Do not acknowledge the existence of a private entity.
                return PermissionResponse(False, 404, f"experiment set with URN '{item.urn}' not found")
            else:
                return PermissionResponse(False)
        elif action == Action.ADD_EXPERIMENT:
            return PermissionResponse(
                user_may_edit,
                404 if private else 403,
                (
                    f"experiment set with URN '{item.urn}' not found"
                    if private
                    else f"insufficient permissions for URN '{item.urn}'"
                ),
            )
        else:
            raise NotImplementedError(f"has_permission(User, ExperimentSet, {action})")
    elif isinstance(item, Experiment):
        if action == Action.READ:
            if user_may_edit or not private:
                return PermissionResponse(True)
            elif private:
                # Do not acknowledge the existence of a private entity.
                return PermissionResponse(False, 404, f"experiment with URN '{item.urn}' not found")
            else:
                return PermissionResponse(False)
        elif action == Action.UPDATE or action == Action.DELETE:
            if user_may_edit:
                return PermissionResponse(True)
            elif private:
                # Do not acknowledge the existence of a private entity.
                return PermissionResponse(False, 404, f"experiment with URN '{item.urn}' not found")
            else:
                return PermissionResponse(False)
        elif action == Action.ADD_SCORE_SET:
            return PermissionResponse(
                user_may_edit,
                404 if private else 403,
                (
                    f"experiment with URN '{item.urn}' not found"
                    if private
                    else f"insufficient permissions for URN '{item.urn}'"
                ),
            )
        else:
            raise NotImplementedError(f"has_permission(User, Experiment, {action})")

    elif isinstance(item, ScoreSet):
        if action == Action.READ:
            if user_may_edit or not private:
                return PermissionResponse(True)
            elif private:
                # Do not acknowledge the existence of a private entity.
                return PermissionResponse(False, 404, f"score set with URN '{item.urn}' not found")
            else:
                return PermissionResponse(False)
        elif action == Action.UPDATE or action == Action.DELETE:
            if user_may_edit:
                return PermissionResponse(True)
            elif private:
                # Do not acknowledge the existence of a private entity.
                return PermissionResponse(False, 404, f"score set with URN '{item.urn}' not found")
            else:
                return PermissionResponse(False)
        elif action == Action.SET_SCORES:
            return PermissionResponse(
                user_may_edit,
                404 if private else 403,
                (
                    f"score set with URN '{item.urn}' not found"
                    if private
                    else f"insufficient permissions for URN '{item.urn}'"
                ),
            )
        else:
            raise NotImplementedError(f"has_permission(User, ScoreSet, {action})")
    else:
        raise NotImplementedError(f"has_permission(User, {item.__class__}, {action}")


def assert_permission(user: User, item: Base, action: Action) -> PermissionResponse:
    permission = has_permission(user, item, action)
    if not permission.permitted:
        assert permission.http_code and permission.message
        raise PermissionException(http_code=permission.http_code, message=permission.message)
    return permission
