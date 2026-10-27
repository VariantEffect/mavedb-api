"""Tests for core permissions functionality."""

from unittest.mock import Mock, patch

import pytest

from mavedb.lib.logging.context import logging_context, logging_context_scope
from mavedb.lib.permissions import (
    assert_permission,
    collection,
    experiment,
    experiment_set,
    score_calibration,
    score_set,
    user,
)
from mavedb.lib.permissions.actions import Action
from mavedb.lib.permissions.core import has_permission as core_has_permission
from mavedb.lib.permissions.exceptions import PermissionException
from mavedb.lib.permissions.models import PermissionResponse
from mavedb.models.collection import Collection
from mavedb.models.experiment import Experiment
from mavedb.models.experiment_set import ExperimentSet
from mavedb.models.score_calibration import ScoreCalibration
from mavedb.models.score_set import ScoreSet
from mavedb.models.user import User

SUPPORTED_ENTITY_TYPES = {
    ScoreSet: score_set.has_permission,
    Experiment: experiment.has_permission,
    ExperimentSet: experiment_set.has_permission,
    Collection: collection.has_permission,
    User: user.has_permission,
    ScoreCalibration: score_calibration.has_permission,
}


class TestCoreDispatcher:
    """Test the core permission dispatcher functionality."""

    @pytest.mark.parametrize("entity, handler", SUPPORTED_ENTITY_TYPES.items())
    def test_dispatcher_routes_to_correct_entity_handler(self, entity_helper, entity, handler):
        """Test that the dispatcher routes requests to the correct entity-specific handler."""
        admin_user = entity_helper.create_user_data("admin")

        with (
            patch("mavedb.lib.permissions.core.type", return_value=entity),
            patch(
                f"mavedb.lib.permissions.core.{handler.__module__.split('.')[-1]}.{handler.__name__}",
                return_value=PermissionResponse(True),
            ) as mocked_handler,
        ):
            core_has_permission(admin_user, entity, Action.READ)
            mocked_handler.assert_called_once_with(admin_user, entity, Action.READ)

    def test_dispatcher_raises_for_unsupported_entity_type(self, entity_helper):
        """Test that unsupported entity types raise NotImplementedError."""
        admin_user = entity_helper.create_user_data("admin")
        unsupported_entity = Mock()  # Some random object

        with pytest.raises(NotImplementedError) as exc_info:
            core_has_permission(admin_user, unsupported_entity, Action.READ)

        error_msg = str(exc_info.value)
        assert "not implemented" in error_msg.lower()
        assert "Mock" in error_msg  # Should mention the actual type
        assert "Supported entity types" in error_msg


class TestAssertPermission:
    """Test the assert_permission function."""

    def test_assert_permission_returns_result_when_permitted(self, entity_helper):
        """Test that assert_permission returns the PermissionResponse when access is granted."""

        with patch("mavedb.lib.permissions.core.has_permission", return_value=PermissionResponse(True)):
            user_data = entity_helper.create_user_data("admin")
            score_set = entity_helper.create_score_set("published")

            result = assert_permission(user_data, score_set, Action.READ)

        assert isinstance(result, PermissionResponse)
        assert result.permitted is True

    def test_assert_permission_raises_when_denied(self, entity_helper):
        """Test that assert_permission raises PermissionException when access is denied."""

        with (
            patch(
                "mavedb.lib.permissions.core.has_permission",
                return_value=PermissionResponse(False, http_code=404, message="Not found"),
            ),
            pytest.raises(PermissionException) as exc_info,
        ):
            user_data = entity_helper.create_user_data("admin")
            score_set = entity_helper.create_score_set("published")

            assert_permission(user_data, score_set, Action.READ)

        exception = exc_info.value
        assert hasattr(exception, "http_code")
        assert hasattr(exception, "message")
        assert exception.http_code == 404
        assert "not found" in exception.message.lower()

    @pytest.mark.parametrize(
        "http_code,message",
        [
            (403, "Forbidden"),
            (401, "Unauthorized"),
            (404, "Not Found"),
        ],
    )
    def test_assert_permission_preserves_error_details(self, entity_helper, http_code, message):
        """Test that assert_permission preserves HTTP codes and messages from permission check."""

        with (
            patch(
                "mavedb.lib.permissions.core.has_permission",
                return_value=PermissionResponse(False, http_code=http_code, message=message),
            ),
            pytest.raises(PermissionException) as exc_info,
        ):
            user_data = entity_helper.create_user_data("admin")
            score_set = entity_helper.create_score_set("published")

            assert_permission(user_data, score_set, Action.READ)

        assert exc_info.value.http_code == http_code, f"Expected {http_code} for {http_code} on {message} entity"


class TestPermissionCheckLogging:
    """Each check is recorded in the request's logging context, capped so list endpoints stay bounded."""

    SCORE_SET_URN = "urn:mavedb:00000001-a-1"

    def private_score_set(self):
        return ScoreSet(urn=self.SCORE_SET_URN, private=True, created_by_id=2, contributors=[])

    def test_has_permission_permitted_check_is_recorded(self, entity_helper):
        with logging_context_scope() as ctx:
            core_has_permission(entity_helper.create_user_data("admin"), self.private_score_set(), Action.READ)

        assert ctx["permissions.checks"] == [
            {"entity": "ScoreSet", "id": self.SCORE_SET_URN, "action": "READ", "permitted": True}
        ]
        assert ctx["permissions.checks_count"] == 1
        assert "permissions.denied" not in ctx

    def test_has_permission_denied_check_is_recorded_with_its_message(self, entity_helper):
        with logging_context_scope() as ctx:
            response = core_has_permission(None, self.private_score_set(), Action.READ)

        assert ctx["permissions.checks"][0]["permitted"] is False
        assert ctx["permissions.denied"] == [
            {
                "entity": "ScoreSet",
                "id": self.SCORE_SET_URN,
                "action": "READ",
                "permitted": False,
                "message": response.message,
            }
        ]

    def test_has_permission_entity_without_urn_is_recorded_by_id(self, entity_helper):
        with logging_context_scope() as ctx:
            core_has_permission(entity_helper.create_user_data("admin"), User(id=5, username="u"), Action.READ)

        assert ctx["permissions.checks"][0]["id"] == 5

    def test_has_permission_past_the_cap_counts_checks_and_still_records_denials(self, entity_helper):
        admin = entity_helper.create_user_data("admin")

        with logging_context_scope() as ctx:
            for _ in range(12):
                core_has_permission(admin, self.private_score_set(), Action.READ)
            core_has_permission(None, self.private_score_set(), Action.READ)

        assert len(ctx["permissions.checks"]) == 10
        assert ctx["permissions.checks_count"] == 13
        assert ctx["permissions.denied_count"] == 1

    def test_has_permission_outside_a_context_records_nothing(self, entity_helper):
        core_has_permission(entity_helper.create_user_data("admin"), self.private_score_set(), Action.READ)

        assert logging_context() == {}
