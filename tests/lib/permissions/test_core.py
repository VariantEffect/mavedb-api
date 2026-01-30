# ruff: noqa: E402

"""Tests for core permissions functionality."""

import pytest

pytest.importorskip("fastapi", reason="Skipping permissions tests; FastAPI is required but not installed.")

from unittest.mock import Mock, patch

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
