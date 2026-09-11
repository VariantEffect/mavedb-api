# ruff: noqa: E402

"""Tests for the generic Viewer contract.

Every concrete viewer inherits its caching, its fail-closed behaviour and its default audience from the base
class, so those are exercised here once against a stand-in entity rather than once per entity type. A
concrete viewer's own rules belong with that entity's permission tests.
"""

import pytest

pytest.importorskip("fastapi", reason="Skipping permissions tests; FastAPI is required but not installed.")

from dataclasses import dataclass
from types import SimpleNamespace
from typing import Any, Optional
from unittest.mock import Mock

from mavedb.lib.permissions.actions import Action
from mavedb.lib.permissions.models import PermissionResponse
from mavedb.lib.permissions.viewer import Viewer
from mavedb.lib.types.authentication import UserData

_PERMISSION_CALLS: list[tuple[Optional[UserData], Any, Action]] = []
"""Every call the fake viewer's rules received, so the base class's caching can be asserted on directly."""


def _entity(entity_id: Optional[int] = 1, *, permitted: bool = True, private: Optional[bool] = False, owner=None):
    """A stand-in entity.

    ``private`` and ``permitted`` are separate because the base class reads the former (an unset flag means
    the entity cannot state its own visibility) while the latter is the verdict a subclass's rules return.
    """
    return SimpleNamespace(id=entity_id, permitted=permitted, private=private, owner=owner)


@dataclass(frozen=True)
class _FakeViewer(Viewer[SimpleNamespace]):
    """A viewer whose rules are whatever the entity was built to say."""

    @staticmethod
    def _has_permission(user_data: Optional[UserData], entity: SimpleNamespace, action: Action) -> PermissionResponse:
        _PERMISSION_CALLS.append((user_data, entity, action))
        return PermissionResponse(entity.permitted or (user_data is not None and user_data is entity.owner))


@pytest.fixture(autouse=True)
def _reset_permission_calls():
    _PERMISSION_CALLS.clear()


class TestViewerDefaults:
    def test_a_viewer_is_anonymous_unless_given_a_caller(self) -> None:
        assert _FakeViewer().user_data is None

    def test_the_caller_is_threaded_through_to_the_rules(self) -> None:
        user_data = Mock()

        _FakeViewer(user_data).may_read(_entity())

        assert [(call_user_data, action) for call_user_data, _, action in _PERMISSION_CALLS] == [
            (user_data, Action.READ)
        ]


class TestViewerFailsClosed:
    def test_an_unset_private_flag_is_withheld(self) -> None:
        # has_permission raises on an unset `private`, and a raising permission check inside a streaming
        # generator surfaces as a truncated download rather than a denial. Fail closed instead.
        assert _FakeViewer().may_read(_entity(private=None)) is False

    def test_the_rules_are_not_consulted_for_an_indeterminate_entity(self) -> None:
        _FakeViewer().may_read(_entity(private=None))

        assert _PERMISSION_CALLS == []

    def test_an_entity_with_no_private_flag_is_not_indeterminate(self) -> None:
        # Not every entity type carries a `private` column; its absence must not read as "unknown".
        assert _FakeViewer().may_read(SimpleNamespace(id=1, permitted=True, owner=None)) is True


class TestViewerMemoization:
    def test_the_same_entity_is_asked_about_only_once(self) -> None:
        # A fan-out re-asks the same handful of entities once per record. Without memoization that is one
        # permission check, and one logging-context write, per record.
        entity = _entity()
        viewer = _FakeViewer()

        for _ in range(5):
            viewer.may_read(entity)

        assert len(_PERMISSION_CALLS) == 1

    def test_memoization_does_not_conflate_distinct_entities(self) -> None:
        viewer = _FakeViewer()

        assert viewer.may_read(_entity(1, permitted=True)) is True
        assert viewer.may_read(_entity(2, permitted=False)) is False

    def test_an_unsaved_entity_is_not_memoized_under_a_null_id(self) -> None:
        # Two distinct unsaved entities both have id None; caching either answer would leak one's verdict
        # onto the other.
        viewer = _FakeViewer()

        assert viewer.may_read(_entity(None, permitted=True)) is True
        assert viewer.may_read(_entity(None, permitted=False)) is False

    def test_one_viewers_answer_does_not_leak_to_another(self) -> None:
        # The memo is a per-instance field. A shared one would answer each caller out of the last one's cache.
        owner = Mock()
        entity = _entity(permitted=False, owner=owner)

        assert _FakeViewer(owner).may_read(entity) is True
        assert _FakeViewer().may_read(entity) is False


class TestViewerVisible:
    def test_visible_drops_the_entities_the_viewer_may_not_read(self) -> None:
        readable, unreadable = _entity(1, permitted=True), _entity(2, permitted=False)

        assert _FakeViewer().visible([readable, unreadable]) == [readable]

    @pytest.mark.parametrize("entities", [None, []], ids=["none", "empty"])
    def test_visible_tolerates_having_nothing_to_filter(self, entities) -> None:
        assert _FakeViewer().visible(entities) == []
