"""What one entity type's rules permit a given caller to read.

Router-boundary ``assert_permission`` covers the entity a request names. A path that fans out from there
to sibling entities — a score set to its calibrations, a variant to every score set measuring the same
allele — leaves that boundary behind, and nothing in a function signature says so. A viewer is what lets
"may this caller see this?" be asked at the point of fan-out, rather than assumed to have been asked
upstream.

This module holds only the entity-agnostic behaviour. Each entity's concrete viewer lives beside that
entity's permission rules — see ``ScoreCalibrationViewer`` in ``permissions.score_calibration``. Callers
do not usually construct a viewer directly; they thread a ``Principal`` and ask it for one (see
``permissions.principal``).

Known limitation: a viewer filters entities that have already been loaded, which is correct but leaves two
gaps. It cannot constrain values *derived* from entities. A count, or a "has any calibration" boolean,
bypasses it entirely. In addition, filtering by reassigning an ORM collection is undone by any later eager load of
that relationship. The durable fix for the second is a composable SQL predicate (a reusable WHERE clause
rather than a sealed loader, so queries keep their joins), which is worth building once a second entity
needs it.
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Generic, Iterable, Optional, TypeVar

from mavedb.lib.permissions.actions import Action
from mavedb.lib.permissions.models import PermissionResponse
from mavedb.lib.types.authentication import UserData

EntityT = TypeVar("EntityT")


@dataclass(frozen=True)
class Viewer(ABC, Generic[EntityT]):
    """One entity type's read rules, bound to a caller.

    Defaults to anonymous, so a viewer constructed with no arguments admits only what any member of the
    public could already see rather than everything in the database.
    """

    user_data: Optional[UserData] = None

    _readable: dict[int, bool] = field(default_factory=dict, compare=False, repr=False)
    """Memoized READ answers, keyed by entity id.

    A fan-out re-asks the same handful of entities repeatedly. The answer cannot change within a request, 
    so it is asked once. Instances are therefore request-scoped: do not share one across requests, or an 
    entity published mid-process would keep its stale answer.
    """

    @staticmethod
    @abstractmethod
    def _has_permission(user_data: Optional[UserData], entity: EntityT, action: Action) -> PermissionResponse:
        """The permission rules for this entity type. Bound to the entity's own permission module."""

    def _is_indeterminate(self, entity: EntityT) -> bool:
        """Whether the entity cannot state its own visibility, and so must be withheld.

        Permission handlers raise on an unset ``private`` flag, and a raising permission check inside a
        streaming generator surfaces to the user as a truncated download rather than a denial. Withholding
        is the safe reading of "I don't know".
        """
        return getattr(entity, "private", False) is None

    def may_read(self, entity: EntityT) -> bool:
        """Whether this viewer is permitted to read an entity."""
        if self._is_indeterminate(entity):
            return False

        entity_id = getattr(entity, "id", None)

        # Every unsaved entity shares a null id, so caching one verdict would apply it to all of them.
        if entity_id is None:
            return self._has_permission(self.user_data, entity, Action.READ).permitted

        if entity_id not in self._readable:
            self._readable[entity_id] = self._has_permission(self.user_data, entity, Action.READ).permitted

        return self._readable[entity_id]

    def visible(self, entities: Optional[Iterable[EntityT]]) -> list[EntityT]:
        """Drop the entities this viewer may not read."""
        if not entities:
            return []

        return [entity for entity in entities if self.may_read(entity)]
