"""The identity a request acts on behalf of, and the viewers derived from it.

A ``Principal`` is the single thing worth threading through a fan-out read. It carries the caller rather
than any one entity's viewer, so a function that later needs to filter a second entity type does not grow a
second parameter — it asks the principal for another viewer.

See ``permissions.viewer`` for what a viewer does, and each entity's permission module for its concrete
viewer.
"""

from dataclasses import dataclass, field
from typing import Any, Optional, TypeVar

from mavedb.lib.permissions.viewer import Viewer
from mavedb.lib.types.authentication import UserData

ViewerT = TypeVar("ViewerT", bound=Viewer[Any])


@dataclass(frozen=True)
class Principal:
    """The caller a read is being served.

    Defaults to anonymous, so a caller that constructs one with no arguments serves what any member of the
    public could already see rather than everything in the database.

    Viewers are built on first use and kept so that repeated access to the same viewer type does not incur
    additional construction overhead or permission checks.

    Request-scoped. Never use a ``Principal`` as a default argument value — Python evaluates defaults once
    at import, so the instance, and every cache inside it, would be shared by all requests for the life of
    the process. An entity published mid-process would keep its stale verdict, and two callers could be
    answered from one another's cache. Take ``Optional[Principal] = None`` and build one when it is missing.
    ``test_principal.py`` enforces this by inspection.
    """

    user_data: Optional[UserData] = None

    _viewers: dict[type, Viewer[Any]] = field(default_factory=dict, compare=False, repr=False)

    def viewer_for(self, viewer_class: type[ViewerT]) -> ViewerT:
        """The viewer of the given type for this caller, built once and reused."""
        if viewer_class not in self._viewers:
            self._viewers[viewer_class] = viewer_class(self.user_data)

        # The dict is heterogeneous by design; the key recovers the value's type.
        return self._viewers[viewer_class]  # type: ignore[return-value]
