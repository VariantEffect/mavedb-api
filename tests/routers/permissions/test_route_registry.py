# ruff: noqa: E402
"""The completeness tripwire: every route is swept by the canary or exempt with a reason (#810)."""

import pytest

arq = pytest.importorskip("arq")
cdot = pytest.importorskip("cdot")
fastapi = pytest.importorskip("fastapi")

from fastapi.routing import iter_route_contexts

from mavedb.server_main import app
from tests.routers.permissions.route_registry import ROUTES, Exempt, Write

_IMPLICIT_METHODS = {"HEAD", "OPTIONS"}
"""Methods Starlette answers for every route on its own, so they aren't routes of their own."""


def _served_routes() -> set[str]:
    # The app includes routers lazily, so app.routes lists routers, not the routes inside them.
    return {
        f"{method} {context.path_format}"
        for context in iter_route_contexts(app.routes)
        for method in (context.methods or ())
        if method not in _IMPLICIT_METHODS
    }


def test_route_registry_unlisted_route_is_missing():
    """Every route the app serves, whatever its method, is in the registry, so a new route can't skip the canary."""
    unlisted = _served_routes() - ROUTES.keys()

    assert not unlisted, (
        "Routes the canary neither sweeps nor exempts. Add each to ROUTES in "
        f"tests/routers/permissions/route_registry.py as a Read, a Write or an Exempt: {sorted(unlisted)}"
    )


def test_route_registry_listed_route_is_served():
    """Every route in the registry is still served, so the registry holds no entries for routes that are gone."""
    stale = ROUTES.keys() - _served_routes()

    assert not stale, f"ROUTES lists routes the api no longer serves: {sorted(stale)}"


def test_route_registry_unswept_route_gives_a_reason():
    """Every exempt route says why the canary can't reach a private record through it, and every write says what
    its response can carry, for when writes are swept."""
    unexplained = [
        route
        for route, entry in ROUTES.items()
        if (isinstance(entry, Exempt) and not entry.reason.strip())
        or (isinstance(entry, Write) and not entry.carries.strip())
    ]

    assert not unexplained, f"Unswept routes without a reason: {unexplained}"
