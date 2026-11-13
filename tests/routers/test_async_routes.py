# ruff: noqa: E402
"""Keep synchronous database work off the event loop.

FastAPI runs ``async def`` endpoints and dependencies on the event loop, so one that uses the synchronous
database session stalls every request on its process (#881, #950). These tests walk the app's routes and
fail when an async endpoint or dependency takes a session from ``get_db``. A sync dependency that uses the
session is fine: FastAPI runs it in the thread pool.
"""

import inspect
from typing import Any, Callable

import pytest

pytest.importorskip("fastapi")

from fastapi.dependencies.models import Dependant
from fastapi.routing import APIRoute

from mavedb import deps
from mavedb.routers import score_sets
from mavedb.server_main import app

# These read the multipart form, which needs an await, and run everything after it in the thread pool.
ASYNC_ENDPOINTS_ALLOWED_TO_USE_DB: set[Callable[..., Any]] = {
    score_sets.upload_score_set_variant_data,
    score_sets.update_score_set_with_variants,
}


def _is_async(call: Any) -> bool:
    if inspect.isroutine(call):
        return inspect.iscoroutinefunction(call)
    # Callable class instances, such as the JWT bearer dependency.
    return inspect.iscoroutinefunction(getattr(call, "__call__", None))


# The app has 161 routes; a walk that finds far fewer has stopped seeing them.
MIN_EXPECTED_ROUTES = 150


def _takes_db(dependant: Dependant) -> bool:
    return any(sub.call is deps.get_db for sub in dependant.dependencies)


def _async_dependencies_taking_db(dependant: Dependant) -> list[str]:
    found = []
    for sub in dependant.dependencies:
        if _is_async(sub.call) and _takes_db(sub):
            found.append(getattr(sub.call, "__qualname__", type(sub.call).__qualname__))
        found.extend(_async_dependencies_taking_db(sub))
    return found


def _api_routes(routes: list[Any]) -> list[APIRoute]:
    # FastAPI keeps each included router behind a wrapper instead of copying its routes into the app.
    found = []
    for route in routes:
        included = getattr(route, "original_router", None)
        if included is not None:
            found.extend(_api_routes(included.routes))
        elif isinstance(route, APIRoute):
            found.append(route)
    return found


@pytest.mark.unit
def test_app_routes_using_db_are_not_async():
    routes = _api_routes(app.routes)
    assert len(routes) >= MIN_EXPECTED_ROUTES, f"Found only {len(routes)} routes; the walk no longer sees them all"

    offenders = []
    for route in routes:
        label = f"{sorted(route.methods)} {route.path}"
        if (
            _is_async(route.endpoint)
            and _takes_db(route.dependant)
            and route.endpoint not in ASYNC_ENDPOINTS_ALLOWED_TO_USE_DB
        ):
            offenders.append(f"{label}: async endpoint {route.endpoint.__qualname__} uses get_db")
        offenders.extend(
            f"{label}: async dependency {name} uses get_db" for name in _async_dependencies_taking_db(route.dependant)
        )
    offenders.extend(
        f"app-wide async dependency {dependency.dependency.__qualname__} uses get_db"
        for dependency in app.router.dependencies
        if _is_async(dependency.dependency)
        and any(
            getattr(param.default, "dependency", None) is deps.get_db
            for param in inspect.signature(dependency.dependency).parameters.values()
        )
    )

    assert offenders == [], "Make these `def`, or move their database work into the thread pool:\n" + "\n".join(
        offenders
    )


@pytest.mark.unit
def test_async_endpoints_allowed_to_use_db_are_registered_async_and_use_db():
    routes_by_endpoint = {route.endpoint: route for route in _api_routes(app.routes)}

    for endpoint in ASYNC_ENDPOINTS_ALLOWED_TO_USE_DB:
        route = routes_by_endpoint.get(endpoint)
        assert route is not None, f"{endpoint.__qualname__} is no longer a route; drop it from the allowlist"
        assert _is_async(endpoint), f"{endpoint.__qualname__} is no longer async; drop it from the allowlist"
        assert _takes_db(route.dependant), f"{endpoint.__qualname__} no longer uses get_db"
