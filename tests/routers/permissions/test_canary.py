# ruff: noqa: E402
"""The canary: no route gives a planted private record to a caller who may not see it (#810).

Each swept route is called as every persona, and the raw body is searched for the markers hidden from that persona,
so JSON, CSV and NDJSON are checked the same way, wherever the filtering happens. ``canary.py`` says what is planted
and who may see it; ``route_registry.py`` says how each route is called.

These are cross-cutting sweeps, not route tests. The positive control counts a marker as reached if any read route
returns it, so it catches a rule that hides a record everywhere, such as an over-strict RLS policy, but not one route
hiding too much; each domain's visibility tests own that.
"""

import pytest

arq = pytest.importorskip("arq")
cdot = pytest.importorskip("cdot")
fastapi = pytest.importorskip("fastapi")

import json
from urllib.parse import unquote

from httpx import Response

from mavedb.routers.shared import ROUTER_BASE_PREFIX
from tests.helpers.authentication import AuthenticatingClient
from tests.routers.permissions.canary import ADMIN, ANONYMOUS, PERSONAS, Canary, Marker
from tests.routers.permissions.route_registry import READS, Call

_UNCITED_PUBLICATION = (
    "No Python filter hides a publication cited only by private records. #809's policy on publication_identifiers "
    "does, once RLS is enabled (#878)."
)
"""The reason each publication route is a known leak."""

KNOWN_LEAKS: dict[str, str] = {
    "GET /api/v1/publication-identifiers/": _UNCITED_PUBLICATION,
    "GET /api/v1/publication-identifiers/{identifier}": _UNCITED_PUBLICATION,
    "GET /api/v1/publication-identifiers/{db_name}/{identifier}": _UNCITED_PUBLICATION,
    "GET /api/v1/publication-identifiers/search/{db_name}/{identifier}": _UNCITED_PUBLICATION,
    "POST /api/v1/publication-identifiers/search": _UNCITED_PUBLICATION,
    "POST /api/v1/publication-identifiers/search/identifier": _UNCITED_PUBLICATION,
}
"""Routes that leak on today's code, with the issue that closes each. Strict, so a fix fails the test until its
entry is removed."""


def _route_params():
    for route in sorted(READS):
        marks = [pytest.mark.xfail(strict=True, reason=KNOWN_LEAKS[route])] if route in KNOWN_LEAKS else []
        yield pytest.param(route, id=route.replace(ROUTER_BASE_PREFIX, ""), marks=marks)


def _calls(canary: Canary, route: str) -> list[Call]:
    return [call if isinstance(call, Call) else Call(call) for call in READS[route].requests(canary)]


def _send(client: AuthenticatingClient, route: str, call: Call) -> Response:
    method = route.split(" ", 1)[0]
    return client.request(method, call.url, json=call.json)


def _echoed(marker: Marker, call: Call, response: Response) -> bool:
    """Whether an error response only repeats an identifier the caller sent, which tells them nothing new.

    A successful response that carries an identifier from the request served the record it names, so it still
    counts.
    """
    sent = unquote(call.url) + json.dumps(call.json)
    return response.is_error and marker.value in sent


def _leaks(canary: Canary, persona: str, route: str, call: Call, response: Response) -> list[str]:
    assert response.status_code < 500, f"{persona} {route} {call.url}: {response.status_code} {response.text}"
    return [
        f"{persona} {route} {call.url} ({response.status_code}): {marker.name}"
        for marker in canary.hidden_markers(persona)
        if marker.value in response.text and not _echoed(marker, call, response)
    ]


@pytest.mark.parametrize("route", _route_params())
def test_route_unentitled_persona_gets_no_private_marker(canary, route):
    """Every persona below admin calls the route against the planted canary and gets no 5xx and no marker it may
    not see. An error that only repeats an identifier the caller sent doesn't count as a leak."""
    leaks = []
    for call in _calls(canary, route):
        for persona in PERSONAS:
            response = _send(canary.clients[persona], route, call)
            leaks.extend(_leaks(canary, persona, route, call, response))

    assert not leaks, "Private markers reached callers who may not see them:\n" + "\n".join(leaks)


@pytest.mark.parametrize("persona", (*PERSONAS, ADMIN))
def test_read_routes_persona_receives_every_marker_it_may_see(canary, persona):
    """The persona signs in, and some read route returns it every marker it may see, public ones included. So a
    marker missing from a sweep response was withheld, not unplanted or unreachable."""
    persona_client = canary.clients[persona]
    assert persona_client.get("/api/v1/users/me").status_code == (401 if persona == ANONYMOUS else 200)
    bodies = [_send(persona_client, route, call).text for route in READS for call in _calls(canary, route)]

    allowed = [marker for marker in canary.markers if not marker.hidden_from(persona)]
    unreached = [marker.name for marker in allowed if not any(marker.value in body for body in bodies)]
    assert not unreached, f"No read route returns these markers to the {persona}: {unreached}"
