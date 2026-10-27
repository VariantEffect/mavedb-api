"""Tests for the middleware that opens the logging context for each request."""

# ruff: noqa: E402

import re
from unittest.mock import patch

import pytest

pytest.importorskip("starlette")

from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import JSONResponse
from starlette.routing import Route
from starlette.testclient import TestClient

from mavedb import __project__, __version__
from mavedb.lib.logging.context import logging_context, save_to_logging_context
from mavedb.lib.logging.models import Source
from mavedb.lib.middleware.request_context import RequestContextMiddleware

UUID_HEX = re.compile(r"^[0-9a-f]{32}$")


def echo_context(request: Request) -> JSONResponse:
    save_to_logging_context({"saved_by_route": True})
    return JSONResponse({k: str(v) for k, v in logging_context().items()})


@pytest.fixture
def client():
    app = Starlette(routes=[Route("/echo", echo_context, methods=["GET", "POST"])])
    app.add_middleware(RequestContextMiddleware)
    return TestClient(app)


def test_seeds_the_context_with_the_request_identity(client):
    body = client.post("/echo", headers={"User-Agent": "probe/1.0"}).json()

    assert body["path"] == "/echo"
    assert body["method"] == "POST"
    assert body["User-Agent"] == "probe/1.0"
    assert body["application"] == __project__
    assert body["version"] == __version__
    assert body["source"] == str(Source.other)
    assert body["saved_by_route"] == "True"


def test_echoes_the_context_ids_on_the_response(client):
    response = client.get("/echo")
    body = response.json()

    for header in ("X-Correlation-ID", "X-Request-ID"):
        assert UUID_HEX.match(response.headers[header])
        assert response.headers[header] == body[header]


def test_mints_fresh_ids_for_every_request(client):
    first = client.get("/echo", headers={"X-Correlation-ID": "a" * 32})
    second = client.get("/echo")

    assert first.headers["X-Correlation-ID"] != "a" * 32
    assert first.headers["X-Correlation-ID"] != second.headers["X-Correlation-ID"]
    assert first.headers["X-Request-ID"] != second.headers["X-Request-ID"]


@pytest.mark.parametrize(
    "headers, expected",
    [
        ({"Origin": "https://ui.example"}, Source.web),
        ({"Referer": "https://api.example/docs"}, Source.docs),
    ],
)
def test_classifies_the_request_source(client, headers, expected):
    with (
        patch("mavedb.lib.middleware.request_context.FRONTEND_URL", "https://ui.example"),
        patch("mavedb.lib.middleware.request_context.API_URL", "https://api.example"),
    ):
        body = client.get("/echo", headers=headers).json()

    assert body["source"] == str(expected)
