"""Tests for the application middleware stack.

These assert the *wiring* in ``mavedb.server_main``, not the middleware class in isolation: the defect
being guarded against is an ordering mistake, which only a test against the real stack can catch.
"""

# ruff: noqa: E402

import json
import logging
from typing import Callable
from unittest.mock import patch

import pytest

pytest.importorskip("psycopg2")
pytest.importorskip("fastapi")

from fastapi.responses import StreamingResponse
from fastapi.testclient import TestClient
from sqlalchemy.orm.exc import StaleDataError

from mavedb.db.errors import DatabaseBusy, RowSecurityViolation
from mavedb.server_main import app

BOOM_PATH = "/__test_boom_!__"
TEST_ORIGIN = "https://www.mavedb.org"


class DeliberateFailure(Exception):
    """Raised only by the probe route below."""


@pytest.fixture
def raising_route():
    """Register a route that raises, and take it back out again afterwards."""

    @app.get(BOOM_PATH)
    def boom():
        raise DeliberateFailure("oh no!")

    route = app.router.routes[-1]
    try:
        yield
    finally:
        app.router.routes.remove(route)


@pytest.fixture
def mid_stream_failure_route():
    """Register a streaming route that fails after its first chunk is sent."""

    def chunks():
        yield b"first chunk"
        raise DeliberateFailure("failed mid-stream")

    @app.get(BOOM_PATH)
    def boom():
        return StreamingResponse(chunks())

    route = app.router.routes[-1]
    try:
        yield
    finally:
        app.router.routes.remove(route)


@pytest.fixture
def mid_stream_raising_route():
    """Register a streaming route that raises whichever exception the test sets, after its first chunk."""
    raised: dict[str, Callable[[], Exception]] = {}

    def chunks():
        yield b"first chunk"
        raise raised["error"]()

    @app.get(BOOM_PATH)
    def boom():
        return StreamingResponse(chunks())

    route = app.router.routes[-1]
    try:
        yield raised
    finally:
        app.router.routes.remove(route)


@pytest.fixture
def slack_messages():
    """Capture the text of each Slack message at the webhook boundary, after the real payload is built."""
    with patch("mavedb.lib.slack.send_slack_message") as mocked:
        yield lambda: [call.args[0] for call in mocked.call_args_list]


@pytest.fixture
def slack_error():
    with patch("mavedb.lib.middleware.errors.send_slack_error") as mocked:
        yield mocked


@pytest.mark.unit
class TestCatchAllErrorMiddleware:
    def test_uncaught_exception_returns_500_with_cors_headers(self, raising_route, slack_error):
        """A browser can only read the error if the 500 passed through CORSMiddleware."""
        with TestClient(app) as tc:
            response = tc.get(BOOM_PATH, headers={"Origin": TEST_ORIGIN})

        assert response.status_code == 500
        assert response.headers.get("access-control-allow-origin") in ("*", TEST_ORIGIN)

    def test_uncaught_exception_body_is_attributable(self, raising_route, slack_error):
        with TestClient(app) as tc:
            response = tc.get(BOOM_PATH, headers={"Origin": TEST_ORIGIN})

        body = response.json()
        assert body["detail"] == "Internal server error"
        assert body["correlation_id"]

    def test_uncaught_exception_still_alerts_slack(self, raising_route, slack_error):
        with TestClient(app) as tc:
            tc.get(BOOM_PATH)

        slack_error.assert_called_once()
        assert isinstance(slack_error.call_args.kwargs["err"], DeliberateFailure)

    def test_uncaught_exception_still_logs(self, raising_route, slack_error, caplog):
        with caplog.at_level(logging.ERROR), TestClient(app) as tc:
            tc.get(BOOM_PATH)

        assert any("Uncaught exception." in record.message for record in caplog.records)

    def test_mid_stream_exception_writes_canonical_log_with_sent_status(
        self, mid_stream_failure_route, slack_error, caplog
    ):
        """The app-level handler runs outside the request context, so the middleware must log it first."""
        with caplog.at_level(logging.INFO), TestClient(app, raise_server_exceptions=False) as tc:
            tc.get(BOOM_PATH)

        canonical = [record for record in caplog.records if getattr(record, "canonical", False)]
        assert len(canonical) == 1
        assert canonical[0].response_code == 200
        assert canonical[0].path == BOOM_PATH
        assert canonical[0].captured_exception_info["type"] == "DeliberateFailure"

    def test_successful_request_is_untouched(self, slack_error):
        with TestClient(app) as tc:
            response = tc.get("/api/v1/api/version", headers={"Origin": TEST_ORIGIN})

        assert response.status_code == 200
        assert response.headers.get("access-control-allow-origin") in ("*", TEST_ORIGIN)
        slack_error.assert_not_called()


@pytest.mark.unit
class TestMidStreamExceptions:
    """Starlette wraps a mapped exception raised after the response started in a ``RuntimeError``."""

    @pytest.mark.parametrize(
        "error,alerted",
        [
            (lambda: DeliberateFailure("failed mid-stream"), True),
            (lambda: RowSecurityViolation("INSERT", {}, Exception("policy")), True),
            (lambda: DatabaseBusy("SELECT", {}, Exception("timeout")), False),
            (lambda: StaleDataError("matched 0 rows"), True),
        ],
        ids=["unmapped", "row_security_violation", "database_busy", "stale_row"],
    )
    def test_mid_stream_exception_logs_original_type_and_alerts_as_mapped(
        self, mid_stream_raising_route, slack_messages, caplog, error, alerted
    ):
        mid_stream_raising_route["error"] = error

        with caplog.at_level(logging.INFO), TestClient(app, raise_server_exceptions=False) as tc:
            response = tc.get(BOOM_PATH)

        expected_type = type(error()).__name__
        canonical = [record for record in caplog.records if getattr(record, "canonical", False)]
        assert len(canonical) == 1
        assert canonical[0].captured_exception_info["type"] == expected_type

        messages = slack_messages()
        if not alerted:
            assert messages == []
            return
        (message,) = messages
        payload = json.loads(message)
        assert payload["type"] == expected_type
        assert payload["correlation_id"] == response.headers["X-Correlation-ID"]
