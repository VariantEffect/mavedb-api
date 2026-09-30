"""Tests for the application middleware stack.

These assert the *wiring* in ``mavedb.server_main``, not the middleware class in isolation: the defect
being guarded against is an ordering mistake, which only a test against the real stack can catch.
"""

# ruff: noqa: E402

import logging
from unittest.mock import patch

import pytest

pytest.importorskip("psycopg2")
pytest.importorskip("fastapi")

from fastapi.testclient import TestClient
from sqlalchemy.exc import OperationalError
from sqlalchemy.exc import TimeoutError as PoolTimeoutError

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


class FakePostgresError(Exception):
    """Stands in for a psycopg2 error, which carries the server's SQLSTATE as ``pgcode``."""

    def __init__(self, pgcode):
        super().__init__(pgcode)
        self.pgcode = pgcode


def postgres_error(pgcode):
    return OperationalError("SELECT 1", {}, FakePostgresError(pgcode))


@pytest.fixture
def database_error_route():
    """Register a route that raises whichever error the test sets on ``raised["error"]``."""
    raised = {}

    @app.get(BOOM_PATH)
    def boom():
        raise raised["error"]

    route = app.router.routes[-1]
    try:
        yield raised
    finally:
        app.router.routes.remove(route)


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

    def test_successful_request_is_untouched(self, slack_error):
        with TestClient(app) as tc:
            response = tc.get("/api/v1/api/version", headers={"Origin": TEST_ORIGIN})

        assert response.status_code == 200
        assert response.headers.get("access-control-allow-origin") in ("*", TEST_ORIGIN)
        slack_error.assert_not_called()


@pytest.mark.unit
class TestDatabaseTimeouts:
    @pytest.mark.parametrize(
        "error",
        [postgres_error("57014"), postgres_error("55P03"), PoolTimeoutError("QueuePool limit reached")],
        ids=["statement_timeout", "lock_timeout", "pool_timeout"],
    )
    def test_timeout_returns_503_with_cors_headers(self, database_error_route, slack_error, error):
        database_error_route["error"] = error

        with TestClient(app) as tc:
            response = tc.get(BOOM_PATH, headers={"Origin": TEST_ORIGIN})

        assert response.status_code == 503
        assert response.headers.get("access-control-allow-origin") in ("*", TEST_ORIGIN)
        assert response.json()["correlation_id"]

    def test_timeout_does_not_alert_slack(self, database_error_route, slack_error):
        database_error_route["error"] = postgres_error("57014")

        with TestClient(app) as tc:
            tc.get(BOOM_PATH)

        slack_error.assert_not_called()

    def test_other_database_errors_are_still_500(self, database_error_route, slack_error):
        database_error_route["error"] = postgres_error("23505")

        with TestClient(app) as tc:
            response = tc.get(BOOM_PATH)

        assert response.status_code == 500
        slack_error.assert_called_once()
