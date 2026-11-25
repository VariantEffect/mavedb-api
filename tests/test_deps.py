# ruff: noqa: E402

import time
from unittest.mock import patch

import pytest

pytest.importorskip("psycopg2")
pytest.importorskip("fastapi")

from fastapi import Depends
from fastapi.responses import StreamingResponse
from fastapi.testclient import TestClient
from sqlalchemy import text
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import Session, sessionmaker

from mavedb import deps
from mavedb.deps import get_db
from mavedb.lib.logging.context import logging_context_scope
from mavedb.server_main import app

PROBE_PATH = "/__test_get_db_!__"


def _leave_idle_past_timeout(db: Session) -> None:
    """Open a transaction and leave it idle past ``idle_in_transaction_session_timeout``, so the server ends it."""
    db.execute(text("SET idle_in_transaction_session_timeout = '50ms'"))
    db.execute(text("SELECT 1"))
    time.sleep(0.5)


@pytest.fixture
def test_session_local(session):
    """Point ``get_db`` at the test database instead of the configured one."""
    with patch.object(deps, "SessionLocal", sessionmaker(autocommit=False, autoflush=False, bind=session.get_bind())):
        yield


@pytest.fixture
def slack_error():
    with patch("mavedb.lib.middleware.errors.send_slack_error") as mocked:
        yield mocked


@pytest.fixture
def slow_streaming_route(test_session_local):
    """Register a route that reads through the real ``get_db``, then streams its body slower than the idle timeout."""

    @app.get(PROBE_PATH)
    def probe(db: Session = Depends(get_db)):
        db.execute(text("SET idle_in_transaction_session_timeout = '50ms'"))
        db.execute(text("SELECT 1"))

        def chunks():
            yield b"first chunk,"
            time.sleep(0.5)
            yield b"last chunk"

        return StreamingResponse(chunks())

    route = app.router.routes[-1]
    # TODO(#183): The old suite's `app_` fixture leaves its `get_db` override on the shared app, so set it aside.
    # Drop this once the test builds its own app with `create_app`.
    leftover_override = app.dependency_overrides.pop(get_db, None)
    try:
        yield
    finally:
        app.router.routes.remove(route)
        if leftover_override is not None:
            app.dependency_overrides[get_db] = leftover_override


@pytest.mark.integration
class TestGetDb:
    def test_get_db_session_ended_by_server_closes_without_raising(self, test_session_local):
        dependency = get_db()
        db = next(dependency)
        _leave_idle_past_timeout(db)

        with logging_context_scope() as context:
            with pytest.raises(StopIteration):
                next(dependency)

        assert context["db_session_ended_by_server"] is True
        assert not db.in_transaction()

    def test_get_db_live_session_closes_without_recording_a_drop(self, test_session_local):
        dependency = get_db()
        db = next(dependency)
        db.execute(text("SELECT 1"))

        with logging_context_scope() as context:
            with pytest.raises(StopIteration):
                next(dependency)

        assert "db_session_ended_by_server" not in context
        assert not db.in_transaction()

    def test_get_db_slow_streamed_response_completes_without_alerting(self, slow_streaming_route, slack_error):
        with TestClient(app) as tc:
            response = tc.get(PROBE_PATH)

        assert response.status_code == 200
        assert response.content == b"first chunk,last chunk"
        slack_error.assert_not_called()


@pytest.mark.unit
class TestClose:
    def test_close_error_on_a_live_connection_raises(self):
        class FailingSession:
            def close(self):
                raise OperationalError("ROLLBACK", {}, Exception("could not roll back"), connection_invalidated=False)

        with pytest.raises(OperationalError):
            deps._close(FailingSession())  # type: ignore[arg-type]
