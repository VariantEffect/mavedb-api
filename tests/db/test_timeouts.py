# ruff: noqa: E402

import logging

import pytest

pytest.importorskip("psycopg2")

from sqlalchemy import create_engine, text
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import Session
from sqlalchemy.pool import NullPool
from starlette_context import context, request_cycle_context

from mavedb.db import timeouts
from mavedb.db.timeouts import (
    CSV_STATEMENT_TIMEOUT_SECONDS,
    allow_long_statements,
    apply_api_timeouts,
    observe_statements,
)


def _show(connection_or_session, setting: str) -> str:
    return connection_or_session.execute(text(f"SHOW {setting}")).scalar()


@pytest.fixture
def engine(session):
    return session.get_bind()


@pytest.mark.unit
class TestAllowLongStatements:
    def test_raises_statement_timeout_for_the_transaction(self, session):
        allow_long_statements(session)

        assert _show(session, "statement_timeout") == f"{CSV_STATEMENT_TIMEOUT_SECONDS}s"


@pytest.mark.unit
class TestApplyApiTimeouts:
    @pytest.fixture
    def api_engine(self, engine):
        api_engine = create_engine(engine.url, poolclass=NullPool)
        apply_api_timeouts(api_engine)
        apply_api_timeouts(api_engine)  # idempotent
        return api_engine

    def test_new_connections_carry_the_api_limits(self, api_engine):
        with api_engine.connect() as conn:
            assert _show(conn, "statement_timeout") == "30s"
            assert _show(conn, "lock_timeout") == "5s"
            assert _show(conn, "idle_in_transaction_session_timeout") == "1min"

    def test_csv_override_raises_and_then_restores_the_limit(self, api_engine):
        with Session(api_engine) as session:
            allow_long_statements(session)
            assert _show(session, "statement_timeout") == f"{CSV_STATEMENT_TIMEOUT_SECONDS}s"
            session.rollback()
            assert _show(session, "statement_timeout") == "30s"

    def test_zero_disables_the_statement_timeout_everywhere(self, engine, monkeypatch):
        monkeypatch.setattr(timeouts, "API_STATEMENT_TIMEOUT_SECONDS", 0)
        disabled_engine = create_engine(engine.url, poolclass=NullPool)
        apply_api_timeouts(disabled_engine)

        with Session(disabled_engine) as session:
            assert _show(session, "statement_timeout") == "0"
            allow_long_statements(session)
            assert _show(session, "statement_timeout") == "0"

    def test_other_engines_are_unbounded(self, engine):
        with engine.connect() as conn:
            assert _show(conn, "statement_timeout") == "0"


@pytest.mark.unit
class TestObserveStatements:
    def test_logs_a_slow_statement_without_its_parameters(self, session, engine, caplog):
        observe_statements(engine, threshold_seconds=0.05)

        with caplog.at_level(logging.WARNING, logger="mavedb.db.timeouts"):
            session.execute(text("SELECT pg_sleep(0.1), :secret"), {"secret": "hunter2"})

        messages = [record.message for record in caplog.records]
        assert any("Slow database statement" in message and "pg_sleep" in message for message in messages)
        assert not any("hunter2" in message for message in messages)

    def test_does_not_log_a_fast_statement(self, session, engine, caplog):
        observe_statements(engine, threshold_seconds=5)

        with caplog.at_level(logging.WARNING, logger="mavedb.db.timeouts"):
            session.execute(text("SELECT 1"))

        assert not caplog.records

    def test_counts_each_statement_in_the_request_context(self, session, engine):
        observe_statements(engine, threshold_seconds=5)

        with request_cycle_context({}):
            session.execute(text("SELECT 1"))
            session.execute(text("SELECT 2"))
            assert context["db_statement_count"] == 2

    def test_counting_outside_a_request_is_a_no_op(self, session, engine):
        observe_statements(engine, threshold_seconds=5)

        session.execute(text("SELECT 1"))

        assert not context.exists()

    def test_logs_a_statement_cancelled_by_the_timeout(self, session, engine, caplog):
        observe_statements(engine, threshold_seconds=0.05)
        session.execute(text("SET LOCAL statement_timeout = '100ms'"))

        with caplog.at_level(logging.WARNING, logger="mavedb.db.timeouts"):
            with pytest.raises(OperationalError):
                session.execute(text("SELECT pg_sleep(1)"))

        assert any("Slow database statement" in record.message for record in caplog.records)
