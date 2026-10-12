"""Database timeouts for the API.

The API sets its limits on each connection it opens, so the worker, scripts and migrations, which share the
same login and legitimately run long statements, are unaffected. Prod connects to RDS directly; behind RDS
Proxy these session-level ``SET``s would pin every connection and belong on the login instead.
"""

import logging
import os
import time

from sqlalchemy import event, text
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session

from mavedb.lib.logging.context import increment_logging_context, logging_context

logger = logging.getLogger(__name__)

# Each limit is in seconds and can be overridden from the environment; 0 disables it, as in Postgres.
# The prod API load balancer's idle timeout is 60s. A statement's limit plus the 10s pool wait stays under it.
API_STATEMENT_TIMEOUT_SECONDS = int(os.getenv("DB_STATEMENT_TIMEOUT_SECONDS", "30"))
CSV_STATEMENT_TIMEOUT_SECONDS = int(os.getenv("DB_CSV_STATEMENT_TIMEOUT_SECONDS", "45"))
# Below the statement timeout, so API writes queued behind a worker's bulk write fail fast.
API_LOCK_TIMEOUT_SECONDS = int(os.getenv("DB_LOCK_TIMEOUT_SECONDS", "5"))
# Above the longest legitimate gap between statements inside one request's transaction.
API_IDLE_IN_TRANSACTION_TIMEOUT_SECONDS = int(os.getenv("DB_IDLE_IN_TRANSACTION_TIMEOUT_SECONDS", "60"))
# Not a limit: statements slower than this are logged so the limits above can be tuned from real data.
SLOW_STATEMENT_SECONDS = float(os.getenv("DB_SLOW_STATEMENT_SECONDS", "5"))

_STARTED_AT_ATTR = "_mavedb_statement_started_at"
_SLOW_STATEMENT_LOG_LENGTH = 200


def _set_api_timeouts(dbapi_connection, _connection_record) -> None:
    cursor = dbapi_connection.cursor()
    try:
        cursor.execute(f"SET statement_timeout = '{API_STATEMENT_TIMEOUT_SECONDS}s'")
        cursor.execute(f"SET lock_timeout = '{API_LOCK_TIMEOUT_SECONDS}s'")
        cursor.execute(f"SET idle_in_transaction_session_timeout = '{API_IDLE_IN_TRANSACTION_TIMEOUT_SECONDS}s'")
    finally:
        cursor.close()

    # psycopg2 opens a transaction for the first statement; commit so the SETs are not rolled back.
    dbapi_connection.commit()


def apply_api_timeouts(engine: Engine) -> None:
    """Apply the API's timeouts to every new connection of ``engine``. Safe to call repeatedly.

    Call this before the first connection is opened: connections already in the pool are not touched.
    """
    if not event.contains(engine, "connect", _set_api_timeouts):
        event.listen(engine, "connect", _set_api_timeouts)


def allow_long_statements(db: Session, seconds: int = CSV_STATEMENT_TIMEOUT_SECONDS) -> None:
    """Raise ``statement_timeout`` for the rest of the current transaction.

    Transaction-scoped, so it does not outlive the request. A ``commit()`` or ``rollback()`` ends it, so call
    this immediately before the long read. Does nothing while the API's statement timeout is disabled, so
    disabling it disables it everywhere.
    """
    if not API_STATEMENT_TIMEOUT_SECONDS:
        return

    db.execute(text("SELECT set_config('statement_timeout', :ms, true)"), {"ms": str(seconds * 1000)})


def observe_statements(engine: Engine, threshold_seconds: float) -> None:
    """Count each request's statements on ``engine``, and log any statement slower than ``threshold_seconds``.

    The count lands in the request's canonical log as ``db_statement_count``, so a route whose query count
    grows with its data shows up on real data, where test fixtures are too small to show it.

    A slow statement cancelled by ``statement_timeout`` is logged too, since those are the ones worth tuning.
    Logs the duration and the start of the SQL text, never the bound parameters, which can hold user data.
    """

    def _log(statement: str, started_at: float) -> None:
        elapsed = time.perf_counter() - started_at
        if elapsed < threshold_seconds:
            return

        text_start = " ".join(statement.split())[:_SLOW_STATEMENT_LOG_LENGTH]
        logger.warning(
            msg=f"Slow database statement ({elapsed:.1f}s): {text_start}",
            extra={**logging_context(), "statement_seconds": round(elapsed, 3)},
        )

    @event.listens_for(engine, "before_cursor_execute")
    def _start_timer(_conn, _cursor, _statement, _parameters, context, _executemany):
        increment_logging_context("db_statement_count")
        setattr(context, _STARTED_AT_ATTR, time.perf_counter())

    @event.listens_for(engine, "after_cursor_execute")
    def _log_completed(_conn, _cursor, statement, _parameters, context, _executemany):
        _log(statement, getattr(context, _STARTED_AT_ATTR))

    @event.listens_for(engine, "handle_error")
    def _log_failed(exception_context):
        context = exception_context.execution_context
        started_at = getattr(context, _STARTED_AT_ATTR, None)
        if started_at is not None and exception_context.statement:
            _log(exception_context.statement, started_at)
