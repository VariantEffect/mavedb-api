import os
from contextlib import contextmanager
from contextvars import ContextVar

from mavedb.db.session import db_session

# Task-local DB session storage. Each asyncio Task (i.e., each concurrent ARQ job)
# gets its own copy of this variable, preventing concurrent jobs from sharing or
# closing each other's sessions via the shared ARQ `ctx` dict.
_task_db_session: ContextVar = ContextVar("_task_db_session", default=None)


def is_test_mode() -> bool:
    """Check if the application is running in test mode based on the MAVEDB_TEST_MODE environment variable.

    Returns:
        bool: True if in test mode, False otherwise.
    """
    # Although not ideal, we use an environment variable to detect whether
    # the application is in test mode. In the context of decorators, test
    # mode makes them no-ops to facilitate unit testing without side effects.
    #
    # This is necessary because decorators are applied at import time, making
    # it difficult to mock their behavior in tests when they must be imported
    # up front and provided to the ARQ worker.
    #
    # This pattern allows us to control decorator behavior in tests without
    # altering production code paths.
    return os.getenv("MAVEDB_TEST_MODE") == "1"


@contextmanager
def ensure_session_ctx(ctx):
    existing = _task_db_session.get()
    if existing is not None:
        # Session already exists for this task (from an outer decorator).
        # Refresh ctx["db"] so downstream code in _execute_managed_* reads
        # this task's session, not a stale value left by another task.
        ctx["db"] = existing
        yield existing
    else:
        with db_session() as session:
            _task_db_session.set(session)
            ctx["db"] = session
            try:
                yield session
            finally:
                _task_db_session.set(None)


def ensure_ctx(args) -> dict:
    # Extract context (first argument by ARQ convention)
    if not args or len(args) < 1 or not isinstance(args[0], dict):
        raise ValueError("Managed functions must receive context as first argument")

    ctx = args[0]
    return ctx


def ensure_job_id(args) -> int:
    # Extract job_id (second argument by MaveDB convention)
    if not args or len(args) < 2 or not isinstance(args[1], int):
        raise ValueError("Job ID not found in function arguments")

    job_id = args[1]
    return job_id
