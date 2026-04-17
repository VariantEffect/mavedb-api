# ruff : noqa: E402

"""
Unit tests for ensure_session_ctx, verifying task-local session isolation.

ARQ runs multiple jobs concurrently as asyncio Tasks sharing
the same ctx dict. Without task-local sessions, one job closing its session can
invalidate sessions used by other jobs, causing them to silently error and
preventing pipeline coordination.
"""

import asyncio
from contextlib import contextmanager
from unittest.mock import MagicMock, patch

import pytest

pytest.importorskip("arq")

from mavedb.worker.lib.decorators.utils import _task_db_session, ensure_session_ctx

pytestmark = pytest.mark.usefixtures("patch_db_session_ctxmgr")


def _mock_session_factory(*sessions):
    """Return a context-manager factory that yields sessions in order."""
    it = iter(sessions)

    @contextmanager
    def factory():
        yield next(it)

    return factory


@pytest.mark.asyncio
@pytest.mark.unit
class TestEnsureSessionCtxConcurrency:
    """Concurrent asyncio Tasks must each get their own DB session."""

    async def test_concurrent_tasks_get_isolated_sessions(self):
        """Two Tasks sharing the same ctx dict should each create their own session,
        not reuse the other's via ctx['db']."""
        shared_ctx: dict = {}
        results: dict = {}

        task_a_entered = asyncio.Event()
        task_b_entered = asyncio.Event()
        task_a_can_exit = asyncio.Event()

        session_a = MagicMock(name="session_a")
        session_b = MagicMock(name="session_b")

        with patch(
            "mavedb.worker.lib.decorators.utils.db_session",
            _mock_session_factory(session_a, session_b),
        ):

            async def task_a():
                with ensure_session_ctx(shared_ctx) as session:
                    results["a"] = session
                    task_a_entered.set()
                    await task_a_can_exit.wait()

            async def task_b():
                await task_a_entered.wait()
                with ensure_session_ctx(shared_ctx) as session:
                    results["b"] = session
                    task_b_entered.set()

            t_a = asyncio.create_task(task_a())
            t_b = asyncio.create_task(task_b())

            await task_b_entered.wait()
            task_a_can_exit.set()
            await asyncio.gather(t_a, t_b)

        assert results["a"] is session_a
        assert results["b"] is session_b
        assert results["a"] is not results["b"]

    async def test_session_survives_other_task_cleanup(self):
        """After Task A exits and cleans up its session, Task B's session
        should remain valid and accessible."""
        shared_ctx: dict = {}
        results: dict = {}

        task_a_exited = asyncio.Event()
        task_b_can_check = asyncio.Event()

        session_a = MagicMock(name="session_a")
        session_b = MagicMock(name="session_b")

        with patch(
            "mavedb.worker.lib.decorators.utils.db_session",
            _mock_session_factory(session_a, session_b),
        ):

            async def task_a():
                with ensure_session_ctx(shared_ctx):
                    pass
                task_a_exited.set()

            async def task_b():
                await task_a_exited.wait()
                with ensure_session_ctx(shared_ctx) as session:
                    results["b"] = session
                    task_b_can_check.set()

            t_a = asyncio.create_task(task_a())
            t_b = asyncio.create_task(task_b())
            await task_b_can_check.wait()
            await asyncio.gather(t_a, t_b)

        assert results["b"] is session_b


@pytest.mark.asyncio
@pytest.mark.unit
class TestEnsureSessionCtxNesting:
    """Nested calls within the same Task should reuse the outer session."""

    async def test_nested_call_reuses_outer_session(self):
        """The inner ensure_session_ctx should return the same session
        as the outer one, without creating a new session."""
        ctx: dict = {}
        outer_session = MagicMock(name="outer_session")
        call_count = 0

        @contextmanager
        def counting_factory():
            nonlocal call_count
            call_count += 1
            yield outer_session

        with patch("mavedb.worker.lib.decorators.utils.db_session", counting_factory):
            with ensure_session_ctx(ctx) as s1:
                with ensure_session_ctx(ctx) as s2:
                    assert s1 is s2 is outer_session

        assert call_count == 1

    async def test_context_var_cleaned_up_after_exit(self):
        """After the outermost ensure_session_ctx exits, the context var
        should be None so a subsequent call creates a fresh session."""
        ctx: dict = {}
        session_1 = MagicMock(name="session_1")
        session_2 = MagicMock(name="session_2")

        with patch(
            "mavedb.worker.lib.decorators.utils.db_session",
            _mock_session_factory(session_1, session_2),
        ):
            with ensure_session_ctx(ctx) as s1:
                assert s1 is session_1
            assert _task_db_session.get() is None

            with ensure_session_ctx(ctx) as s2:
                assert s2 is session_2
            assert _task_db_session.get() is None

    async def test_context_var_cleaned_up_on_exception(self):
        """If an exception occurs inside the context manager, the context
        var should still be cleaned up."""
        ctx: dict = {}
        session = MagicMock(name="session")

        @contextmanager
        def raising_db_session():
            yield session

        with patch("mavedb.worker.lib.decorators.utils.db_session", raising_db_session):
            with pytest.raises(RuntimeError):
                with ensure_session_ctx(ctx):
                    raise RuntimeError("boom")

        assert _task_db_session.get() is None
