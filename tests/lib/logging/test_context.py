"""Tests for the per-request logging context, which must work with only the core dependencies installed."""

import asyncio
import functools
import subprocess
import sys
import threading
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor

import pytest

from mavedb.lib.logging.context import (
    LoggingContextCollision,
    append_to_logging_context,
    correlation_id_for_context,
    increment_logging_context,
    logging_context,
    logging_context_scope,
    record_exception,
    run_in_executor_with_logging_context,
    save_to_logging_context,
    set_strict_collisions,
)
from mavedb.lib.permissions.models import PermissionResponse


@pytest.fixture(autouse=True)
def strict_collisions():
    """These tests pin strict-mode behaviour themselves rather than relying on the suite-wide setting."""
    previous = set_strict_collisions(True)
    yield
    set_strict_collisions(previous)


@pytest.fixture
def production_collisions(strict_collisions):
    """Collisions as production handles them, recorded rather than raised."""
    set_strict_collisions(False)
    yield


def raise_and_record(message: str) -> None:
    try:
        raise ValueError(message)
    except ValueError as err:
        record_exception(err)


def record_in_child(value: str) -> str:
    """Module level, so a process pool can pickle it."""
    save_to_logging_context({"child_fact": value})
    increment_logging_context("counter", 2)
    append_to_logging_context("items", value)
    return value.upper()


def test_helpers_are_no_ops_outside_a_scope():
    assert save_to_logging_context({"key": "value"}) == {}
    increment_logging_context("counter")

    assert logging_context() == {}
    assert correlation_id_for_context() is None


def test_scope_collects_saved_and_counted_entries():
    with logging_context_scope({"X-Correlation-ID": "abc"}) as ctx:
        save_to_logging_context({"key": "value"})
        increment_logging_context("counter")
        increment_logging_context("counter", by=2)

        assert logging_context() is ctx
        assert ctx == {"X-Correlation-ID": "abc", "key": "value", "counter": 3}
        assert correlation_id_for_context() == "abc"

    assert logging_context() == {}


def test_scope_copies_its_initial_entries():
    initial = {"key": "value"}

    with logging_context_scope(initial):
        save_to_logging_context({"other": 1})

    assert initial == {"key": "value"}


def test_nested_scope_restores_the_outer_context():
    with logging_context_scope({"level": "outer"}):
        with logging_context_scope({"level": "inner"}):
            assert logging_context()["level"] == "inner"

        assert logging_context()["level"] == "outer"


def test_save_same_value_twice_is_not_a_collision():
    with logging_context_scope() as ctx:
        save_to_logging_context({"key": "value"})
        save_to_logging_context({"key": "value"})

        assert ctx == {"key": "value"}


def test_save_different_value_raises_in_strict_mode():
    with logging_context_scope({"key": "first"}) as ctx:
        with pytest.raises(LoggingContextCollision, match="'key'"):
            save_to_logging_context({"key": "second"})

        assert ctx["key"] == "first"


def test_save_with_overwrite_replaces_the_value():
    with logging_context_scope({"status": "running"}) as ctx:
        save_to_logging_context({"status": "succeeded"}, overwrite=True)

        assert ctx["status"] == "succeeded"


def test_save_different_value_in_production_keeps_newer_and_records_collision(production_collisions):
    with logging_context_scope({"key": "first"}) as ctx:
        save_to_logging_context({"key": "second"})
        save_to_logging_context({"key": "third"})

        assert ctx["key"] == "third"
        assert ctx["context_collisions"] == ["key"]


def test_increment_of_a_non_counter_is_a_collision():
    with logging_context_scope({"key": "text"}):
        with pytest.raises(LoggingContextCollision):
            increment_logging_context("key")


def test_append_keeps_first_items_up_to_cap_and_counts_all():
    with logging_context_scope() as ctx:
        for i in range(5):
            append_to_logging_context("items", i, cap=3)

        assert ctx["items"] == [0, 1, 2]
        assert ctx["items_count"] == 5


def test_operations_write_into_an_explicit_dict_without_an_open_context():
    target: dict = {}

    save_to_logging_context({"key": "value"}, into=target)
    increment_logging_context("counter", into=target)
    append_to_logging_context("items", "a", into=target)

    assert target == {"key": "value", "counter": 1, "items": ["a"], "items_count": 1}
    assert logging_context() == {}


def test_record_exception_keeps_latest_info_and_earlier_exceptions():
    with logging_context_scope() as ctx:
        for i in range(7):
            raise_and_record(f"error {i}")

        assert ctx["captured_exception_info"]["string"] == "error 6"
        assert [e["string"] for e in ctx["captured_exceptions"]] == [f"error {i}" for i in range(5)]
        assert ctx["captured_exceptions_count"] == 7


def test_record_exception_outside_an_except_block_keeps_its_traceback():
    # Frames are filtered to MaveDB package code, so raise from there rather than from this file, whose
    # path depends on where the checkout lives.
    try:
        bool(PermissionResponse(permitted=True))
    except TypeError as err:
        caught = err

    with logging_context_scope() as ctx:
        record_exception(caught)

        assert ctx["captured_exception_info"]["func"] == "__bool__"
        assert ctx["captured_exception_info"]["file"].endswith("mavedb/lib/permissions/models.py")


@pytest.mark.parametrize(
    "make_executor",
    [functools.partial(ThreadPoolExecutor, 1), functools.partial(ProcessPoolExecutor, 1)],
    ids=["thread", "process"],
)
def test_executor_hop_merges_back_what_the_child_changed(make_executor):
    async def run():
        with logging_context_scope({"parent_fact": "p", "counter": 1}) as ctx:
            with make_executor() as executor:
                result = await run_in_executor_with_logging_context(executor, functools.partial(record_in_child, "c"))
            return result, ctx

    result, ctx = asyncio.run(run())

    assert result == "C"
    assert ctx == {
        "parent_fact": "p",
        "counter": 3,
        "child_fact": "c",
        "items": ["c"],
        "items_count": 1,
    }


def test_executor_hop_leaves_values_that_are_not_plain_data_behind():
    lock = threading.Lock()

    async def run():
        with logging_context_scope({"lock": lock, "counter": 1}) as ctx:
            with ProcessPoolExecutor(1) as executor:
                await run_in_executor_with_logging_context(executor, functools.partial(record_in_child, "c"))
            return ctx

    ctx = asyncio.run(run())

    assert ctx["lock"] is lock
    assert ctx["counter"] == 3
    assert ctx["child_fact"] == "c"


def test_executor_hop_propagates_the_child_exception():
    def fail():
        raise RuntimeError("child failed")

    async def run():
        with logging_context_scope():
            await run_in_executor_with_logging_context(None, fail)

    with pytest.raises(RuntimeError, match="child failed"):
        asyncio.run(run())


def test_importable_without_server_dependencies():
    """Library code records logging context, so importing it must not pull in the web stack (#459)."""
    blocked = ("fastapi", "starlette", "starlette_context")
    script = "\n".join(
        [
            "import sys",
            *(f"sys.modules[{name!r}] = None" for name in blocked),
            "import mavedb.lib.logging.context",
            "import mavedb.lib.permissions",
        ]
    )

    result = subprocess.run([sys.executable, "-c", script], capture_output=True, text=True)

    assert result.returncode == 0, result.stderr
