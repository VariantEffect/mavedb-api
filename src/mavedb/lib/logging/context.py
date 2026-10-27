"""Per-request logging context, readable from any module without the server dependencies.

The context is a dict held in a ``ContextVar``. ``RequestContextMiddleware`` opens one for each HTTP
request and ``with_job_management`` one for each worker job. Where none is open (scripts) the helpers
here are no-ops, so library code can record context unconditionally.
"""

import asyncio
import functools
import logging
import sys
import traceback
from concurrent.futures import Executor
from contextlib import contextmanager
from contextvars import ContextVar
from types import TracebackType
from typing import Any, Callable, Iterator, Optional, TypeVar

logger = logging.getLogger(__name__)

_logging_context: ContextVar[dict[str, Any]] = ContextVar("mavedb_logging_context")

T = TypeVar("T")


@contextmanager
def logging_context_scope(initial: Optional[dict[str, Any]] = None) -> Iterator[dict[str, Any]]:
    """Open a logging context for the duration of the block, yielding the dict it stores.

    Args:
        initial: Entries to start the context with. The dict is copied, so the caller's is never mutated.

    Yields:
        The live context dict.
    """
    with bound_logging_context(dict(initial or {})) as data:
        yield data


@contextmanager
def bound_logging_context(data: dict[str, Any]) -> Iterator[dict[str, Any]]:
    """Make ``data`` itself the logging context for the duration of the block, without copying it.

    For a caller that already owns the dict its log is built from, such as a worker job's ``JobManager``.
    """
    token = _logging_context.set(data)
    try:
        yield data
    finally:
        _logging_context.reset(token)


async def run_in_executor_with_logging_context(executor: Optional[Executor], fn: Callable[[], T]) -> T:
    """Run ``fn`` in ``executor`` within a copy of the current logging context, then merge back what it changed.

    Neither ``loop.run_in_executor`` nor a process pool carries the caller's context across, so without
    this whatever ``fn`` saves is lost. The copy holds only the entries made of plain data (strings,
    numbers, lists and dicts of them), so an odd value saved for logging can't stop the work from being
    pickled. Entries ``fn`` wrote replace the caller's, which is right as long as the caller doesn't change
    the same entries meanwhile. If ``fn`` raises, what it recorded is lost.

    Args:
        executor: The executor to run ``fn`` in, or None for the event loop's default thread pool.
        fn: A zero-argument callable; bind arguments with ``functools.partial``.

    Returns:
        What ``fn`` returned.
    """
    snapshot = {}
    for key, value in logging_context().items():
        portable = _portable(value)
        if portable is not _NOT_PORTABLE:
            snapshot[key] = portable

    loop = asyncio.get_running_loop()
    result, written = await loop.run_in_executor(
        executor, functools.partial(_call_in_logging_context_scope, snapshot, fn)
    )
    save_to_logging_context(written, overwrite=True)
    return result


def _call_in_logging_context_scope(initial: dict[str, Any], fn: Callable[[], T]) -> tuple[T, dict[str, Any]]:
    # Module level, so a process pool can pickle it.
    data = _WriteTrackingDict(initial)
    with bound_logging_context(data):
        result = fn()
    return result, {key: data[key] for key in data.written}


class _WriteTrackingDict(dict):
    """A dict that remembers which keys were assigned, so an executor hop returns only what it wrote.

    The writers in this module always assign the key, including after mutating a list in place, so every
    write passes through here.
    """

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.written: set[str] = set()

    def __setitem__(self, key: str, value: Any) -> None:
        self.written.add(key)
        super().__setitem__(key, value)


_NOT_PORTABLE = object()


def _portable(value: Any) -> Any:
    """A fresh copy of ``value`` if it's plain data, else ``_NOT_PORTABLE``."""
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, (list, tuple)):
        items = [_portable(item) for item in value]
        return _NOT_PORTABLE if any(item is _NOT_PORTABLE for item in items) else items
    if isinstance(value, dict):
        entries = {key: _portable(item) for key, item in value.items()}
        if any(not isinstance(key, str) or item is _NOT_PORTABLE for key, item in entries.items()):
            return _NOT_PORTABLE
        return entries
    return _NOT_PORTABLE


class LoggingContextCollision(Exception):
    """Raised in strict mode when a write would replace a different value already in the logging context."""


_strict_collisions = False

_COLLISIONS_CAP = 10
_EXCEPTIONS_CAP = 5


def set_strict_collisions(enabled: bool) -> bool:
    """Make collisions raise ``LoggingContextCollision`` instead of being recorded. The test suite turns this on.

    Outside strict mode a collision keeps the newer value and adds the key to ``context_collisions``, so it
    shows up in the canonical log rather than failing a request.

    Returns:
        The previous setting, so a caller can restore it.
    """
    global _strict_collisions
    previous, _strict_collisions = _strict_collisions, enabled
    return previous


def _target(into: Optional[dict[str, Any]]) -> Optional[dict[str, Any]]:
    return into if into is not None else _logging_context.get(None)


def _collide(data: dict[str, Any], key: str, existing: Any, new: Any) -> None:
    if _strict_collisions:
        raise LoggingContextCollision(
            f"Logging context key {key!r} already holds {existing!r}; refusing to replace it with {new!r}. "
            "Use a distinct key, increment or append, or pass overwrite=True if the latest value is what matters."
        )
    seen = data.get("context_collisions", [])
    if key not in seen and len(seen) < _COLLISIONS_CAP:
        seen.append(key)
    data["context_collisions"] = seen


def save_to_logging_context(
    ctx: dict, *, overwrite: bool = False, into: Optional[dict[str, Any]] = None
) -> dict[str, Any]:
    """Record facts in the logging context.

    Each key should hold one fact for the whole request or job. Saving the same value again is a no-op;
    saving a different one is a collision (see ``set_strict_collisions``). For something that happens
    repeatedly use ``increment_logging_context`` or ``append_to_logging_context``.

    Args:
        ctx: The entries to record.
        overwrite: Replace existing values without a collision, for state where the latest value is what
            matters (a job's status, its progress message).
        into: Write into this dict rather than the open context, as ``JobManager`` does with its own.

    Returns:
        The context written to, or an empty dict when none is open.
    """
    data = _target(into)
    if data is None:
        logger.debug("Skipped saving to context. Context does not exist.")
        return {}

    for key, value in ctx.items():
        if not overwrite and key in data and data[key] != value:
            _collide(data, key, data[key], value)
        data[key] = value

    return data


def increment_logging_context(key: str, by: int = 1, *, into: Optional[dict[str, Any]] = None) -> None:
    """Add ``by`` to a counter in the logging context. A no-op when no context is open."""
    data = _target(into)
    if data is None:
        return

    current = data.get(key, 0)
    if isinstance(current, bool) or not isinstance(current, int):
        _collide(data, key, current, by)
        current = 0
    data[key] = current + by


def append_to_logging_context(key: str, value: Any, *, cap: int = 10, into: Optional[dict[str, Any]] = None) -> None:
    """Add ``value`` to a list in the logging context, keeping the first ``cap`` and counting all of them.

    The list is stored under ``key`` and the total under ``{key}_count``, so a loop over many items stays a
    bounded log entry. A no-op when no context is open.
    """
    data = _target(into)
    if data is None:
        return

    items = data.get(key, [])
    if not isinstance(items, list):
        _collide(data, key, items, value)
        items = []
    if len(items) < cap:
        items.append(value)
    data[key] = items
    increment_logging_context(f"{key}_count", into=data)


def logging_context_list_is_full(key: str, *, cap: int = 10) -> bool:
    """Whether ``append_to_logging_context(key, ...)`` would only count, so a caller can skip building the item.

    True when no context is open, since nothing would be recorded at all.
    """
    data = _logging_context.get(None)
    if data is None:
        return True
    items = data.get(key, [])
    return isinstance(items, list) and len(items) >= cap


def record_exception(err: BaseException, *, into: Optional[dict[str, Any]] = None) -> None:
    """Record a caught exception in the logging context.

    ``captured_exception_info`` holds the latest one, the field log queries filter on.
    ``captured_exceptions`` keeps the first few in order, so an earlier error isn't hidden by a later one.
    A no-op when no context is open.
    """
    data = _target(into)
    if data is None:
        return

    info = _exception_info(err, err.__traceback__)
    data["captured_exception_info"] = info
    append_to_logging_context("captured_exceptions", info, cap=_EXCEPTIONS_CAP, into=data)


def logging_context() -> dict:
    data = _logging_context.get(None)
    if data is None:
        logger.debug("Could not access logging context. Context does not exist.")
        return {}

    return data


def correlation_id_for_context() -> Optional[str]:
    return logging_context().get("X-Correlation-ID", None)


def format_raised_exception_info_as_dict(err: BaseException) -> dict:
    _, _, tb = sys.exc_info()
    return {"captured_exception_info": _exception_info(err, tb)}


def _exception_info(err: BaseException, tb: Optional[TracebackType]) -> dict[str, Any]:
    info: dict[str, Any] = {"type": err.__class__.__name__, "string": str(err)}

    # attempt to show only *our* code, not the many layers of library code
    frames = [
        {"file": fs.filename, "line": fs.lineno, "func": fs.name}
        for fs in traceback.extract_tb(tb)
        if "/mavedb/" in fs.filename and "/.direnv/" not in fs.filename
    ]
    if frames:
        info.update(frames[-1])

    return info
