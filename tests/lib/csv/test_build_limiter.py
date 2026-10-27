# ruff: noqa: E402

import gc
import threading
from contextlib import ExitStack

import pytest

pytest.importorskip("fastapi")

from fastapi import HTTPException

from mavedb.lib.csv import build_limiter
from mavedb.lib.csv.build_limiter import csv_build_slot, rows_to_build, stream_in_build_slot


@pytest.fixture
def one_slot(monkeypatch):
    """A single build slot, a 1000-row threshold and no waiting, so a held slot refuses immediately."""
    slots = threading.BoundedSemaphore(1)
    monkeypatch.setattr(build_limiter, "_slots", slots)
    monkeypatch.setattr(build_limiter, "CSV_BUILD_MIN_ROWS", 1000)
    monkeypatch.setattr(build_limiter, "CSV_BUILD_WAIT_SECONDS", 0.01)
    return slots


@pytest.mark.unit
@pytest.mark.parametrize(
    "num_variants, start, limit, expected",
    [
        (648022, None, None, 648022),
        (648022, 600000, 50000, 48022),
        (648022, None, 5, 5),
        (10, 20, None, 0),
        (10, None, 0, 0),
    ],
)
def test_rows_to_build(num_variants, start, limit, expected):
    assert rows_to_build(num_variants, start, limit) == expected


@pytest.mark.unit
class TestCsvBuildSlot:
    def test_refuses_a_large_build_when_every_slot_is_held(self, one_slot):
        one_slot.acquire()

        with pytest.raises(HTTPException) as exc_info:
            with csv_build_slot(5000):
                pass

        assert exc_info.value.status_code == 503
        assert exc_info.value.headers == {"Retry-After": str(build_limiter.RETRY_AFTER_SECONDS)}

    def test_small_builds_skip_the_cap(self, one_slot):
        one_slot.acquire()

        with csv_build_slot(999):
            pass

    def test_releases_the_slot_even_when_the_build_fails(self, one_slot):
        with pytest.raises(RuntimeError):
            with csv_build_slot(5000):
                raise RuntimeError("build failed")

        assert one_slot.acquire(blocking=False)

    def test_zero_slots_disables_the_cap(self, monkeypatch):
        monkeypatch.setattr(build_limiter, "_slots", None)

        with csv_build_slot(10_000_000):
            pass


def _held_slot(rows: int) -> ExitStack:
    with ExitStack() as setup:
        setup.enter_context(csv_build_slot(rows))
        return setup.pop_all()


@pytest.mark.unit
class TestStreamInBuildSlot:
    def test_stream_in_build_slot_holds_the_slot_until_the_stream_is_sent(self, one_slot):
        stream = stream_in_build_slot(_held_slot(5000), iter(["a", "b"]))

        assert not one_slot.acquire(blocking=False)
        assert list(stream) == ["a", "b"]
        assert one_slot.acquire(blocking=False)

    def test_stream_in_build_slot_client_disconnects_releases_the_slot(self, one_slot):
        stream = stream_in_build_slot(_held_slot(5000), iter(["a", "b"]))
        next(stream)

        stream.close()

        assert one_slot.acquire(blocking=False)

    def test_stream_in_build_slot_build_fails_releases_the_slot(self, one_slot):
        def failing():
            yield "a"
            raise RuntimeError("build failed")

        stream = stream_in_build_slot(_held_slot(5000), failing())

        with pytest.raises(RuntimeError):
            list(stream)
        assert one_slot.acquire(blocking=False)

    def test_stream_in_build_slot_never_sent_releases_the_slot(self, one_slot):
        stream = stream_in_build_slot(_held_slot(5000), iter(["a"]))

        del stream
        gc.collect()

        assert one_slot.acquire(blocking=False)
