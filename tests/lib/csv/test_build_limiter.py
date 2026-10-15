# ruff: noqa: E402

import threading

import pytest

pytest.importorskip("fastapi")

from fastapi import HTTPException

from mavedb.lib.csv import build_limiter
from mavedb.lib.csv.build_limiter import csv_build_slot, rows_to_build


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
    [(648022, None, None, 648022), (648022, 600000, 50000, 48022), (648022, None, 5, 5), (10, 20, None, 0)],
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
