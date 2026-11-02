# ruff: noqa: E402

import asyncio
import gc
import itertools
import threading
from contextlib import ExitStack

import pytest

pytest.importorskip("fastapi")

from fastapi import HTTPException
from sqlalchemy import select

from mavedb.lib.csv import build_limiter
from mavedb.lib.csv.build_limiter import (
    BuildSlotStreamingResponse,
    csv_build_slot,
    rows_to_build,
    stream_in_build_slot,
)


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
    def test_refuses_a_large_build_when_every_slot_is_held(self, one_slot, session):
        one_slot.acquire()

        with pytest.raises(HTTPException) as exc_info:
            with csv_build_slot(5000, session):
                pass

        assert exc_info.value.status_code == 503
        assert exc_info.value.headers == {"Retry-After": str(build_limiter.RETRY_AFTER_SECONDS)}

    def test_small_builds_skip_the_cap(self, one_slot, session):
        one_slot.acquire()

        with csv_build_slot(999, session):
            pass

    def test_releases_the_slot_even_when_the_build_fails(self, one_slot, session):
        with pytest.raises(RuntimeError):
            with csv_build_slot(5000, session):
                raise RuntimeError("build failed")

        assert one_slot.acquire(blocking=False)

    def test_zero_slots_disables_the_cap(self, monkeypatch, session):
        monkeypatch.setattr(build_limiter, "_slots", None)

        with csv_build_slot(10_000_000, session):
            pass

    def test_csv_build_slot_waiting_returns_the_connection_to_the_pool(self, one_slot, session):
        session.execute(select(1))
        one_slot.acquire()

        with pytest.raises(HTTPException):
            with csv_build_slot(5000, session):
                pass

        assert not session.in_transaction()

    def test_csv_build_slot_free_slot_keeps_the_transaction(self, one_slot, session):
        session.execute(select(1))

        with csv_build_slot(5000, session):
            assert session.in_transaction()

    def test_csv_build_slot_streamed_downloads_holding_their_slots_leave_other_builds_free(
        self, one_slot, monkeypatch, session
    ):
        streamed = threading.BoundedSemaphore(1)
        streamed.acquire()
        monkeypatch.setattr(build_limiter, "_streamed_download_slots", streamed)

        with csv_build_slot(5000, session):
            pass

        with pytest.raises(HTTPException):
            with csv_build_slot(5000, session, streamed_download=True):
                pass


def _held_slot(rows: int, session) -> ExitStack:
    with ExitStack() as setup:
        setup.enter_context(csv_build_slot(rows, session))
        return setup.pop_all()


@pytest.mark.unit
class TestStreamInBuildSlot:
    def test_stream_in_build_slot_holds_the_slot_until_the_stream_is_sent(self, one_slot, session):
        stream = stream_in_build_slot(_held_slot(5000, session), iter(["a", "b"]))

        assert not one_slot.acquire(blocking=False)
        assert list(stream) == ["a", "b"]
        assert one_slot.acquire(blocking=False)

    def test_stream_in_build_slot_client_disconnects_releases_the_slot(self, one_slot, session):
        stream = stream_in_build_slot(_held_slot(5000, session), iter(["a", "b"]))
        next(stream)

        stream.close()

        assert one_slot.acquire(blocking=False)

    def test_stream_in_build_slot_build_fails_releases_the_slot(self, one_slot, session):
        def failing():
            yield "a"
            raise RuntimeError("build failed")

        stream = stream_in_build_slot(_held_slot(5000, session), failing())

        with pytest.raises(RuntimeError):
            list(stream)
        assert one_slot.acquire(blocking=False)

    def test_stream_in_build_slot_never_sent_releases_the_slot(self, one_slot, session):
        stream = stream_in_build_slot(_held_slot(5000, session), iter(["a"]))

        del stream
        gc.collect()

        assert one_slot.acquire(blocking=False)


class TestBuildSlotStreamingResponse:
    def test_build_slot_streaming_response_client_disconnects_releases_the_slot(self, one_slot, session):
        response = BuildSlotStreamingResponse(_held_slot(5000, session), itertools.repeat(b"x"))

        async def send_until_the_client_leaves():
            first_chunk_sent = asyncio.Event()

            async def receive():
                await first_chunk_sent.wait()
                return {"type": "http.disconnect"}

            async def send(message):
                if message["type"] == "http.response.body":
                    first_chunk_sent.set()

            # ASGI 2.3, as uvicorn reports, is the version where Starlette listens for the disconnect.
            await response({"type": "http", "asgi": {"spec_version": "2.3"}}, receive, send)

        # The stream and the cancelled send form a reference cycle, so the garbage collector would release the
        # slot too; this checks the response does it without waiting for that.
        gc.disable()
        try:
            asyncio.run(send_until_the_client_leaves())
            assert one_slot.acquire(blocking=False)
        finally:
            gc.enable()
