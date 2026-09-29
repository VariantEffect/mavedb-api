"""Load shedding for the score set CSV exports.

Building a large CSV is CPU-bound Python, and Python runs one thread at a time per process. A few large builds
running at once in one API process starve everything else that process serves, so each process runs only a few at a
time and refuses the rest with a 503 the client can retry.

TODO(#845, #771): A stopgap. Remove it once full exports are cheap or served from cache, and ``csv_build_refused``
no longer appears in the request logs.
"""

import logging
import os
import threading
from collections.abc import Iterator
from contextlib import contextmanager
from typing import Optional

from fastapi import HTTPException

from mavedb.constants import MAVEDB_BULK_DOWNLOAD_URL
from mavedb.lib.logging.context import logging_context, save_to_logging_context

logger = logging.getLogger(__name__)

# Large builds one API process runs at once; 0 disables the cap.
CSV_BUILD_SLOTS = int(os.getenv("CSV_BUILD_SLOTS", "2"))
# Builds of fewer rows skip the cap, so previews and small score sets are never refused.
CSV_BUILD_MIN_ROWS = int(os.getenv("CSV_BUILD_MIN_ROWS", "10000"))
# How long a request waits for a slot. Leaves most of the load balancer's 60s idle timeout for the build itself.
CSV_BUILD_WAIT_SECONDS = 10
RETRY_AFTER_SECONDS = 30

BUSY_DETAIL = (
    "The server is busy building other large downloads. Retry after the number of seconds in the Retry-After header."
    f" To download many datasets, consider using the bulk archive instead: {MAVEDB_BULK_DOWNLOAD_URL}"
)

_slots = threading.BoundedSemaphore(CSV_BUILD_SLOTS) if CSV_BUILD_SLOTS else None


def rows_to_build(num_variants: Optional[int], start: Optional[int], limit: Optional[int]) -> int:
    """The number of rows a request for ``start``/``limit`` of a score set with ``num_variants`` variants returns."""
    # TODO(#372): `num_variants` is NOT NULL but typed optional.
    remaining = max((num_variants or 0) - (start or 0), 0)
    return min(remaining, limit) if limit else remaining


@contextmanager
def csv_build_slot(rows: int) -> Iterator[None]:
    """Hold one of this process's CSV build slots while building ``rows`` rows.

    Waits up to ``CSV_BUILD_WAIT_SECONDS`` for a slot, then raises a 503 with ``Retry-After``. Builds smaller than
    ``CSV_BUILD_MIN_ROWS`` run without a slot.
    """
    if _slots is None or rows < CSV_BUILD_MIN_ROWS:
        yield
        return

    if not _slots.acquire(timeout=CSV_BUILD_WAIT_SECONDS):
        save_to_logging_context({"csv_build_refused": True, "csv_build_rows": rows})
        logger.info(msg="Refused a large CSV build; every build slot is in use.", extra=logging_context())
        raise HTTPException(status_code=503, detail=BUSY_DETAIL, headers={"Retry-After": str(RETRY_AFTER_SECONDS)})

    try:
        yield
    finally:
        _slots.release()
