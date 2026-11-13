"""Worker lifecycle management hooks.

This module defines the startup, shutdown, and job lifecycle hooks
for the ARQ worker. These hooks manage:
- Process pool for CPU-intensive tasks
- HGVS data provider setup
- Job state initialization and cleanup
"""

from concurrent import futures

from mavedb.data_providers.services import cdot_rest, seqrepo_data_proxy
from mavedb.db.session import engine
from mavedb.db.timeouts import apply_worker_timeouts
from mavedb.worker.settings.constants import MAX_JOBS


def process_pool() -> futures.ProcessPoolExecutor:
    """Create the process pool that jobs hand their blocking work to.

    The pool has one process per job slot, since no more than MAX_JOBS jobs can use it at
    once. Each process exits after one task, so memory a task builds up (such as a parsed
    mapping response) is returned instead of held until the worker restarts. Setting
    max_tasks_per_child also makes Python start processes fresh with ``spawn`` rather than
    by copying the worker.

    Returns:
        A ProcessPoolExecutor sized and recycled for the worker.
    """
    return futures.ProcessPoolExecutor(max_workers=MAX_JOBS, max_tasks_per_child=1)


def standalone_ctx():
    """Create a standalone worker context dictionary."""
    ctx = {}
    ctx["pool"] = process_pool()
    ctx["redis"] = None  # Redis connection can be set up here if needed.
    ctx["hdp"] = cdot_rest()
    ctx["seqrepo"] = seqrepo_data_proxy()
    ctx["state"] = {}

    # Additional context setup can be added here as needed.
    # This function should not drift from the lifecycle hooks
    # below and is useful for invoking worker jobs outside of ARQ.

    return ctx


async def startup(ctx):
    apply_worker_timeouts(engine)
    ctx["pool"] = process_pool()


async def shutdown(ctx):
    pass


async def on_job_start(ctx):
    ctx["hdp"] = cdot_rest()
    ctx["seqrepo"] = seqrepo_data_proxy()
    ctx["state"] = {}


async def on_job_end(ctx):
    pass
