"""Worker lifecycle management hooks.

This module defines the startup, shutdown, and job lifecycle hooks
for the ARQ worker. These hooks manage:
- Process pool for CPU-intensive tasks
- HGVS data provider setup
- Job state initialization and cleanup
"""

from concurrent import futures

from mavedb.data_providers.services import cdot_rest


def standalone_ctx():
    """Create a standalone worker context dictionary."""
    ctx = {}
    ctx["pool"] = futures.ProcessPoolExecutor()
    ctx["hdp"] = cdot_rest()
    ctx["state"] = {}

    # Additional context setup can be added here as needed.
    # This function should not drift from the lifecycle hooks
    # below and is useful for invoking worker jobs outside of ARQ.

    return ctx


async def startup(ctx):
    ctx["pool"] = futures.ProcessPoolExecutor()


async def shutdown(ctx):
    pass


async def on_job_start(ctx):
    ctx["hdp"] = cdot_rest()
    ctx["state"] = {}


async def on_job_end(ctx):
    pass
