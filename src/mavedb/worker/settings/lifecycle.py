"""Worker lifecycle management hooks.

This module defines the startup, shutdown, and job lifecycle hooks
for the ARQ worker. These hooks manage:
- Process pool for CPU-intensive tasks
- HGVS data provider setup
- Job state initialization and cleanup
"""

from concurrent import futures

from mavedb.data_providers.services import cdot_rest


async def startup(ctx):
    ctx["pool"] = futures.ProcessPoolExecutor()


async def shutdown(ctx):
    pass


async def on_job_start(ctx):
    ctx["hdp"] = cdot_rest()
    ctx["state"] = {}


async def on_job_end(ctx):
    pass
