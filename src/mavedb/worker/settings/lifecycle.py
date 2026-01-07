"""Worker lifecycle management hooks.

This module defines the startup, shutdown, and job lifecycle hooks
for the ARQ worker. These hooks manage:
- Process pool for CPU-intensive tasks
- Database session management per job
- HGVS data provider setup
- Job state initialization and cleanup
"""

from concurrent import futures

from mavedb.data_providers.services import cdot_rest
from mavedb.db.session import SessionLocal


async def startup(ctx):
    ctx["pool"] = futures.ProcessPoolExecutor()


async def shutdown(ctx):
    pass


async def on_job_start(ctx):
    db = SessionLocal()
    db.current_user_id = None
    ctx["db"] = db
    ctx["hdp"] = cdot_rest()
    ctx["state"] = {}


async def on_job_end(ctx):
    db = ctx["db"]
    db.close()
