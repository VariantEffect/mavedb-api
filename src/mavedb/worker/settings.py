import os
from typing import Callable

from arq.connections import RedisSettings
from arq import cron

from mavedb.worker.jobs import create_variants_for_score_set
from mavedb.db.session import SessionLocal
from mavedb.data_providers.services import cdot_rest

# ARQ requires at least one task on startup.
BACKGROUND_FUNCTIONS: list[Callable] = [create_variants_for_score_set]
BACKGROUND_CRONJOBS: list[Callable] = []

REDIS_IP = os.getenv("REDIS_IP") or "localhost"
REDIS_PORT = int(os.getenv("REDIS_PORT") or 6379)
REDIS_SSL = (os.getenv("REDIS_SSL") or "false").lower() == "true"


RedisWorkerSettings = RedisSettings(host=REDIS_IP, port=REDIS_PORT, ssl=REDIS_SSL)


async def startup(ctx):
    pass


async def shutdown(ctx):
    pass


async def on_job_start(ctx):
    db = SessionLocal()
    db.current_user_id = None
    ctx["db"] = db
    ctx["hdp"] = cdot_rest()


async def on_job_end(ctx):
    db = ctx["db"]
    db.close()


class ArqWorkerSettings:
    """
    Settings for the ARQ worker.
    """

    on_startup = startup
    on_shutdown = shutdown
    on_job_start = on_job_start
    on_job_end = on_job_end
    redis_settings = RedisWorkerSettings
    functions: list = BACKGROUND_FUNCTIONS
    cron_jobs: list = BACKGROUND_CRONJOBS

    job_timeout = 5 * 60 * 60  # Keep jobs alive for a long while...
