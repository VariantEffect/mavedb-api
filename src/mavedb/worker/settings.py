import os
from concurrent import futures
from typing import Callable

from arq.connections import RedisSettings
from arq.cron import CronJob, cron

from mavedb.data_providers.services import cdot_rest
from mavedb.db.session import SessionLocal
from mavedb.lib.logging.canonical import log_job
from mavedb.worker.jobs import (
    create_variants_for_score_set,
    map_variants_for_score_set,
    variant_mapper_manager,
    refresh_materialized_views,
    refresh_published_variants_view,
)

# ARQ requires at least one task on startup.
BACKGROUND_FUNCTIONS: list[Callable] = [
    create_variants_for_score_set,
    variant_mapper_manager,
    map_variants_for_score_set,
    refresh_published_variants_view,
]
# In UTC time. Depending on daylight savings time, this will bounce around by an hour but should always be very early in the morning
# for all of the USA.
BACKGROUND_CRONJOBS: list[CronJob] = [
    cron(refresh_materialized_views, name="refresh_all_materialized_views", hour=20, minute=0)
]

REDIS_IP = os.getenv("REDIS_IP") or "localhost"
REDIS_PORT = int(os.getenv("REDIS_PORT") or 6379)
REDIS_SSL = (os.getenv("REDIS_SSL") or "false").lower() == "true"


RedisWorkerSettings = RedisSettings(host=REDIS_IP, port=REDIS_PORT, ssl=REDIS_SSL)


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


class ArqWorkerSettings:
    """
    Settings for the ARQ worker.
    """

    on_startup = startup
    on_shutdown = shutdown
    on_job_start = on_job_start
    on_job_end = on_job_end
    after_job_end = log_job
    redis_settings = RedisWorkerSettings
    functions: list = BACKGROUND_FUNCTIONS
    cron_jobs: list = BACKGROUND_CRONJOBS

    job_timeout = 5 * 60 * 60  # Keep jobs alive for a long while...
