import os
from typing import Callable

from arq.connections import RedisSettings
from arq import cron

from mavedb.worker.jobs import dummy_task

# ARQ requires at least one task on startup.
BACKGROUND_FUNCTIONS: list[Callable] = [dummy_task]
BACKGROUND_CRONJOBS: list[Callable] = []

REDIS_IP = os.getenv("REDIS_IP") or "localhost"
REDIS_PORT = int(os.getenv("REDIS_PORT") or 6379)


RedisWorkerSettings = RedisSettings(host=REDIS_IP, port=REDIS_PORT)


# TODO: If we need to define custom startup and shutdown behavior
#       on our worker, we can do so here.
async def startup(ctx):
    pass


async def shutdown(ctx):
    pass


class ArqWorkerSettings:
    """
    Settings for the ARQ worker.
    """

    on_startup = startup
    on_shutdown = shutdown
    redis_settings = RedisWorkerSettings
    functions: list = BACKGROUND_FUNCTIONS
    cron_jobs: list = BACKGROUND_CRONJOBS

    job_timeout = 5 * 60 * 60  # Keep jobs alive for a long while...
