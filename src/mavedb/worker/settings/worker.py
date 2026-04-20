"""Main ARQ worker configuration class.

This module defines the primary ArqWorkerSettings class that brings together
all worker configuration including:
- Job functions and cron jobs from the jobs registry
- Redis connection settings
- Lifecycle hooks for startup/shutdown and job execution
- Timeout and logging configuration

This is the main configuration class used to start the ARQ worker.
"""

from mavedb.lib.logging.canonical import log_job
from mavedb.worker.jobs import BACKGROUND_CRONJOBS, BACKGROUND_FUNCTIONS
from mavedb.worker.settings.lifecycle import on_job_end, on_job_start, shutdown, startup
from mavedb.worker.settings.redis import RedisWorkerSettings


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

    # Limit concurrency to prevent event loop starvation from sync psycopg2 DB
    # operations. With the default max_jobs=10, multiple jobs issuing blocking DB
    # calls simultaneously can starve the event loop and cause apparent hangs.
    # 2 jobs still compete, but the practical impact is much less severe. If we wanted
    # to eventually increase concurrency, we could look into using a connection pool
    # with async support (e.g. asyncpg) to mitigate the issue.
    max_jobs = 2
    job_timeout = 5 * 60 * 60  # Keep jobs alive for a long while...
