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

# Limit concurrency to prevent event loop starvation from sync psycopg2 DB
# operations. With the default max_jobs=10, multiple jobs issuing blocking DB
# calls simultaneously can starve the event loop and cause apparent hangs.
# 2 jobs still compete, but the practical impact is much less severe.
#
# TODO#715 Migrate to psycopg3 async driver to safely increase concurrency.
# psycopg3 supports both sync (API) and async (worker) modes on the same
# driver, enabling incremental migration of job functions without touching
# the FastAPI layer. Once all jobs use async sessions, raise MAX_JOBS to 10+.
MAX_JOBS = 2
# ARQ's hard coroutine-kill ceiling. Kept deliberately high (24h) so ARQ is the *last* resort:
# our own DB-driven sweeper (cleanup_stalled_jobs) should detect and recover stalls long before
# this fires, keeping the JobRun state machine in sync with reality. VEP fan-out over large allele
# sets can legitimately run for hours, so a low ceiling would kill healthy work.
# cleanup's RUNNING_TIMEOUT_MINUTES backstop sits ~30 min under this to try and sweep these jobs before
# ARQ's hard timeout fires.
JOB_TIMEOUT_SECONDS = 24 * 60 * 60  # 24 hours


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

    max_jobs = MAX_JOBS
    job_timeout = JOB_TIMEOUT_SECONDS
    # Required for cleanup_stalled_jobs to abort a wedged RUNNING job's coroutine (via Job.abort())
    # before re-driving it. Without this, ARQ ignores abort requests and the sweeper cannot safely
    # confirm a stalled job is dead before retrying it.
    allow_abort_jobs = True
