"""Configuration constants for worker settings.

This module centralizes all environment variable handling for the worker,
providing sensible defaults and type conversion for configuration values.
All worker-related environment variables should be defined here, along with
the job concurrency limit that both the worker classes and the lifecycle hooks
size themselves from.
"""

import os

REDIS_IP = os.getenv("REDIS_IP") or "localhost"
REDIS_PORT = int(os.getenv("REDIS_PORT") or 6379)
REDIS_SSL = (os.getenv("REDIS_SSL") or "false").lower() == "true"

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
