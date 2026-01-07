"""Retry and backoff utilities for job error handling.

This module provides utilities for implementing exponential backoff and
retry logic for failed jobs. It helps ensure reliable job execution
by automatically retrying transient failures with appropriate delays.
"""

import logging
from datetime import timedelta
from typing import Any, Optional

from arq import ArqRedis

from mavedb.worker.jobs.utils.constants import ENQUEUE_BACKOFF_ATTEMPT_LIMIT

logger = logging.getLogger(__name__)


async def enqueue_job_with_backoff(
    redis: ArqRedis, job_name: str, attempt: int, backoff: int, *args
) -> tuple[Optional[str], bool, Any]:
    """
    Enqueue a job with exponential backoff and attempt tracking, for robust retry logic.

    Args:
        redis (ArqRedis): The Redis connection for job queueing.
        job_name (str): The name of the job to enqueue.
        attempt (int): The current attempt number (used for backoff calculation).
        backoff (int): The base backoff time in seconds.
        *args: Additional arguments to pass to the job.

    Returns:
        tuple[Optional[str], bool, Any]:
            - The new job ID if enqueued, else None.
            - Boolean indicating if the backoff limit was NOT reached (True if retry scheduled).
            - The updated backoff value (seconds).

    Notes:
        - If the attempt exceeds ENQUEUE_BACKOFF_ATTEMPT_LIMIT, no job is enqueued and limit is considered reached.
        - The attempt value is incremented and passed as the last argument to the job.
        - The job is deferred by the calculated backoff time.
    """
    new_job_id = None
    limit_reached = attempt > ENQUEUE_BACKOFF_ATTEMPT_LIMIT
    if not limit_reached:
        limit_reached = True
        backoff = backoff * (2**attempt)
        attempt = attempt + 1

        # NOTE: for jobs supporting backoff, `attempt` should be the final argument.
        new_job = await redis.enqueue_job(
            job_name,
            *args,
            attempt,
            _defer_by=timedelta(seconds=backoff),
        )

        if new_job:
            new_job_id = new_job.job_id

    return (new_job_id, not limit_reached, backoff)
