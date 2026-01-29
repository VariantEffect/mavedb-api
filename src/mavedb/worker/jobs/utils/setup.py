"""Job state management utilities.

This module provides utilities for managing job state and context across
the worker job lifecycle. It handles setup of logging context, correlation
IDs, and other state information needed for job traceability and monitoring.
"""

import logging

from mavedb.models.job_run import JobRun

logger = logging.getLogger(__name__)


def validate_job_params(required_params: list[str], job: JobRun) -> None:
    """
    Validate that the given job has all required parameters present in its job_params.
    """
    if not job.job_params:
        raise ValueError("Job has no job_params defined.")

    for param in required_params:
        if param not in job.job_params:
            raise ValueError(f"Missing required job param: {param}")
