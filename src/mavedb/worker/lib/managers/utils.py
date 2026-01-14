"""Utility functions for job and pipeline management.

This module provides helper functions for common operations in job and pipeline
management, such as creating standardized result structures, data formatting, and
dependency checking.
"""

import logging
from datetime import datetime
from typing import Optional

from mavedb.models.enums.job_pipeline import DependencyType, JobStatus
from mavedb.worker.lib.managers.constants import TERMINAL_JOB_STATUSES
from mavedb.worker.lib.managers.types import JobResultData

logger = logging.getLogger(__name__)


def construct_bulk_cancellation_result(reason: str) -> JobResultData:
    """Construct a standardized JobResultData structure for bulk job cancellations.

    Args:
        reason: Human-readable reason for the cancellation

    Returns:
        JobResultData: Standardized result data with cancellation metadata
    """
    return {
        "output": {},
        "logs": "",
        "metadata": {
            "reason": reason,
            "timestamp": datetime.now().isoformat(),
        },
    }


def job_dependency_is_met(dependency_type: Optional[DependencyType], dependent_job_status: JobStatus) -> bool:
    """Check if a job dependency is met based on the dependency type and the status of the dependent job.

    Args:
        dependency_type: Type of dependency ('hard' or 'soft')
        dependent_job_status: Status of the dependent job

    Returns:
        bool: True if the dependency is met, False otherwise

    Notes:
        - For 'hard' dependencies, the dependent job must have succeeded.
        - For 'soft' dependencies, the dependent job must be in a terminal state.
        - If no dependency type is specified, the dependency is considered met.
    """
    if not dependency_type:
        logger.debug("No dependency type specified; assuming dependency is met.")
        return True

    if dependency_type == DependencyType.SUCCESS_REQUIRED:
        if dependent_job_status != JobStatus.SUCCEEDED:
            logger.debug(f"Dependency not met: dependent job did not succeed ({dependent_job_status}).")
            return False

    if dependency_type == DependencyType.COMPLETION_REQUIRED:
        if dependent_job_status not in TERMINAL_JOB_STATUSES:
            logger.debug(
                f"Dependency not met: dependent job has not reached a terminal status ({dependent_job_status})."
            )
            return False

    return True
