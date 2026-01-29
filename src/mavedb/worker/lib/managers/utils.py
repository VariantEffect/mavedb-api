"""Utility functions for job and pipeline management.

This module provides helper functions for common operations in job and pipeline
management, such as creating standardized result structures, data formatting, and
dependency checking.
"""

import logging
from datetime import datetime
from typing import Literal, Optional, Union

from mavedb.models.enums.job_pipeline import DependencyType, JobStatus
from mavedb.worker.lib.managers.constants import COMPLETED_JOB_STATUSES
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
        "status": "cancelled",
        "data": {
            "reason": reason,
            "timestamp": datetime.now().isoformat(),
        },
        "exception": None,
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
        if dependent_job_status not in COMPLETED_JOB_STATUSES:
            logger.debug(
                f"Dependency not met: dependent job has not reached a completed status ({dependent_job_status})."
            )
            return False

    return True


def job_should_be_skipped_due_to_unfulfillable_dependency(
    dependency_type: Optional[DependencyType], dependent_job_status: JobStatus
) -> Union[tuple[Literal[False], None], tuple[Literal[True], str]]:
    """Determine if a job should be skipped due to an unfulfillable dependency.

    Args:
        dependency_type: Type of dependency ('hard' or 'soft')
        dependent_job_status: Status of the dependent job

    Returns:
        Union[tuple[Literal[False], None], tuple[Literal[True], str]]: Tuple indicating
            if the job should be skipped and the reason

    Notes:
        - A job should be skipped if it has a 'hard' dependency and the dependent job did not succeed.
    """

    # If dependency must have SUCCEEDED but is in a terminal non-success state, skip.
    if dependency_type == DependencyType.SUCCESS_REQUIRED:
        if dependent_job_status in (JobStatus.FAILED, JobStatus.SKIPPED, JobStatus.CANCELLED):
            logger.debug(
                f"Job should be skipped due to unfulfillable 'success_required' dependency "
                f"({dependent_job_status})."
            )
            return True, f"Dependency did not succeed ({dependent_job_status})"

    # If dependency requires 'completion' and you want CANCELLED to NOT qualify, skip here too.
    if dependency_type == DependencyType.COMPLETION_REQUIRED:
        if dependent_job_status in (JobStatus.CANCELLED, JobStatus.SKIPPED):
            logger.debug(
                f"Job should be skipped due to unfulfillable 'completion_required' dependency "
                f"({dependent_job_status})."
            )
            return True, f"Dependency was not completed successfully ({dependent_job_status})"

    return False, None
