"""Constants for job management and pipeline coordination.

This module defines commonly used job status groupings that are used throughout
the job management system for state validation, dependency checking, and
pipeline coordination.
"""

from mavedb.models.enums.job_pipeline import FailureCategory, JobStatus

# Job status constants for common groupings
STARTABLE_JOB_STATUSES = [JobStatus.QUEUED, JobStatus.PENDING]
"""Job statuses that can be transitioned to RUNNING state."""

COMPLETED_JOB_STATUSES = [JobStatus.SUCCEEDED, JobStatus.FAILED]
"""Job statuses indicating finished execution (completed states)."""

TERMINAL_JOB_STATUSES = [JobStatus.SUCCEEDED, JobStatus.FAILED, JobStatus.CANCELLED, JobStatus.SKIPPED]
"""Job statuses indicating finished execution (terminal states)."""

CANCELLED_JOB_STATUSES = [JobStatus.CANCELLED, JobStatus.SKIPPED, JobStatus.FAILED]
"""Job statuses that should stop execution (termination conditions)."""

RETRYABLE_JOB_STATUSES = [JobStatus.FAILED, JobStatus.CANCELLED, JobStatus.SKIPPED]
"""Job statuses that can be retried."""

ACTIVE_JOB_STATUSES = [JobStatus.PENDING, JobStatus.QUEUED, JobStatus.RUNNING]
"""Job statuses that can be cancelled/skipped when pipeline fails."""

RETRYABLE_FAILURE_CATEGORIES = (
    FailureCategory.NETWORK_ERROR,
    FailureCategory.TIMEOUT,
    FailureCategory.SERVICE_UNAVAILABLE,
    # TODO: Add more retryable exception types as needed
)
"""Failure categories that are considered retryable errors."""
