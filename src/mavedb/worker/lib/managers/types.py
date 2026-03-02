from __future__ import annotations

from dataclasses import dataclass
from typing import Any, TypedDict

from mavedb.models.enums.job_pipeline import JobStatus


@dataclass
class JobExecutionOutcome:
    """Result of a job execution, returned by job functions to the management layer.

    Use factory methods to construct instances rather than direct construction:
    - ``JobExecutionOutcome.succeeded()`` — job completed successfully
    - ``JobExecutionOutcome.failed()`` — controlled business logic failure
    - ``JobExecutionOutcome.errored()`` — unhandled exception / system crash
    - ``JobExecutionOutcome.skipped()`` — job intentionally not executed
    """

    status: JobStatus
    data: dict[str, Any]
    error: str | None
    exception: Exception | None

    @classmethod
    def succeeded(cls, data: dict[str, Any] | None = None) -> JobExecutionOutcome:
        """Job completed successfully."""
        return cls(status=JobStatus.SUCCEEDED, data=data or {}, error=None, exception=None)

    @classmethod
    def failed(cls, reason: str, data: dict[str, Any] | None = None) -> JobExecutionOutcome:
        """Controlled failure — job determined the outcome was unsuccessful."""
        return cls(status=JobStatus.FAILED, data=data or {}, error=reason, exception=None)

    @classmethod
    def errored(cls, exception: Exception, data: dict[str, Any] | None = None) -> JobExecutionOutcome:
        """Unhandled exception — job crashed."""
        return cls(status=JobStatus.ERRORED, data=data or {}, error=str(exception), exception=exception)

    @classmethod
    def skipped(cls, data: dict[str, Any] | None = None) -> JobExecutionOutcome:
        """Job intentionally not executed."""
        return cls(status=JobStatus.SKIPPED, data=data or {}, error=None, exception=None)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-serializable dictionary representation.

        Excludes the ``exception`` field since Exception objects are not
        JSON-serializable. Use this for logging, ARQ result storage, and
        any context where a plain dict is needed.
        """
        return {
            "status": self.status.value,
            "data": self.data,
            "error": self.error,
        }


class RetryHistoryEntry(TypedDict):
    attempt: int
    timestamp: str
    status: str  # JobStatus.value from the failed attempt
    error_message: str  # Brief summary of the error
    reason: str  # Why the retry was triggered


class PipelineProgress(TypedDict):
    total_jobs: int
    completed_jobs: int
    successful_jobs: int
    failed_jobs: int
    running_jobs: int
    pending_jobs: int
    completion_percentage: float
    duration: int  # seconds
    status_counts: dict
