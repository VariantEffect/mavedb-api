from __future__ import annotations

from typing import TypedDict


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
