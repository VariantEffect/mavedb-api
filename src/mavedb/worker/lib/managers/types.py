from typing import Literal, Optional, TypedDict


class JobResultData(TypedDict):
    status: Literal["ok", "failed", "skipped", "exception", "cancelled"]
    data: dict
    exception: Optional[Exception]


class RetryHistoryEntry(TypedDict):
    attempt: int
    timestamp: str
    result: JobResultData
    reason: str


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
