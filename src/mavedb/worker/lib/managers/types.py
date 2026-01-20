from typing import Optional, TypedDict


class ExceptionDetails(TypedDict):
    type: str
    message: str
    traceback: Optional[str]


class JobResultData(TypedDict):
    status: str
    data: dict
    exception_details: Optional[ExceptionDetails]


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
