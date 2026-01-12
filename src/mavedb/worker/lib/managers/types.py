from typing import TypedDict


class JobResultData(TypedDict):
    output: dict
    logs: str
    metadata: dict


class RetryHistoryEntry(TypedDict):
    attempt: int
    timestamp: str
    result: JobResultData
    reason: str
