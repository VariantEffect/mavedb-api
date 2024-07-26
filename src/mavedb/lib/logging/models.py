from enum import Enum
from typing import Any, TypedDict


class LogType(str, Enum):
    api_request = "api_request"
    worker_job = "worker_job"


class Source(str, Enum):
    docs = "docs"
    other = "other"
    web = "web"
    worker = "worker"


class LogRecord(TypedDict):
    log_type: LogType
    source: Source
    time_ns: int
    duration_ns: int


class APIRecord(LogRecord):
    path: str
    method: str
    response_code: int
    user_agent: str
    auth_method: str


class WorkerRecord(LogRecord):
    id: str
    queued_ns: int
    job: str
    attempt: int
    success: bool
    result: Any
