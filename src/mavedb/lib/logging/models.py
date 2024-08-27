from enum import Enum


class LogType(str, Enum):
    api_request = "api_request"
    worker_job = "worker_job"


class Source(str, Enum):
    docs = "docs"
    other = "other"
    web = "web"
    worker = "worker"
