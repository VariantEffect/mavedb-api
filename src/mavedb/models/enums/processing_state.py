import enum


class ProcessingState(enum.Enum):
    incomplete = "incomplete"
    processing = "processing"
    failed = "failed"
    success = "success"
