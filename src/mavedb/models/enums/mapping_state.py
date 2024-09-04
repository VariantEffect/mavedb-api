import enum


class MappingState(enum.Enum):
    incomplete = "incomplete"
    processing = "processing"
    failed = "failed"
    complete = "complete"
