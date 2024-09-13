import enum


class MappingState(enum.Enum):
    incomplete = "incomplete"
    processing = "processing"
    failed = "failed"
    complete = "complete"
    pending_variant_processing = "pending_variant_processing"
    not_attempted = "not_attempted"
    queued = "queued"
