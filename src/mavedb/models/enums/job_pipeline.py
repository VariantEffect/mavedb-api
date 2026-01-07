"""
Job and pipeline related enums.
"""

from enum import Enum


class JobStatus(str, Enum):
    """Status of a job execution."""

    SUCCEEDED = "succeeded"
    FAILED = "failed"
    PENDING = "pending"
    QUEUED = "queued"
    RUNNING = "running"
    CANCELLED = "cancelled"
    SKIPPED = "skipped"


class PipelineStatus(str, Enum):
    """Status of a pipeline execution."""

    SUCCEEDED = "succeeded"
    FAILED = "failed"
    CREATED = "created"
    RUNNING = "running"
    PAUSED = "paused"
    CANCELLED = "cancelled"
    PARTIAL = "partial"  # Pipeline completed with mixed results (some succeeded, some skipped/cancelled)


class DependencyType(str, Enum):
    """Types of job dependencies."""

    SUCCESS_REQUIRED = "success_required"  # Job only runs if dependency succeeded
    COMPLETION_REQUIRED = "completion_required"  # Job runs if dependency completed (success OR failure)


class FailureCategory(str, Enum):
    """Categories of job failures for better classification and handling."""

    # System-level failures
    SYSTEM_ERROR = "system_error"
    TIMEOUT = "timeout"
    RESOURCE_EXHAUSTION = "resource_exhaustion"
    CONFIGURATION_ERROR = "configuration_error"
    DEPENDENCY_FAILURE = "dependency_failure"

    # Queue and scheduling failures
    ENQUEUE_ERROR = "enqueue_error"
    SCHEDULING_ERROR = "scheduling_error"
    CANCELLED = "cancelled"

    # Data and validation failures
    VALIDATION_ERROR = "validation_error"
    DATA_ERROR = "data_error"

    # External service failures
    NETWORK_ERROR = "network_error"
    API_RATE_LIMITED = "api_rate_limited"
    SERVICE_UNAVAILABLE = "service_unavailable"
    AUTHENTICATION_FAILED = "authentication_failed"

    # Permission and access failures
    PERMISSION_ERROR = "permission_error"
    QUOTA_EXCEEDED = "quota_exceeded"

    # Variant processing specific
    INVALID_HGVS = "invalid_hgvs"
    REFERENCE_MISMATCH = "reference_mismatch"
    VRS_MAPPING_FAILED = "vrs_mapping_failed"
    TRANSCRIPT_NOT_FOUND = "transcript_not_found"

    # Catch-all
    UNKNOWN = "unknown"


class AnnotationStatus(str, Enum):
    """Status of individual variant annotations."""

    SUCCESS = "success"
    FAILED = "failed"
    SKIPPED = "skipped"
