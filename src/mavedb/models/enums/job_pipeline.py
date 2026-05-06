"""
Job and pipeline related enums.
"""

from enum import Enum


class JobStatus(str, Enum):
    """Status of a job execution."""

    SUCCEEDED = "succeeded"
    FAILED = "failed"
    ERRORED = "errored"
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
    CONFIGURATION_ERROR = "configuration_error"
    DEPENDENCY_FAILURE = "dependency_failure"

    # Data and validation failures
    VALIDATION_ERROR = "validation_error"
    DATA_ERROR = "data_error"

    # External service failures
    NETWORK_ERROR = "network_error"
    SERVICE_UNAVAILABLE = "service_unavailable"

    # Variant processing specific
    VRS_MAPPING_FAILED = "vrs_mapping_failed"

    # Catch-all
    UNKNOWN = "unknown"


class AnnotationStatus(str, Enum):
    """Status of individual variant annotations."""

    SUCCESS = "success"
    FAILED = "failed"
    SKIPPED = "skipped"


class AnnotationFailureCategory(str, Enum):
    """Categories of annotation-level failures on individual variants.

    These describe WHY a specific variant's annotation failed or was skipped,
    as opposed to job-level FailureCategory which describes why an entire job failed.
    """

    MISSING_IDENTIFIER = "missing_identifier"  # Required identifier (e.g. ClinGen allele ID) not present on variant
    UNSUPPORTED_IDENTIFIER = "unsupported_identifier"  # Identifier exists but is in an unsupported format (multi-variant, unrecognized prefix)
    EXTERNAL_API_ERROR = "external_api_error"  # External service call failed (network error, timeout, auth, rate limit)
    EXTERNAL_SERVICE_REJECTED = "external_service_rejected"  # External service was reachable but explicitly rejected our submission (e.g. CAR returned InvalidHGVS)
    EXTERNAL_REFERENCE_NOT_FOUND = (
        "external_reference_not_found"  # Lookup succeeded but external resource doesn't exist
    )
    NO_LINKED_ALLELE = "no_linked_allele"  # No linked allele found in external registry (ClinVar, CA/PA translations)
    UNKNOWN = "unknown"  # Catch-all for uncategorized failures


class JobType(str, Enum):
    """Types of jobs in the pipeline."""

    VARIANT_CREATION = "variant_creation"
    VARIANT_MAPPING = "variant_mapping"
    MAPPED_VARIANT_ANNOTATION = "mapped_variant_annotation"
    PIPELINE_MANAGEMENT = "pipeline_management"
    DATA_MANAGEMENT = "data_management"
    SYSTEM_MAINTENANCE = "system_maintenance"
