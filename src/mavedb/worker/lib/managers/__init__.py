"""Manager classes and shared utilities for job and pipeline coordination.

This package provides managers for job lifecycle and pipeline coordination,
along with shared constants, exceptions, and types used across the worker system.

Main Classes:
    JobManager: Individual job lifecycle management
    PipelineManager: Pipeline coordination and dependency management

Shared Utilities:
    Constants: Job statuses, timeouts, retry limits
    Exceptions: Standardized error hierarchy
    Types: TypedDict definitions and common type hints

Example Usage:
    >>> from mavedb.worker.lib.managers import JobManager, PipelineManager
    >>> from mavedb.worker.lib.managers import JobStateError, TERMINAL_JOB_STATUSES
    >>>
    >>> job_manager = JobManager(db, redis, job_id)
    >>> pipeline_manager = PipelineManager(db, redis)
    >>>
    >>> # Individual job operations
    >>> job_manager.start_job()
    >>> job_manager.succeed_job({"output": "success"})
    >>>
    >>> # Pipeline coordination
    >>> await pipeline_manager.coordinate_after_completion(True)
"""

# Main manager classes
# Commonly used constants
# Main manager classes
from .base_manager import BaseManager
from .constants import (
    ACTIVE_JOB_STATUSES,
    TERMINAL_JOB_STATUSES,
)

# Exception hierarchy
from .exceptions import (
    DatabaseConnectionError,
    JobStateError,
    JobTransitionError,
)
from .job_manager import JobManager
from .pipeline_manager import PipelineManager

# Type definitions
from .types import JobResultData, RetryHistoryEntry

__all__ = [
    # Main classes
    "BaseManager",
    "JobManager",
    "PipelineManager",
    # Constants
    "ACTIVE_JOB_STATUSES",
    "TERMINAL_JOB_STATUSES",
    # Exceptions
    "DatabaseConnectionError",
    "JobStateError",
    "JobTransitionError",
    "PipelineCoordinationError",
    # Types
    "JobResultData",
    "RetryHistoryEntry",
]
