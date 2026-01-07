"""Worker job utility functions and constants.

This module provides shared utilities used across worker jobs:
- Job state management and context setup
- Retry logic with exponential backoff
- Configuration constants for queues and timeouts

These utilities help ensure consistent behavior and error handling
across all worker job implementations.
"""

from .constants import (
    ENQUEUE_BACKOFF_ATTEMPT_LIMIT,
    LINKING_BACKOFF_IN_SECONDS,
    MAPPING_BACKOFF_IN_SECONDS,
    MAPPING_CURRENT_ID_NAME,
    MAPPING_QUEUE_NAME,
)
from .job_state import setup_job_state
from .retry import enqueue_job_with_backoff

__all__ = [
    "setup_job_state",
    "enqueue_job_with_backoff",
    "MAPPING_QUEUE_NAME",
    "MAPPING_CURRENT_ID_NAME",
    "MAPPING_BACKOFF_IN_SECONDS",
    "LINKING_BACKOFF_IN_SECONDS",
    "ENQUEUE_BACKOFF_ATTEMPT_LIMIT",
]
