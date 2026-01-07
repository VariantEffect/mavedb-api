"""Job state management utilities.

This module provides utilities for managing job state and context across
the worker job lifecycle. It handles setup of logging context, correlation
IDs, and other state information needed for job traceability and monitoring.
"""

import logging
from typing import Any, Optional

logger = logging.getLogger(__name__)


def setup_job_state(
    ctx, invoker: Optional[int], resource: Optional[str], correlation_id: Optional[str]
) -> dict[str, Any]:
    """
    Initialize and store job state information in the context dictionary for traceability.

    Args:
        ctx: The job context dictionary, must contain 'state' and 'job_id' keys.
        invoker: The user ID or identifier who initiated the job (may be None).
        resource: The resource string associated with the job (may be None).
        correlation_id: Optional correlation ID for tracing requests across services.

    Returns:
        dict[str, Any]: The job state dictionary for the current job_id.
    """
    ctx["state"][ctx["job_id"]] = {
        "application": "mavedb-worker",
        "user": invoker,
        "resource": resource,
        "correlation_id": correlation_id,
    }
    return ctx["state"][ctx["job_id"]]
