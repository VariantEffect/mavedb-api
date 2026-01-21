"""
Decorator utilities for job and pipeline management.

This module exposes decorators for managing job and pipeline lifecycle hooks, error handling,
and logging in worker functions. Use these decorators to ensure consistent state management
and observability for background jobs and pipelines.

Available decorators:
- with_job_management: Handles job context and state transitions
- with_pipeline_management: Handles pipeline context and coordination in addition to job management

Example usage::
    from mavedb.worker.lib.decorators import managed_workflow

    @with_pipeline_management
    async def my_worker_function_in_a_pipeline(...):
        ...

    @with_job_management
    async def my_standalone_job_function(...):
        ...
"""

from .job_guarantee import with_guaranteed_job_run_record
from .job_management import with_job_management
from .pipeline_management import with_pipeline_management

__all__ = ["with_job_management", "with_pipeline_management", "with_guaranteed_job_run_record"]
