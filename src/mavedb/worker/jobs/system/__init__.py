"""System maintenance jobs for worker health and job lifecycle management.

This package contains jobs that maintain the worker system itself, including:
- cleanup_stalled_jobs: Periodic cleanup of zombie/stalled jobs
"""

from mavedb.worker.jobs.system.cleanup import cleanup_stalled_jobs

__all__ = ["cleanup_stalled_jobs"]
