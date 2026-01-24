"""
Pipeline management job entrypoints.

This module exposes job functions for pipeline management, such as starting a pipeline.
Import job functions here and add them to __all__ for job discovery and import convenience.
"""

from .start_pipeline import start_pipeline

__all__ = [
    "start_pipeline",
]
