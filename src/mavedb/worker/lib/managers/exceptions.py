"""
Manager Exceptions for explicit error handling.
"""


class ManagerError(Exception):
    """Base exception for Manager operations."""

    pass


## Pipeline Manager Exceptions


class PipelineManagerError(ManagerError):
    """Pipeline Manager specific errors."""

    pass


class PipelineCoordinationError(PipelineManagerError):
    """Pipeline coordination failed - may be recoverable."""

    pass


class PipelineTransitionError(PipelineManagerError):
    """Pipeline is in wrong state for requested operation."""

    pass


class PipelineStateError(PipelineManagerError):
    """Critical pipeline state operations failed - database issues preventing state persistence."""

    pass


## Job Manager Exceptions


class JobManagerError(ManagerError):
    """Job Manager specific errors."""

    pass


class JobStateError(JobManagerError):
    """Critical job state operations failed - database issues preventing state persistence."""

    pass


class JobTransitionError(JobManagerError):
    """Job is in wrong state for requested operation."""

    pass


class DatabaseConnectionError(JobStateError):
    """Database connection issues preventing any operations."""

    pass
