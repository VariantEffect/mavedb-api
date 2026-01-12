"""
Manager Exceptions for explicit error handling.
"""


class ManagerError(Exception):
    """Base exception for Manager operations."""

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
