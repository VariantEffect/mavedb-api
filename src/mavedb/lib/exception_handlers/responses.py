"""The shape of an error response, and the body builders the domain tables share."""

from dataclasses import dataclass
from typing import Any, Callable, Mapping, Optional, Union

from mavedb.lib.logging.context import correlation_id_for_context


@dataclass(frozen=True, kw_only=True)
class ErrorResponse:
    """How an exception type is answered. ``status`` and ``body`` may be functions of the exception."""

    status: Union[int, Callable[[Any], int]]
    body: Callable[[Any], dict[str, Any]]
    alert: bool
    """Whether the exception is logged as an error and sent to Slack."""
    headers: Optional[Mapping[str, str]] = None
    """Headers sent with the response, such as ``Retry-After``."""

    def __post_init__(self) -> None:
        if isinstance(self.status, int) and not 400 <= self.status <= 599:
            raise ValueError(f"An error response needs a 4xx or 5xx status, not {self.status}.")

    def status_for(self, exc: Exception) -> int:
        return self.status(exc) if callable(self.status) else self.status


ErrorResponses = dict[type[Exception], ErrorResponse]
"""A domain's table: how the api answers each exception type it maps."""


def message(exc: Exception) -> dict[str, Any]:
    """A body carrying the exception's own message."""
    return {"message": str(exc)}


def fixed(key: str, text: str) -> Callable[[Any], dict[str, Any]]:
    """A body with the same text for every instance, under ``key``."""
    return lambda _exc: {key: text}


def attributable(text: str) -> Callable[[Any], dict[str, Any]]:
    """A body carrying the correlation id that finds this request's logs."""
    return lambda _exc: {"detail": text, "correlation_id": correlation_id_for_context()}


def merge_error_responses(*tables: ErrorResponses) -> ErrorResponses:
    """Combine the domain tables into one, refusing an exception type that two of them map."""
    merged: ErrorResponses = {}
    for table in tables:
        duplicated = merged.keys() & table.keys()
        if duplicated:
            raise ValueError(f"Mapped by more than one table: {sorted(t.__name__ for t in duplicated)}")
        merged.update(table)
    return merged
