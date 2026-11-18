"""Typed exceptions for the database errors the api and worker answer differently from any other.

``translate_database_errors`` raises these in place of the SQLAlchemy error they refine. Each subclasses the
SQLAlchemy type it replaces, so code catching that type still catches it.
"""

import re
from typing import Optional

from sqlalchemy import event
from sqlalchemy.engine import Engine, ExceptionContext
from sqlalchemy.exc import DBAPIError, OperationalError, ProgrammingError


class RowSecurityViolation(ProgrammingError):
    """A row-level security policy rejected a write that Python allowed."""


class MissingGrant(ProgrammingError):
    """The connection's role has no grant on a table, sequence or other object, which is a deployment bug."""


class DatabaseBusy(OperationalError):
    """A statement ran past ``statement_timeout`` or waited past ``lock_timeout``."""


# Postgres raises both a row-level security rejection and a missing grant under this one SQLSTATE.
_INSUFFICIENT_PRIVILEGE = "42501"
"""The SQLSTATE for a statement the connection's role isn't allowed to run."""

# Both come from the limits `db/timeouts.py` sets on every connection, so they signal load, not a broken query.
_BUSY_SQLSTATES = frozenset({"57014", "55P03"})
"""The SQLSTATEs for a statement past ``statement_timeout`` (query_canceled) or ``lock_timeout`` (lock_not_available)."""

# Only the message tells the two 42501 forms apart. This one also covers a named policy and the
# `(USING expression)` variants that `ON CONFLICT DO UPDATE` raises.
_ROW_SECURITY_MESSAGE = re.compile(
    r'^new row violates row-level security policy(?: "[^"]*")?(?: \(USING expression\))? for table '
)
"""Matches the message for a write a row-level security policy rejected."""

# Any object kind, since a role split can as easily miss USAGE on an id sequence as a grant on its table.
_MISSING_GRANT_MESSAGE = re.compile(r"^permission denied for ")
"""Matches the message for a statement on an object the connection's role has no grant on."""


def _translated_type(err: DBAPIError) -> Optional[type[DBAPIError]]:
    pgcode = getattr(err.orig, "pgcode", None)
    if pgcode in _BUSY_SQLSTATES:
        return DatabaseBusy
    if pgcode != _INSUFFICIENT_PRIVILEGE:
        return None

    message = getattr(getattr(err.orig, "diag", None), "message_primary", None) or ""
    if _ROW_SECURITY_MESSAGE.match(message):
        return RowSecurityViolation
    if _MISSING_GRANT_MESSAGE.match(message):
        return MissingGrant
    return None


def _translate(context: ExceptionContext) -> Optional[DBAPIError]:
    err = context.sqlalchemy_exception
    if context.is_pre_ping or not isinstance(err, DBAPIError) or err.orig is None:
        return None
    orig = err.orig

    translated = _translated_type(err)
    # Only ever narrow the type, so an `except` for the original still matches.
    if translated is None or not issubclass(translated, type(err)):
        return None
    return translated(
        err.statement,
        err.params,
        orig,
        hide_parameters=err.hide_parameters,
        connection_invalidated=err.connection_invalidated,
    )


def translate_database_errors(engine: Engine) -> None:
    """Raise the typed exceptions above in place of the SQLAlchemy errors they refine, on ``engine``."""
    if not event.contains(engine, "handle_error", _translate):
        event.listen(engine, "handle_error", _translate)
