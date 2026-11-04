"""Tell Postgres who a request is acting for, so row-level security policies can read it.

The principal belongs to the request, but Postgres only sees transactions, and after a ``commit()`` the
session's next transaction may run on a different pooled connection. So the principal is stored on the
session once, by :func:`bind_principal`, and an ``after_begin`` listener sends it into every transaction
that session starts.

The values are transaction-local (``set_config(..., true)``). A session-level ``SET`` would stay on the
pooled connection and be inherited by the next request to use it.

Policies read the two settings below. A connection that has carried a transaction-local value reads ``''``
rather than unset afterwards, so a reader must ``nullif(..., '')`` before casting either one.

- ``app.user_id``: the caller's ``users.id`` as text.
- ``app.active_roles``: a ``text[]`` literal of the caller's active role names, such as ``{admin}``, or
  ``{}`` when the caller is acting with no role.

Anonymous is the absence of a principal: an anonymous caller is never sent, so both settings read as unset or
``''``, which ``nullif`` turns into NULL. The worker and scripts never bind either: their database login
decides what they see.
"""

from typing import Optional

from sqlalchemy import Connection, event, text
from sqlalchemy.orm import Session, SessionTransaction

from mavedb.lib.types.authentication import UserData

USER_ID_SETTING = "app.user_id"
ACTIVE_ROLES_SETTING = "app.active_roles"

_PRINCIPAL_INFO_KEY = "mavedb_principal"

_SEND_PRINCIPAL = text(
    f"SELECT set_config('{USER_ID_SETTING}', :user_id, true), set_config('{ACTIVE_ROLES_SETTING}', :roles, true)"
)
# What a reused connection reads once a transaction-local value has ended, so a cleared principal reads the
# same as one never sent.
_CLEARED = {"user_id": "", "roles": ""}


def principal_settings(user_data: UserData) -> dict[str, str]:
    """The values sent for ``user_data``, keyed by the bound parameter names."""
    # Role names are enum values, so they need no quoting inside the array literal.
    role_names = sorted(role.value for role in user_data.active_roles)
    return {"user_id": str(user_data.user.id), "roles": "{" + ",".join(role_names) + "}"}


def bind_principal(session: Session, user_data: Optional[UserData]) -> None:
    """Make ``user_data`` the principal for every transaction ``session`` runs from now on.

    Authentication queries on the request's session before it knows the caller, so a transaction is usually
    already open; the principal is sent into that one immediately.

    Args:
        session: The request's database session.
        user_data: The authenticated caller, or ``None`` for an anonymous one, which sends nothing.
    """
    if user_data is None:
        previous = session.info.pop(_PRINCIPAL_INFO_KEY, None)
        if previous is not None and session.in_transaction():
            session.connection().execute(_SEND_PRINCIPAL, _CLEARED)
        return

    session.info[_PRINCIPAL_INFO_KEY] = principal_settings(user_data)

    # If the transaction has no connection yet, acquiring one fires the listener too, sending the values
    # twice. That costs one statement and keeps this off SQLAlchemy's private transaction state.
    if session.in_transaction():
        session.connection().execute(_SEND_PRINCIPAL, session.info[_PRINCIPAL_INFO_KEY])


@event.listens_for(Session, "after_begin")
def _send_principal(session: Session, transaction: SessionTransaction, connection: Connection) -> None:
    # A savepoint runs inside a transaction that already carries the principal.
    if transaction.nested:
        return

    settings = session.info.get(_PRINCIPAL_INFO_KEY)
    if settings is not None:
        connection.execute(_SEND_PRINCIPAL, settings)
