"""Sign test requests in as a user with real credentials, so the real ``get_current_user`` runs on every request.

A request carries either a JWT signed with a key generated for the test run, or an access key stored for the user.
``get_current_user`` verifies either one as it would in production and binds the principal (#823), so tests see
the same principal Postgres would. Nothing overrides the authentication dependencies.

Production trusts only ORCID's signing key. The ``trusted_signing_key`` fixture swaps in this run's public key for
the length of each test; that swap is the one place a test reaches inside ``mavedb``, and becomes a settings value
once the test harness reads settings at call time.

Tests get a client per persona from the ``client_for`` fixture or a named persona fixture such as
``outsider_client``. :func:`credential_headers` is for a test that builds its own client, such as an ``AsyncClient``.
"""

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from enum import Enum
from functools import cache
from typing import Any, Literal, Mapping, Optional, Sequence, Union

from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from fastapi.testclient import TestClient
from jose import jwt
from sqlalchemy.orm import Session

from mavedb.lib.authentication import ACCESS_TOKEN_NAME
from mavedb.models.enums.user_role import UserRole
from mavedb.models.user import User
from tests.helpers.constants import ANONYMOUS, Anonymous
from tests.helpers.util.access_key import create_api_key_for_user

TEST_JWT_AUDIENCE = "mavedb-test"
"""The ``aud`` claim test tokens carry, and the audience the app expects while it trusts the test key."""

Persona = Union[Mapping[str, Any], User, Literal[Anonymous.ANONYMOUS]]
"""Who a request signs in as: a constant such as ``TEST_USER``, a saved ``User``, or ``ANONYMOUS``."""


class AllHeldRoles(Enum):
    ALL_HELD_ROLES = "all_held_roles"


ALL_HELD_ROLES = AllHeldRoles.ALL_HELD_ROLES
"""Act with every role the user holds: a JWT sent without ``X-Active-Roles``."""


@dataclass(frozen=True, kw_only=True)
class Jwt:
    """Sign in with a JWT signed by the run's key.

    ``active_roles`` is sent as ``X-Active-Roles``: ``[]`` acts with no role, and ``ALL_HELD_ROLES`` sends no
    header. A plain string sends a name that isn't a ``UserRole``, which the app ignores.
    """

    active_roles: Union[Sequence[Union[UserRole, str]], Literal[AllHeldRoles.ALL_HELD_ROLES]] = ALL_HELD_ROLES


@dataclass(frozen=True, kw_only=True)
class ApiKey:
    """Sign in with an access key created for the user, carrying ``role`` or, with ``None``, no role.

    The app ignores ``X-Active-Roles`` for an access key, so the key's role is the only one the request acts with.
    """

    role: Optional[UserRole]


Credential = Union[Jwt, ApiKey]
"""How a request signs in."""

_AUTH_HEADERS = ("Authorization", "X-Active-Roles", ACCESS_TOKEN_NAME)
"""Every header that carries a credential or acting roles."""


@dataclass(frozen=True)
class SigningKey:
    """An RSA key pair standing in for ORCID's."""

    private_pem: str
    public_pem: str

    def token_for(self, username: str) -> str:
        """A JWT for ``username``, valid for an hour, signed with the private half."""
        now = datetime.now(timezone.utc)
        claims = {"sub": username, "aud": TEST_JWT_AUDIENCE, "iat": now, "exp": now + timedelta(hours=1)}
        return jwt.encode(claims, self.private_pem, algorithm="RS256")


def generate_signing_key() -> SigningKey:
    """A new key pair. Tests use one other than :func:`run_signing_key` to forge a token the app must reject."""
    private_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    private_pem = private_key.private_bytes(
        serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8, serialization.NoEncryption()
    ).decode()
    public_pem = (
        private_key.public_key()
        .public_bytes(serialization.Encoding.PEM, serialization.PublicFormat.SubjectPublicKeyInfo)
        .decode()
    )
    return SigningKey(private_pem=private_pem, public_pem=public_pem)


@cache
def run_signing_key() -> SigningKey:
    """The signing key the app trusts in tests. Generated once per process because RSA key generation is slow."""
    return generate_signing_key()


def _username(user: Union[Mapping[str, Any], User]) -> str:
    return user.username if isinstance(user, User) else user["username"]


def _persona_username(user: Persona) -> Optional[str]:
    return None if user is ANONYMOUS else _username(user)


def _role_name(role: Union[str, UserRole]) -> str:
    return role.name if isinstance(role, UserRole) else role


def credential_headers(db: Session, user: Persona, *, via: Credential = Jwt()) -> dict[str, str]:
    """The headers that sign a request in as ``user``.

    Use it for a client that isn't an :class:`AuthenticatingClient`, such as an ``AsyncClient``.

    Args:
        db: Where the access key is stored when ``via`` is an :class:`ApiKey`.
        user: Who to sign in as. ``ANONYMOUS`` sends no credentials.
        via: A :class:`Jwt` or an :class:`ApiKey`.

    Returns:
        Headers to send with the request.
    """
    # Tests aren't type-checked, so a None persona would otherwise fail later with an unhelpful TypeError.
    if user is None:
        raise TypeError("Pass ANONYMOUS, not None, to sign in as nobody.")
    if user is ANONYMOUS:
        return {}

    username = _username(user)

    if isinstance(via, ApiKey):
        return {ACCESS_TOKEN_NAME: create_api_key_for_user(db, username, role=via.role)}

    headers = {"Authorization": f"Bearer {run_signing_key().token_for(username)}"}
    if via.active_roles is not ALL_HELD_ROLES:
        headers["X-Active-Roles"] = ",".join(_role_name(role) for role in via.active_roles)
    return headers


class AuthenticatingClient(TestClient):
    """A test client that signs every request in as one persona.

    Tests get one from the ``client_for`` fixture, or a named persona fixture such as ``outsider_client``, so the
    actor is named where the client is. Don't override ``get_current_user`` or ``require_current_user`` instead:
    that skips the code that binds the principal, so the request would run as nobody once RLS reads it.

    ```
    client_for(ADMIN_USER)                                     # JWT; acts with every role the user holds
    client_for(ADMIN_USER, via=Jwt(active_roles=[]))           # JWT acting with no role
    client_for(MAPPER, via=ApiKey(role=UserRole.mapper))       # a new access key carrying a role
    client_for(TEST_USER, via=ApiKey(role=None))               # a new access key with no role
    client_for(ANONYMOUS)                                      # no credentials
    ```

    Gotchas:
        - The user must exist. ``client_for`` adds a constant persona such as ``OUTSIDER_USER``; a client built
          directly doesn't, and a JWT for an unknown username makes ``get_current_user`` create the user and call
          ORCID for their email.
        - The first JWT request in a test signs the user in for real: it writes ``last_login``, clears
          ``is_first_login`` and commits the shared session, which expires every loaded ORM object. Read the ids
          you need before the request, and use an :class:`ApiKey` when a test asserts on login state.
        - An :class:`ApiKey` client stores a new access key for the user when it's built.
    """

    def __init__(self, *args: Any, db: Session, user: Persona, via: Credential = Jwt(), **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.username: Optional[str] = _persona_username(user)
        """Who requests are signed in as, or ``None`` when they're anonymous."""
        self.headers.update(credential_headers(db, user, via=via))

    @property
    def auth_headers(self) -> dict[str, str]:
        """The credential headers requests carry, for a test that calls the ASGI app directly."""
        return {name: self.headers[name] for name in _AUTH_HEADERS if name in self.headers}
