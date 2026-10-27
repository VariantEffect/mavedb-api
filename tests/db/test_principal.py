# ruff: noqa: E402

from types import SimpleNamespace
from typing import Optional
from unittest import mock

import pytest

pytest.importorskip("psycopg2")
fastapi = pytest.importorskip("fastapi")

from fastapi import Depends, FastAPI
from fastapi.testclient import TestClient
from sqlalchemy import create_engine, select, text
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import NullPool

from mavedb import deps
from mavedb.db.principal import bind_principal, principal_settings
from mavedb.lib.authentication import get_current_user
from mavedb.lib.types.authentication import UserData
from mavedb.models.enums.user_role import UserRole
from mavedb.models.user import User
from tests.helpers.constants import ADMIN_USER, TEST_USER
from tests.helpers.util.access_key import create_api_key_for_user

# What #809's app_user_id() and app_roles() will read. A connection that has carried a transaction-local value
# reads '' afterwards, which these casts must turn into NULL rather than an error.
_READ_PRINCIPAL = text(
    "SELECT current_setting('app.user_id', true) AS user_id,"
    " current_setting('app.active_roles', true) AS roles,"
    " nullif(current_setting('app.user_id', true), '')::integer AS user_id_cast,"
    " nullif(current_setting('app.active_roles', true), '')::text[] AS roles_cast,"
    " pg_backend_pid() AS backend_pid"
)


def _read_principal(session: Session) -> dict:
    return dict(session.execute(_READ_PRINCIPAL).mappings().one())


def _user_data(user_id: int, roles: list[UserRole]) -> UserData:
    # principal_settings reads only the id, so an unsaved stand-in for the user is enough.
    return UserData(SimpleNamespace(id=user_id), roles)  # type: ignore[arg-type]


def _user_id(session: Session, username: str) -> int:
    return session.scalars(select(User.id).where(User.username == username)).one()


@pytest.fixture
def pooled_engine(session):
    """An engine with one pooled connection, so consecutive sessions provably share it."""
    engine = create_engine(session.get_bind().url, pool_size=1, max_overflow=0)
    yield engine
    engine.dispose()


@pytest.mark.unit
class TestPrincipalSettings:
    @pytest.mark.parametrize(
        "user_data, expected",
        [
            (_user_data(7, []), {"user_id": "7", "roles": "{}"}),
            (_user_data(7, [UserRole.admin]), {"user_id": "7", "roles": "{admin}"}),
            (_user_data(7, [UserRole.mapper, UserRole.admin]), {"user_id": "7", "roles": "{admin,mapper}"}),
        ],
        ids=["no-active-roles", "admin", "admin-and-mapper"],
    )
    def test_principal_settings_for_caller_formats_user_id_and_role_array(self, user_data, expected):
        assert principal_settings(user_data) == expected


@pytest.mark.unit
class TestBindPrincipal:
    def test_bind_principal_before_any_transaction_is_sent_when_one_begins(self, session):
        bind_principal(session, _user_data(7, [UserRole.admin]))

        principal = _read_principal(session)
        assert (principal["user_id"], principal["roles"]) == ("7", "{admin}")

    def test_bind_principal_in_open_transaction_is_sent_immediately(self, session):
        session.execute(text("SELECT 1"))

        bind_principal(session, _user_data(7, []))

        assert _read_principal(session)["user_id"] == "7"

    def test_bind_principal_after_commit_is_resent_on_a_new_connection(self, session):
        # NullPool opens a new connection for each transaction, as a busy pool may after a commit.
        engine = create_engine(session.get_bind().url, poolclass=NullPool)
        with Session(engine) as request_session:
            bind_principal(request_session, _user_data(7, [UserRole.mapper]))
            before = _read_principal(request_session)
            request_session.commit()
            after = _read_principal(request_session)

        assert after["backend_pid"] != before["backend_pid"]
        assert (after["user_id"], after["roles"]) == ("7", "{mapper}")

    def test_bind_principal_in_savepoint_carries_the_enclosing_transactions_principal(self, session):
        bind_principal(session, _user_data(7, [UserRole.admin]))

        with session.begin_nested():
            principal = _read_principal(session)

        assert (principal["user_id"], principal["roles"]) == ("7", "{admin}")

    def test_bind_principal_does_not_leak_to_the_next_session_on_a_pooled_connection(self, pooled_engine):
        with Session(pooled_engine) as first:
            bind_principal(first, _user_data(7, [UserRole.admin]))
            first_principal = _read_principal(first)
            first.commit()

        with Session(pooled_engine) as second:
            second_principal = _read_principal(second)

        assert second_principal["backend_pid"] == first_principal["backend_pid"]
        assert (second_principal["user_id"], second_principal["roles"]) == ("", "")
        assert second_principal["user_id_cast"] is None

    @pytest.mark.parametrize("stale", [False, True], ids=["fresh-connection", "connection-used-by-a-bound-session"])
    def test_unbound_session_reads_as_nobody_without_error(self, pooled_engine, stale):
        if stale:
            with Session(pooled_engine) as bound:
                bind_principal(bound, _user_data(7, [UserRole.admin]))
                _read_principal(bound)
                bound.commit()

        with Session(pooled_engine) as unbound:
            principal = _read_principal(unbound)

        assert principal["user_id_cast"] is None
        assert principal["roles_cast"] is None

    def test_bind_principal_anonymous_reads_as_nobody_without_error(self, session):
        bind_principal(session, None)

        principal = _read_principal(session)
        assert principal["user_id_cast"] is None
        assert principal["roles_cast"] is None

    @pytest.mark.parametrize("commit_between", [False, True], ids=["same-transaction", "next-transaction"])
    def test_bind_principal_anonymous_after_a_caller_clears_the_principal(self, session, commit_between):
        bind_principal(session, _user_data(7, [UserRole.admin]))
        _read_principal(session)

        bind_principal(session, None)
        if commit_between:
            session.commit()

        principal = _read_principal(session)
        assert principal["user_id_cast"] is None
        assert principal["roles_cast"] is None


@pytest.mark.unit
class TestRequestPrincipal:
    """The principal as a request sees it, authenticated through the real ``get_current_user``."""

    @pytest.fixture
    def client(self, pooled_engine, setup_lib_db):
        app = FastAPI()
        request_sessions = sessionmaker(autocommit=False, autoflush=False, bind=pooled_engine)

        def override_get_db():
            db = request_sessions()
            try:
                yield db
            finally:
                db.close()

        # Commits mid-request, as the routers do, and reports the principal in the transactions either side.
        @app.get("/probe")
        def probe(_user: Optional[UserData] = Depends(get_current_user), db: Session = Depends(deps.get_db)):
            before = _read_principal(db)
            db.commit()
            after = _read_principal(db)
            return {"before": before, "after": after}

        @app.get("/unauthenticated-probe")
        def unauthenticated_probe(db: Session = Depends(deps.get_db)):
            return _read_principal(db)

        app.dependency_overrides[deps.get_db] = override_get_db
        with TestClient(app) as client:
            yield client

    @pytest.fixture
    def jwt_for(self):
        def _jwt_for(username: str):
            return mock.patch("mavedb.lib.authentication.decode_jwt", lambda _: {"sub": username})

        return _jwt_for

    @pytest.mark.parametrize("auth_method", ["jwt", "api_key"], ids=["jwt", "api-key"])
    def test_request_principal_for_authenticated_caller_survives_mid_request_commit(
        self, client, session, jwt_for, auth_method
    ):
        user_id = _user_id(session, TEST_USER["username"])
        if auth_method == "jwt":
            with jwt_for(TEST_USER["username"]):
                response = client.get("/probe", headers={"Authorization": "Bearer test"})
        else:
            api_key = create_api_key_for_user(session, TEST_USER["username"])
            response = client.get("/probe", headers={"X-API-key": api_key})

        assert response.status_code == 200
        for transaction in ("before", "after"):
            principal = response.json()[transaction]
            assert (principal["user_id"], principal["roles"]) == (str(user_id), "{}")

    @pytest.mark.parametrize(
        "active_roles_header, expected_roles",
        [(None, "{admin}"), ("admin", "{admin}"), ("ordinary user", "{}")],
        ids=["all-held-roles-by-default", "requested-role", "roles-dropped"],
    )
    def test_request_principal_for_elevated_caller_carries_only_active_roles(
        self, client, session, jwt_for, active_roles_header, expected_roles
    ):
        headers = {"Authorization": "Bearer test"}
        if active_roles_header is not None:
            headers["X-Active-Roles"] = active_roles_header

        with jwt_for(ADMIN_USER["username"]):
            response = client.get("/probe", headers=headers)

        assert response.status_code == 200
        user_id = _user_id(session, ADMIN_USER["username"])
        for transaction in ("before", "after"):
            principal = response.json()[transaction]
            assert (principal["user_id"], principal["roles"]) == (str(user_id), expected_roles)

    def test_request_principal_for_unheld_role_is_refused_before_the_route_runs(self, client, jwt_for):
        with jwt_for(TEST_USER["username"]):
            response = client.get("/probe", headers={"Authorization": "Bearer test", "X-Active-Roles": "admin"})

        assert response.status_code == 403
        assert response.json() == {"detail": "This user is not a member of the requested acting role."}

    def test_request_principal_for_anonymous_caller_reads_as_nobody(self, client):
        response = client.get("/probe")

        assert response.status_code == 200
        for transaction in ("before", "after"):
            principal = response.json()[transaction]
            assert (principal["user_id_cast"], principal["roles_cast"]) == (None, None)

    @pytest.mark.parametrize(
        "second_request",
        ["/probe", "/unauthenticated-probe"],
        ids=["anonymous-caller", "route-without-authentication"],
    )
    def test_request_principal_does_not_leak_to_the_next_request_on_a_pooled_connection(
        self, client, session, jwt_for, second_request
    ):
        with jwt_for(TEST_USER["username"]):
            first = client.get("/probe", headers={"Authorization": "Bearer test"}).json()["after"]
        second = client.get(second_request).json()
        second = second.get("before", second)

        assert first["user_id"] == str(_user_id(session, TEST_USER["username"]))
        assert second["backend_pid"] == first["backend_pid"]
        assert second["user_id"] == ""
        assert second["user_id_cast"] is None
