# ruff: noqa: E402

import os
import subprocess
import sys
import uuid
from pathlib import Path

import pytest

pytest.importorskip("psycopg2")

from sqlalchemy import URL, create_engine, text
from sqlalchemy.exc import OperationalError, ProgrammingError
from sqlalchemy.orm import Session
from sqlalchemy.pool import NullPool

from mavedb.db import bootstrap
from mavedb.db.bootstrap import apply_grants, bootstrap_cluster, bootstrap_database, role_problems, setup
from mavedb.db.rls import (
    API_ROLE,
    FUNCTIONS,
    GUARDED,
    MIGRATE_ROLE,
    OWNER_ROLE,
    RELATIONS,
    WORKER_ROLE,
    ApiAccess,
)
from mavedb.db.view import REFRESH_MAT_VIEWS_FUNCTION, refresh_all_mat_views
from tests.conftest import TEST_ROLE_PASSWORD

API_ROOT = Path(__file__).resolve().parents[2]

TABLE_PRIVILEGES = {"SELECT", "INSERT", "UPDATE", "DELETE"}

# What each part may change, read back as plain values so two snapshots compare equal when nothing changed.
CATALOG_QUERIES = {
    "roles": "SELECT rolname, rolcanlogin, rolsuper, rolinherit, rolcreaterole, rolbypassrls FROM pg_roles "
    "WHERE rolname = ANY(:roles) ORDER BY 1",
    "memberships": "SELECT r.rolname, m.rolname, am.admin_option FROM pg_auth_members am "
    "JOIN pg_roles r ON r.oid = am.roleid JOIN pg_roles m ON m.oid = am.member "
    "WHERE r.rolname = ANY(:roles) OR m.rolname = ANY(:roles) ORDER BY 1, 2",
    "role_settings": "SELECT r.rolname, s.setdatabase, s.setconfig FROM pg_db_role_setting s "
    "JOIN pg_roles r ON r.oid = s.setrole WHERE r.rolname = ANY(:roles) ORDER BY 1, 2",
    "parameter_acls": "SELECT p.parname, r.rolname, a.privilege_type FROM pg_parameter_acl p "
    "CROSS JOIN LATERAL aclexplode(p.paracl) a JOIN pg_roles r ON r.oid = a.grantee "
    "WHERE r.rolname = ANY(:roles) ORDER BY 1, 2, 3",
    "schema_acl": "SELECT nspowner::regrole::text, nspacl::text FROM pg_namespace WHERE nspname = 'public'",
    "default_acls": "SELECT defaclrole::regrole::text, defaclobjtype, defaclacl::text FROM pg_default_acl ORDER BY 1, 2",
    "relations": "SELECT c.relname, c.relowner::regrole::text, c.relacl::text FROM pg_class c "
    "JOIN pg_namespace n ON n.oid = c.relnamespace WHERE n.nspname = 'public' AND c.relkind IN ('r', 'p', 'f', 'v', 'm', 'S') "
    "ORDER BY 1",
    "functions": "SELECT p.oid::regprocedure::text, p.proowner::regrole::text, p.proacl::text FROM pg_proc p "
    "JOIN pg_namespace n ON n.oid = p.pronamespace WHERE n.nspname = 'public' ORDER BY 1",
}

# Every grant on the schema's relations and sequences to anyone but their owner, with a missing ACL read as
# Postgres's default.
RELATION_GRANTS = """
SELECT c.relname, coalesce(g.rolname, 'PUBLIC'), a.privilege_type
  FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
  CROSS JOIN LATERAL aclexplode(
      coalesce(c.relacl, acldefault((CASE c.relkind WHEN 'S' THEN 's' ELSE 'r' END)::"char", c.relowner))) a
  LEFT JOIN pg_roles g ON g.oid = a.grantee
 WHERE n.nspname = 'public' AND c.relkind IN ('r', 'p', 'f', 'v', 'm', 'S') AND a.grantee <> c.relowner
"""

FUNCTION_GRANTS = """
SELECT p.proname, coalesce(g.rolname, 'PUBLIC'), a.privilege_type
  FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace
  CROSS JOIN LATERAL aclexplode(coalesce(p.proacl, acldefault('f'::"char", p.proowner))) a
  LEFT JOIN pg_roles g ON g.oid = a.grantee
 WHERE n.nspname = 'public' AND a.grantee <> p.proowner
"""


def _has_privileges(conn, role: str, table: str, privileges: list[str]) -> bool:
    return all(
        conn.execute(text("SELECT has_table_privilege(:r, :t, :p)"), {"r": role, "t": table, "p": p}).scalar_one()
        for p in privileges
    )


def _grants(conn, query: str = RELATION_GRANTS) -> dict[str, set[tuple[str, str]]]:
    grants: dict[str, set[tuple[str, str]]] = {}
    for name, grantee, privilege in conn.execute(text(query)):
        grants.setdefault(name, set()).add((grantee, privilege))
    return grants


def _expected_grantees(name: str) -> list[str]:
    relation = RELATIONS.get(name)
    if relation is None:
        return []
    return [API_ROLE] * (relation.api is not ApiAccess.NONE) + [WORKER_ROLE] * relation.worker


def _snapshot(engine, api_role: str) -> dict[str, list]:
    # Only these roles: concurrent tests on the same cluster create and drop roles of their own.
    roles = [OWNER_ROLE, MIGRATE_ROLE, WORKER_ROLE, api_role]
    with engine.connect() as conn:
        return {
            name: [tuple(row) for row in conn.execute(text(query), {"roles": roles})]
            for name, query in CATALOG_QUERIES.items()
        }


def _url(postgresql, database: str, username: str | None = None, password: str | None = None) -> URL:
    return URL.create(
        "postgresql+psycopg2",
        username=username or postgresql.info.user,
        password=password or postgresql.info.password or None,
        host=postgresql.info.host,
        port=postgresql.info.port,
        database=database,
    )


def _drop_role(conn, role: str) -> None:
    if conn.execute(text("SELECT EXISTS (SELECT FROM pg_roles WHERE rolname = :r)"), {"r": role}).scalar_one():
        # Also revokes its cluster-wide grants, such as on parameters, which would otherwise block the drop.
        conn.execute(text(f"DROP OWNED BY {role}"))
        conn.execute(text(f"DROP ROLE {role}"))


@pytest.fixture
def superuser(postgresql):
    engine = create_engine(_url(postgresql, postgresql.info.dbname), poolclass=NullPool)
    yield engine
    engine.dispose()


@pytest.fixture
def fresh_role(superuser):
    """Return a factory for roles no other test uses, dropped afterwards."""
    created = []

    def _create(prefix: str = "mavedb_test") -> str:
        role = f"{prefix}_{uuid.uuid4().hex[:8]}"
        created.append(role)
        return role

    yield _create
    with superuser.connect().execution_options(isolation_level="AUTOCOMMIT") as conn:
        for role in reversed(created):
            _drop_role(conn, role)


@pytest.fixture
def fresh_api_role(fresh_role, monkeypatch):
    """Stand a role no other test uses in for the api's, so a test can change it without touching the cluster's.

    The api role is shared by every run against the same cluster, and these tests give it objects, databases,
    attributes and memberships.
    """
    role = fresh_role("mavedb_test_api")
    monkeypatch.setattr(bootstrap, "API_ROLE", role)
    return role


@pytest.fixture
def fresh_database(postgresql, superuser, fresh_api_role):
    """Return a factory for an empty database on the test cluster, as on a new deployment.

    Depends on ``fresh_api_role`` so its databases are dropped before the role, which has grants in them.
    """
    created = []

    def _create(owner: str | None = None) -> str:
        name = f"{postgresql.info.dbname}_fresh_{len(created)}"
        with superuser.connect().execution_options(isolation_level="AUTOCOMMIT") as conn:
            conn.execute(text(f"CREATE DATABASE {name} TEMPLATE template0" + (f" OWNER {owner}" if owner else "")))
        created.append(name)
        return name

    yield _create
    with superuser.connect().execution_options(isolation_level="AUTOCOMMIT") as conn:
        for name in created:
            conn.execute(text(f"DROP DATABASE IF EXISTS {name} WITH (FORCE)"))


def _run_all_parts(engine) -> None:
    with engine.begin() as conn:
        bootstrap_cluster(conn)
    with engine.begin() as conn:
        bootstrap_database(conn)
    with engine.begin() as conn:
        apply_grants(conn)


@pytest.mark.integration
class TestBootstrapParts:
    def test_each_part_on_a_fresh_database_second_run_changes_nothing(self, postgresql, fresh_database, fresh_api_role):
        engine = create_engine(_url(postgresql, fresh_database()), poolclass=NullPool)
        _run_all_parts(engine)
        with engine.begin() as conn:
            conn.execute(text(f"SET LOCAL ROLE {OWNER_ROLE}"))
            conn.execute(text("CREATE TABLE widgets (id serial PRIMARY KEY)"))
            conn.execute(text(REFRESH_MAT_VIEWS_FUNCTION.statement))

        _run_all_parts(engine)
        first = _snapshot(engine, fresh_api_role)
        parts = {"cluster": bootstrap_cluster, "database": bootstrap_database, "grants": apply_grants}
        for name, part in parts.items():
            with engine.begin() as conn:
                part(conn)
            assert _snapshot(engine, fresh_api_role) == first, name

        engine.dispose()

    def test_bootstrap_cluster_new_role_has_no_password_and_existing_password_is_kept(self, superuser, fresh_api_role):
        with superuser.begin() as conn:
            bootstrap_cluster(conn)
        with superuser.begin() as conn:
            assert (
                conn.execute(
                    text("SELECT rolpassword FROM pg_authid WHERE rolname = :r"), {"r": fresh_api_role}
                ).scalar_one()
                is None
            )
            conn.execute(text(f"ALTER ROLE {fresh_api_role} PASSWORD 'kept'"))
            password = conn.execute(
                text("SELECT rolpassword FROM pg_authid WHERE rolname = :r"), {"r": fresh_api_role}
            ).scalar_one()

        with superuser.begin() as conn:
            bootstrap_cluster(conn)
        with superuser.connect() as conn:
            assert (
                conn.execute(
                    text("SELECT rolpassword FROM pg_authid WHERE rolname = :r"), {"r": fresh_api_role}
                ).scalar_one()
                == password
            )

    @pytest.mark.parametrize("attribute", ["SUPERUSER", "BYPASSRLS", "CREATEROLE", "CREATEDB", "REPLICATION"])
    def test_bootstrap_cluster_existing_role_with_unsafe_attribute_raises(self, superuser, fresh_api_role, attribute):
        with superuser.begin() as conn:
            conn.execute(text(f"CREATE ROLE {fresh_api_role} LOGIN {attribute}"))

        with superuser.begin() as conn, pytest.raises(RuntimeError, match=f"{fresh_api_role} has {attribute}"):
            bootstrap_cluster(conn)

    def test_bootstrap_cluster_existing_role_in_another_role_raises(self, superuser, fresh_api_role):
        with superuser.begin() as conn:
            conn.execute(text(f"CREATE ROLE {fresh_api_role} LOGIN"))
            conn.execute(text(f"GRANT pg_read_all_data TO {fresh_api_role}"))

        with (
            superuser.begin() as conn,
            pytest.raises(RuntimeError, match=f"{fresh_api_role} is a member of pg_read_all_data"),
        ):
            bootstrap_cluster(conn)

    def test_bootstrap_cluster_another_role_in_an_existing_role_raises(self, superuser, fresh_role, fresh_api_role):
        stranger = fresh_role("mavedb_test_stranger")
        with superuser.begin() as conn:
            conn.execute(text(f"CREATE ROLE {fresh_api_role} LOGIN"))
            conn.execute(text(f"CREATE ROLE {stranger} LOGIN IN ROLE {fresh_api_role}"))

        with (
            superuser.begin() as conn,
            pytest.raises(RuntimeError, match=f"{stranger} is a member of {fresh_api_role}"),
        ):
            bootstrap_cluster(conn)

    @pytest.mark.parametrize("parameter", ["session_replication_role", "log_statement"])
    def test_bootstrap_cluster_existing_role_that_can_set_a_superuser_parameter_raises(
        self, superuser, fresh_api_role, parameter
    ):
        with superuser.begin() as conn:
            conn.execute(text(f"CREATE ROLE {fresh_api_role} LOGIN"))
            conn.execute(text(f"GRANT SET ON PARAMETER {parameter} TO {fresh_api_role}"))

        with superuser.begin() as conn, pytest.raises(RuntimeError, match=f"{fresh_api_role} can set {parameter}"):
            bootstrap_cluster(conn)

    def test_bootstrap_database_api_able_to_create_in_public_raises(self, postgresql, fresh_database, fresh_api_role):
        engine = create_engine(_url(postgresql, fresh_database()), poolclass=NullPool)
        with engine.begin() as conn:
            bootstrap_cluster(conn)
            conn.execute(text(f"GRANT CREATE ON SCHEMA public TO {fresh_api_role}"))

        with (
            engine.begin() as conn,
            pytest.raises(RuntimeError, match=f"{fresh_api_role} can create objects in schema public"),
        ):
            bootstrap_database(conn)
        engine.dispose()

    @pytest.mark.parametrize(
        "grant, problem",
        [
            ("GRANT CREATE ON SCHEMA public TO {role}", "can create objects in schema public"),
            ("GRANT CREATE ON DATABASE {database} TO {role}", "can create schemas in this database"),
        ],
        ids=["schema", "database"],
    )
    def test_apply_grants_api_able_to_create_objects_raises(
        self, postgresql, fresh_database, fresh_api_role, grant, problem
    ):
        database = fresh_database()
        engine = create_engine(_url(postgresql, database), poolclass=NullPool)
        with engine.begin() as conn:
            setup(conn)
            conn.execute(text(grant.format(role=fresh_api_role, database=database)))

        with engine.begin() as conn, pytest.raises(RuntimeError, match=f"{fresh_api_role} {problem}"):
            apply_grants(conn)
        engine.dispose()

    @pytest.mark.parametrize(
        "statement",
        ["CREATE TABLE widgets (id int)", "CREATE FUNCTION unlisted() RETURNS int LANGUAGE sql AS 'SELECT 1'"],
        ids=["table", "function"],
    )
    def test_apply_grants_object_owned_by_another_role_raises(self, postgresql, fresh_database, statement):
        # Made by hand as the superuser, so the grants step can't govern it and its owner skips its policies.
        engine = create_engine(_url(postgresql, fresh_database()), poolclass=NullPool)
        with engine.begin() as conn:
            setup(conn)
            conn.execute(text(statement))

        with engine.begin() as conn, pytest.raises(RuntimeError, match=f"belong to a role other than {OWNER_ROLE}"):
            apply_grants(conn)
        engine.dispose()

    def test_apply_grants_role_given_a_membership_after_setup_raises(self, postgresql, fresh_database, fresh_api_role):
        engine = create_engine(_url(postgresql, fresh_database()), poolclass=NullPool)
        with engine.begin() as conn:
            setup(conn)
            conn.execute(text(f"GRANT pg_read_all_data TO {fresh_api_role}"))

        with engine.begin() as conn, pytest.raises(RuntimeError, match="aren't as the bootstrap sets them up"):
            apply_grants(conn)
        engine.dispose()

    def test_apply_grants_api_role_owning_objects_moves_them_to_the_owner(
        self, postgresql, fresh_database, fresh_api_role
    ):
        # As on staging and production before this change, where the api's login owns every table.
        engine = create_engine(_url(postgresql, fresh_database()), poolclass=NullPool)
        with engine.begin() as conn:
            bootstrap_cluster(conn)
            conn.execute(text("CREATE TABLE licenses (id serial PRIMARY KEY)"))
            conn.execute(text(f"ALTER TABLE licenses OWNER TO {fresh_api_role}"))
        with engine.begin() as conn:
            bootstrap_database(conn)
        with engine.begin() as conn:
            apply_grants(conn)

        with engine.connect() as conn:
            owners = conn.execute(
                text(
                    "SELECT relname, relowner::regrole::text FROM pg_class "
                    "WHERE relname IN ('licenses', 'licenses_id_seq') ORDER BY 1"
                )
            ).all()
            assert [tuple(row) for row in owners] == [("licenses", OWNER_ROLE), ("licenses_id_seq", OWNER_ROLE)]
            assert _has_privileges(conn, fresh_api_role, "licenses", sorted(TABLE_PRIVILEGES))
        engine.dispose()

    def test_apply_grants_leaves_exactly_the_listed_grants(self, postgresql, fresh_database, fresh_api_role):
        engine = create_engine(_url(postgresql, fresh_database()), poolclass=NullPool)
        with engine.begin() as conn:
            setup(conn)
            conn.execute(text(f"SET LOCAL ROLE {OWNER_ROLE}"))
            conn.execute(text("CREATE TABLE licenses (id serial PRIMARY KEY)"))
            conn.execute(text("CREATE TABLE annotation_event (id serial PRIMARY KEY)"))
            conn.execute(text("CREATE TABLE widgets (id serial PRIMARY KEY)"))
            conn.execute(text("CREATE FUNCTION unlisted() RETURNS int LANGUAGE sql AS 'SELECT 1'"))
            # Stray grants, as a migration or an operator could leave them.
            conn.execute(text("GRANT SELECT ON widgets TO PUBLIC"))
            conn.execute(text(f"GRANT ALL ON annotation_event TO {fresh_api_role}"))
            conn.execute(text(f"GRANT TRUNCATE ON licenses TO {WORKER_ROLE}"))
            conn.execute(text(f"GRANT EXECUTE ON FUNCTION unlisted() TO PUBLIC, {fresh_api_role}"))
        with engine.begin() as conn:
            apply_grants(conn)

        with engine.connect() as conn:
            grants = _grants(conn)
            function_grants = _grants(conn, FUNCTION_GRANTS)
        engine.dispose()

        dml = {(role, p) for role in (fresh_api_role, WORKER_ROLE) for p in TABLE_PRIVILEGES}
        assert grants["licenses"] == dml
        assert grants["licenses_id_seq"] == {(r, p) for r in (fresh_api_role, WORKER_ROLE) for p in ("USAGE", "SELECT")}
        assert grants["annotation_event"] == {(WORKER_ROLE, p) for p in TABLE_PRIVILEGES}
        assert "widgets" not in grants
        assert "widgets_id_seq" not in grants
        assert "unlisted" not in function_grants

    def test_apply_grants_function_unlike_its_entry_gets_no_executors(self, postgresql, fresh_database, fresh_api_role):
        # Listed as SECURITY DEFINER for the worker; one that isn't is not the function that was reviewed.
        engine = create_engine(_url(postgresql, fresh_database()), poolclass=NullPool)
        with engine.begin() as conn:
            setup(conn)
            conn.execute(text(f"SET LOCAL ROLE {OWNER_ROLE}"))
            conn.execute(
                text(
                    "CREATE FUNCTION mavedb_refresh_mat_views(view_name regclass, concurrent boolean) RETURNS void "
                    "LANGUAGE sql AS ''"
                )
            )
        with engine.begin() as conn:
            apply_grants(conn)
            assert "mavedb_refresh_mat_views" not in _grants(conn, FUNCTION_GRANTS)
        engine.dispose()

    def test_apply_grants_definer_function_without_pinned_search_path_gets_no_executors(
        self, postgresql, fresh_database, fresh_api_role
    ):
        engine = create_engine(_url(postgresql, fresh_database()), poolclass=NullPool)
        with engine.begin() as conn:
            setup(conn)
            conn.execute(text(f"SET LOCAL ROLE {OWNER_ROLE}"))
            conn.execute(
                text(
                    "CREATE FUNCTION mavedb_refresh_mat_views(view_name regclass, concurrent boolean) RETURNS void "
                    "LANGUAGE sql SECURITY DEFINER AS ''"
                )
            )
        with engine.begin() as conn:
            apply_grants(conn)
            assert "mavedb_refresh_mat_views" not in _grants(conn, FUNCTION_GRANTS)
        engine.dispose()

    @pytest.mark.parametrize(
        "create, api_granted",
        [
            ("CREATE VIEW guarded_relation AS SELECT 1 AS id", False),
            ("CREATE VIEW guarded_relation WITH (security_invoker = true) AS SELECT 1 AS id", True),
            ("CREATE MATERIALIZED VIEW guarded_relation AS SELECT 1 AS id", False),
        ],
        ids=["view", "security-invoker-view", "materialized-view"],
    )
    def test_apply_grants_guarded_view_reaches_the_api_only_when_it_reads_as_the_caller(
        self, postgresql, fresh_database, fresh_api_role, monkeypatch, create, api_granted
    ):
        monkeypatch.setitem(RELATIONS, "guarded_relation", GUARDED)
        engine = create_engine(_url(postgresql, fresh_database()), poolclass=NullPool)
        with engine.begin() as conn:
            setup(conn)
            conn.execute(text(f"SET LOCAL ROLE {OWNER_ROLE}"))
            conn.execute(text(create))
        with engine.begin() as conn:
            apply_grants(conn)
            grants = _grants(conn).get("guarded_relation", set())
        engine.dispose()

        assert ((fresh_api_role, "SELECT") in grants) is api_granted
        assert (WORKER_ROLE, "SELECT") in grants

    def test_apply_grants_foreign_table_gets_exactly_the_listed_grants(
        self, postgresql, fresh_database, fresh_api_role
    ):
        engine = create_engine(_url(postgresql, fresh_database()), poolclass=NullPool)
        with engine.begin() as conn:
            setup(conn)
            # A wrapper with no handler: enough to create a foreign table, not to read one.
            conn.execute(text("CREATE FOREIGN DATA WRAPPER test_fdw"))
            conn.execute(text("CREATE SERVER test_server FOREIGN DATA WRAPPER test_fdw"))
            conn.execute(text("CREATE FOREIGN TABLE licenses (id int) SERVER test_server"))
            conn.execute(text(f"ALTER FOREIGN TABLE licenses OWNER TO {OWNER_ROLE}"))
            conn.execute(text("GRANT SELECT ON licenses TO PUBLIC"))
        with engine.begin() as conn:
            apply_grants(conn)
            grants = _grants(conn)
        engine.dispose()

        assert grants["licenses"] == {(role, p) for role in (fresh_api_role, WORKER_ROLE) for p in TABLE_PRIVILEGES}

    def test_bootstrap_database_new_function_is_closed_to_public(self, postgresql, fresh_database):
        engine = create_engine(_url(postgresql, fresh_database()), poolclass=NullPool)
        with engine.begin() as conn:
            setup(conn)
            conn.execute(text(f"SET LOCAL ROLE {OWNER_ROLE}"))
            conn.execute(text("CREATE FUNCTION unlisted() RETURNS int LANGUAGE sql AS 'SELECT 1'"))
            assert "unlisted" not in _grants(conn, FUNCTION_GRANTS)
        engine.dispose()

    def test_apply_grants_api_role_owning_a_database_raises(self, postgresql, fresh_database, fresh_api_role):
        engine = create_engine(_url(postgresql, fresh_database()), poolclass=NullPool)
        with engine.begin() as conn:
            setup(conn)
        fresh_database(owner=fresh_api_role)

        with engine.begin() as conn, pytest.raises(RuntimeError, match="owns databases"):
            apply_grants(conn)
        engine.dispose()

    def test_apply_grants_without_setup_raises(self, postgresql, fresh_database):
        engine = create_engine(_url(postgresql, fresh_database()), poolclass=NullPool)
        with engine.begin() as conn, pytest.raises(RuntimeError, match="no roles set up"):
            apply_grants(conn)
        engine.dispose()

    def test_parts_as_a_non_superuser_master_leave_no_borrowed_memberships(
        self, postgresql, superuser, fresh_role, fresh_database, fresh_api_role
    ):
        # RDS's master user can create roles but isn't a superuser, so it has to borrow the owner's and the
        # api's privileges to change their objects. It must give them back.
        master = fresh_role("mavedb_test_master")
        with superuser.begin() as conn:
            conn.execute(text(f"CREATE ROLE {master} LOGIN CREATEROLE PASSWORD '{TEST_ROLE_PASSWORD}'"))
            bootstrap_cluster(conn)
        database = fresh_database(owner=master)
        with create_engine(_url(postgresql, database), poolclass=NullPool).begin() as conn:
            conn.execute(text("CREATE TABLE widgets (id serial PRIMARY KEY)"))
            conn.execute(text(f"ALTER TABLE widgets OWNER TO {fresh_api_role}"))

        engine = create_engine(_url(postgresql, database, master, TEST_ROLE_PASSWORD), poolclass=NullPool)
        try:
            with engine.begin() as conn:
                bootstrap_database(conn)
            with engine.begin() as conn:
                apply_grants(conn)

            with engine.connect() as conn:
                owner = conn.execute(text("SELECT relowner::regrole::text FROM pg_class WHERE relname = 'widgets'"))
                assert owner.scalar_one() == OWNER_ROLE
                memberships = conn.execute(
                    text(
                        "SELECT count(*) FROM pg_auth_members WHERE member = (SELECT oid FROM pg_roles WHERE rolname = :m)"
                    ),
                    {"m": master},
                )
                assert memberships.scalar_one() == 0
        finally:
            engine.dispose()
            with superuser.connect().execution_options(isolation_level="AUTOCOMMIT") as conn:
                conn.execute(text(f"DROP DATABASE IF EXISTS {database} WITH (FORCE)"))

    def test_privileges_of_python_error_gives_the_borrowed_role_back(
        self, postgresql, superuser, fresh_role, fresh_database
    ):
        # The transaction still works after an error that isn't the database's, so a caller could commit it.
        master = fresh_role("mavedb_test_master")
        with superuser.begin() as conn:
            conn.execute(text(f"CREATE ROLE {master} LOGIN CREATEROLE PASSWORD '{TEST_ROLE_PASSWORD}'"))
            bootstrap_cluster(conn)
        engine = create_engine(_url(postgresql, fresh_database(), master, TEST_ROLE_PASSWORD), poolclass=NullPool)

        with engine.begin() as conn:
            with pytest.raises(ValueError), bootstrap._privileges_of(conn, OWNER_ROLE):
                raise ValueError("not the database's")
        with engine.connect() as conn:
            assert not conn.execute(
                text("SELECT pg_has_role(:m, :o, 'MEMBER')"), {"m": master, "o": OWNER_ROLE}
            ).scalar_one()
        engine.dispose()


@pytest.mark.integration
class TestRoles:
    def test_role_problems_on_the_test_cluster_finds_none(self, session):
        assert role_problems(session.connection()) == []

    def test_roles_only_membership_is_migrate_in_owner(self, session):
        memberships = session.execute(
            text(
                "SELECT r.rolname, m.rolname FROM pg_auth_members am JOIN pg_roles r ON r.oid = am.roleid "
                "JOIN pg_roles m ON m.oid = am.member WHERE r.rolname = ANY(:roles) OR m.rolname = ANY(:roles)"
            ),
            {"roles": [OWNER_ROLE, MIGRATE_ROLE, WORKER_ROLE, API_ROLE]},
        ).all()

        assert [tuple(m) for m in memberships] == [(OWNER_ROLE, MIGRATE_ROLE)]

    def test_owner_cannot_log_in(self, login_as):
        with pytest.raises(OperationalError, match="not permitted to log in"):
            login_as(OWNER_ROLE).connect()

    def test_migrate_runs_as_owner(self, login_as):
        with login_as(MIGRATE_ROLE).connect() as conn:
            assert conn.execute(text("SELECT current_user")).scalar_one() == OWNER_ROLE

    def test_worker_lock_timeout_comes_from_its_role(self, login_as):
        with login_as(WORKER_ROLE).connect() as conn:
            assert conn.execute(text("SHOW lock_timeout")).scalar_one() == "1min"
            assert conn.execute(text("SHOW statement_timeout")).scalar_one() == "0"

    def test_table_created_by_migrate_is_owned_by_owner_and_closed_until_listed(self, login_as):
        with login_as(MIGRATE_ROLE).begin() as conn:
            conn.execute(text("CREATE TABLE widgets (id serial PRIMARY KEY, name text)"))
            assert (
                conn.execute(
                    text("SELECT relowner::regrole::text FROM pg_class WHERE relname = 'widgets'")
                ).scalar_one()
                == OWNER_ROLE
            )

        for role in (API_ROLE, WORKER_ROLE):
            with login_as(role).connect() as conn, pytest.raises(ProgrammingError, match="permission denied"):
                conn.execute(text("SELECT name FROM widgets"))

    @pytest.mark.parametrize("role", [API_ROLE, WORKER_ROLE])
    @pytest.mark.parametrize(
        "statement",
        ["CREATE TABLE intruder (id int)", f"SET ROLE {OWNER_ROLE}", "SET session_replication_role = replica"],
        ids=["create", "set-role", "replication-role"],
    )
    def test_api_and_worker_cannot_escalate(self, login_as, role, statement):
        with login_as(role).connect() as conn, pytest.raises(ProgrammingError):
            conn.execute(text(statement))

    def test_migrate_cannot_set_session_replication_role(self, login_as):
        # It turns off foreign key checks; restores set it as the master user.
        with login_as(MIGRATE_ROLE).connect() as conn, pytest.raises(ProgrammingError, match="permission denied"):
            conn.execute(text("SET session_replication_role = replica"))

    def test_worker_has_no_privileges_on_access_keys(self, session, login_as):
        privileges = session.execute(
            text(
                "SELECT privilege_type FROM information_schema.role_table_grants WHERE table_name = 'access_keys' AND grantee = :r"
            ),
            {"r": WORKER_ROLE},
        ).all()
        assert privileges == []

        with login_as(WORKER_ROLE).connect() as conn, pytest.raises(ProgrammingError, match="permission denied"):
            conn.execute(text("SELECT 1 FROM access_keys"))

    def test_api_has_no_privileges_on_relations_listed_without_api_access(self, session):
        ungranted = sorted(name for name, relation in RELATIONS.items() if relation.api is ApiAccess.NONE)
        granted = (
            session.execute(
                text(
                    "SELECT DISTINCT table_name FROM information_schema.role_table_grants WHERE grantee = :r AND table_name = ANY(:t)"
                ),
                {"r": API_ROLE, "t": ungranted},
            )
            .scalars()
            .all()
        )

        assert granted == []

    def test_api_keeps_grants_on_tables_score_set_delete_cascades_through(self, session):
        # Score set delete loads these collections in the ORM, so revoking them would break it (#827, G1).
        for table in (
            "mapped_variants",
            "gnomad_variants_mapped_variants",
            "mapped_variants_clinical_controls",
            "target_gene_mappings",
            "keywords",
            "scoreset_keywords",
            "experiment_keywords",
            "experiment_set_keywords",
        ):
            assert _has_privileges(session, API_ROLE, table, ["SELECT", "DELETE"]), table


@pytest.mark.integration
class TestRefreshMatViews:
    def test_worker_refreshes_through_the_function(self, login_as):
        with Session(login_as(WORKER_ROLE)) as session:
            refresh_all_mat_views(session)
            refresh_all_mat_views(session, concurrently=False)
            session.commit()

    def test_worker_cannot_refresh_directly(self, login_as):
        with login_as(WORKER_ROLE).connect() as conn, pytest.raises(ProgrammingError, match="must be owner"):
            conn.execute(text("REFRESH MATERIALIZED VIEW published_variants_materialized_view"))

    def test_api_cannot_execute_the_function(self, login_as):
        with Session(login_as(API_ROLE)) as session, pytest.raises(ProgrammingError, match="permission denied"):
            refresh_all_mat_views(session)

    def test_function_given_a_table_raises(self, login_as):
        with Session(login_as(WORKER_ROLE)) as session, pytest.raises(Exception, match="not a materialized view"):
            session.execute(text("SELECT mavedb_refresh_mat_views('scoresets'::regclass)"))

    def test_migration_creates_the_function_the_template_does(self):
        migration = (API_ROOT / "alembic" / "versions" / "4b8e2c7d1f3a_add_mavedb_refresh_mat_views.py").read_text()

        assert REFRESH_MAT_VIEWS_FUNCTION.statement.strip() in migration


@pytest.fixture
def replayed_database(postgresql, superuser):
    """Return an engine on a new database set up for the roles and upgraded to head as the migrate login."""
    database = f"{postgresql.info.dbname}_replayed"
    with superuser.connect().execution_options(isolation_level="AUTOCOMMIT") as conn:
        conn.execute(text(f"CREATE DATABASE {database} TEMPLATE template0"))
    engine = create_engine(_url(postgresql, database), poolclass=NullPool)
    with engine.begin() as conn:
        setup(conn)

    env = {
        **os.environ,
        "DB_HOST": postgresql.info.host,
        "DB_PORT": str(postgresql.info.port),
        "DB_DATABASE_NAME": database,
        "DB_USERNAME": MIGRATE_ROLE,
        "DB_PASSWORD": TEST_ROLE_PASSWORD,
    }
    subprocess.run([sys.executable, "-m", "alembic", "upgrade", "head"], cwd=API_ROOT, env=env, check=True)

    yield engine
    engine.dispose()
    with superuser.connect().execution_options(isolation_level="AUTOCOMMIT") as conn:
        conn.execute(text(f"DROP DATABASE IF EXISTS {database} WITH (FORCE)"))


@pytest.mark.integration
class TestMigrationReplay:
    def test_upgrade_leaves_owner_owned_schema_and_worker_only_function(self, replayed_database, login_as):
        with replayed_database.connect() as conn:
            owners = (
                conn.execute(
                    text(
                        "SELECT DISTINCT relowner::regrole::text FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace "
                        "WHERE n.nspname = 'public' AND c.relkind IN ('r', 'p', 'f', 'v', 'm', 'S')"
                    )
                )
                .scalars()
                .all()
            )
            executors = (
                conn.execute(
                    text(
                        "SELECT r FROM unnest(ARRAY['public', :api, :worker]) r "
                        "WHERE has_function_privilege(r, 'mavedb_refresh_mat_views(regclass, boolean)', 'EXECUTE')"
                    ),
                    {"api": API_ROLE, "worker": WORKER_ROLE},
                )
                .scalars()
                .all()
            )
            function_owner = conn.execute(
                text("SELECT proowner::regrole::text FROM pg_proc WHERE proname = 'mavedb_refresh_mat_views'")
            ).scalar_one()
            database = conn.execute(text("SELECT current_database()")).scalar_one()

        assert owners == [OWNER_ROLE]
        assert function_owner == OWNER_ROLE
        assert executors == [WORKER_ROLE]
        with Session(login_as(WORKER_ROLE, database)) as session:
            refresh_all_mat_views(session)

    def test_upgrade_every_relation_and_function_is_listed(self, replayed_database):
        # A new table, view or function fails here until rls.py says who may use it.
        with replayed_database.connect() as conn:
            relations = set(
                conn.execute(
                    text(
                        "SELECT c.relname FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace "
                        "WHERE n.nspname = 'public' AND c.relkind IN ('r', 'p', 'f', 'v', 'm')"
                    )
                ).scalars()
            )
            functions = conn.execute(
                text(
                    "SELECT p.proname, p.prosecdef FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace "
                    "WHERE n.nspname = 'public'"
                )
            ).all()

        assert sorted(relations - RELATIONS.keys()) == []
        assert sorted(name for name, _ in functions if name not in FUNCTIONS) == []
        assert (
            sorted(
                name for name, definer in functions if name in FUNCTIONS and FUNCTIONS[name].security_definer != definer
            )
            == []
        )

    def test_upgrade_guarded_relations_read_as_the_caller_and_definers_pin_search_path(self, replayed_database):
        # A guarded view that reads as its owner, or a guarded materialized view, would skip the policies the
        # entry promises; a SECURITY DEFINER function with an open search_path runs whatever a caller plants.
        with replayed_database.connect() as conn:
            relations = conn.execute(
                text(
                    "SELECT c.relname, c.relkind, coalesce((SELECT o.option_value::boolean "
                    "FROM pg_options_to_table(c.reloptions) o WHERE o.option_name = 'security_invoker'), false) "
                    "FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace "
                    "WHERE n.nspname = 'public' AND c.relkind IN ('v', 'm')"
                )
            ).all()
            definers = conn.execute(
                text(
                    "SELECT p.proname, p.proconfig FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace "
                    "WHERE n.nspname = 'public' AND p.prosecdef"
                )
            ).all()

        guarded = {name for name, relation in RELATIONS.items() if relation.api is ApiAccess.GUARDED}
        assert (
            sorted(name for name, kind, invoker in relations if name in guarded and (kind == "m" or not invoker)) == []
        )
        assert (
            sorted(name for name, config in definers if not any(c.startswith("search_path=") for c in config or []))
            == []
        )

    def test_upgrade_grants_match_the_registry(self, replayed_database):
        # The baseline is dumped without privileges, so these grants have to come from rls.py alone. A grant
        # anything else made would differ here, and be lost at the next squash.
        with replayed_database.connect() as conn:
            kinds = dict(
                conn.execute(
                    text(
                        "SELECT c.relname, c.relkind FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace "
                        "WHERE n.nspname = 'public' AND c.relkind IN ('r', 'p', 'f', 'v', 'm')"
                    )
                ).all()
            )
            sequence_tables = dict(
                conn.execute(
                    text(
                        "SELECT s.relname, t.relname FROM pg_class s JOIN pg_namespace n ON n.oid = s.relnamespace "
                        "LEFT JOIN pg_depend d ON d.classid = 'pg_class'::regclass AND d.objid = s.oid "
                        "AND d.refclassid = 'pg_class'::regclass AND d.deptype IN ('a', 'i') "
                        "LEFT JOIN pg_class t ON t.oid = d.refobjid WHERE n.nspname = 'public' AND s.relkind = 'S'"
                    )
                ).all()
            )
            function_names = set(
                conn.execute(
                    text(
                        "SELECT p.proname FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace WHERE n.nspname = 'public'"
                    )
                ).scalars()
            )
            grants = _grants(conn)
            function_grants = _grants(conn, FUNCTION_GRANTS)

        expected = {}
        for name, kind in kinds.items():
            privileges = TABLE_PRIVILEGES if kind in ("r", "p", "f") else {"SELECT"}
            expected[name] = {(role, p) for role in _expected_grantees(name) for p in privileges}
        for sequence, table in sequence_tables.items():
            expected[sequence] = {(role, p) for role in _expected_grantees(table) for p in ("USAGE", "SELECT")}
        expected_functions = {
            name: {(role, "EXECUTE") for role in FUNCTIONS[name].executors}
            for name in function_names & FUNCTIONS.keys()
        }

        assert {name: grants.get(name, set()) for name in expected} == expected
        assert {name: function_grants.get(name, set()) for name in function_names} == {
            name: expected_functions.get(name, set()) for name in function_names
        }
