"""Create the database roles, check they can be trusted, and make their grants match ``mavedb.db.rls`` (#827).

Three parts, each safe to re-run, and none of which sets or changes a password:

1. ``bootstrap_cluster``: once per cluster, as the master user. Creates the roles and their settings, then
   refuses to go on if any role has privileges the design doesn't give it.
2. ``bootstrap_database``: once per database, as the master user. Schema grants, and new functions closed to
   ``PUBLIC``.
3. ``apply_grants``: after every migration. ``alembic/env.py`` runs it at the end of every upgrade, in the same
   transaction. Moves anything the api owns to the owner, revokes every grant on the owner's relations,
   sequences and functions, then grants exactly what ``mavedb.db.rls`` lists.

Part 3 is the only thing that grants privileges on the schema's objects; migrations may not GRANT or REVOKE
(``bin/check-migrations``). That's what lets a squashed baseline be dumped without privileges: replaying it
and running part 3 gives the same grants as the migrations it replaced.

Roles belong to the whole cluster and creating them needs the master user, so parts 1 and 2 can't be
migrations. Run them with ``python -m mavedb.scripts.bootstrap_roles setup``.
"""

import logging
from collections import defaultdict
from contextlib import contextmanager
from typing import Iterator

from sqlalchemy import Connection, text
from sqlalchemy.exc import DBAPIError

from mavedb.db.rls import (
    API_ROLE,
    FUNCTIONS,
    MIGRATE_ROLE,
    OWNER_ROLE,
    RELATIONS,
    WORKER_ROLE,
    ApiAccess,
    Relation,
)

logger = logging.getLogger(__name__)

# The worker's jobs share one event loop and their database calls are synchronous, so a call waiting on a lock
# stalls every job on the worker, the lock's holder included when it is another job on the same worker. Its jobs
# legitimately run long statements and idle in transactions, so this is its only limit. Change it here and run
# `bootstrap_roles setup` again; a worker picks it up on its next connection.
WORKER_LOCK_TIMEOUT = "60s"

SETUP_HINT = "Run `python -m mavedb.scripts.bootstrap_roles setup` against this database as the master user."

# Role attributes that skip grants or RLS, or hand out privileges, none of which these roles should have.
_UNSAFE_ATTRIBUTES = {
    "rolsuper": "SUPERUSER",
    "rolbypassrls": "BYPASSRLS",
    "rolcreaterole": "CREATEROLE",
    "rolcreatedb": "CREATEDB",
    "rolreplication": "REPLICATION",
}

_OWNER_OID = "(SELECT oid FROM pg_roles WHERE rolname = :owner)"

# Relation kinds that hold rows a role can write: tables, partitioned tables and foreign tables.
_TABLE_KINDS = ("r", "p", "f")


def _roles() -> list[str]:
    return [OWNER_ROLE, MIGRATE_ROLE, WORKER_ROLE, API_ROLE]


def _execute(conn: Connection, statement: str) -> None:
    logger.info(statement)
    conn.execute(text(statement))


@contextmanager
def _privileges_of(conn: Connection, *roles: str) -> Iterator[None]:
    """Give the current user the privileges of ``roles`` for the duration of the block.

    On RDS the master user isn't a superuser, so moving or granting another role's objects needs membership in
    that role. Membership is granted and revoked again in the caller's transaction, so ``MIGRATE_ROLE`` stays
    the only member of ``OWNER_ROLE`` once it commits. PG 15 lets a ``CREATEROLE`` user grant itself any
    non-superuser role; PG 16 requires ``ADMIN OPTION`` on the role instead.
    """
    if conn.execute(text("SELECT rolsuper FROM pg_roles WHERE rolname = current_user")).scalar_one():
        yield
        return

    borrowed = [
        role
        for role in roles
        if not conn.execute(text("SELECT pg_has_role(current_user, :role, 'USAGE')"), {"role": role}).scalar_one()
    ]
    if (
        borrowed
        and not conn.execute(text("SELECT rolcreaterole FROM pg_roles WHERE rolname = current_user")).scalar_one()
    ):
        raise RuntimeError(f"This step needs the privileges of {borrowed}. Run it as the master user.")
    for role in borrowed:
        _execute(conn, f"GRANT {role} TO current_user")
    try:
        yield
    except DBAPIError:
        # The transaction is aborted, so a REVOKE would fail and hide this error. Rolling back undoes the GRANT.
        raise
    except BaseException:
        _give_back(conn, borrowed)
        raise
    _give_back(conn, borrowed)


def _give_back(conn: Connection, borrowed: list[str]) -> None:
    for role in borrowed:
        _execute(conn, f"REVOKE {role} FROM current_user")


def role_problems(conn: Connection) -> list[str]:
    """Return each way the roles differ from what part 1 sets up, or an empty list when they match.

    None of the roles may skip grants or RLS, create roles or databases, or set any superuser-only parameter,
    such as ``session_replication_role``, which turns off foreign key checks; restores set that as the master
    user. The owner can't log in. Their one membership, among themselves or with any other role, is the migrate
    login in the owner, and the migrate login runs as the owner.
    """
    roles = _roles()
    columns = ", ".join(_UNSAFE_ATTRIBUTES)
    found = {
        row.rolname: row
        for row in conn.execute(
            text(f"SELECT rolname, rolcanlogin, {columns} FROM pg_roles WHERE rolname = ANY(:roles)"),
            {"roles": roles},
        )
    }

    problems = []
    for role in roles:
        row = found.get(role)
        if row is None:
            problems.append(f"{role} doesn't exist")
            continue
        problems += [f"{role} has {name}" for column, name in _UNSAFE_ATTRIBUTES.items() if getattr(row, column)]
    if OWNER_ROLE in found and found[OWNER_ROLE].rolcanlogin:
        problems.append(f"{OWNER_ROLE} can log in")

    # Any grant on a parameter, to PUBLIC or to one of the roles, lets them change a superuser-only setting.
    parameters = conn.execute(
        text(
            "SELECT coalesce(r.rolname, 'PUBLIC'), p.parname FROM pg_parameter_acl p "
            "CROSS JOIN LATERAL aclexplode(p.paracl) a LEFT JOIN pg_roles r ON r.oid = a.grantee "
            "WHERE a.grantee = 0 OR r.rolname = ANY(:roles) ORDER BY 1, 2"
        ),
        {"roles": roles},
    )
    problems += [f"{grantee} can set {parameter}" for grantee, parameter in parameters]

    memberships = [
        tuple(row)
        for row in conn.execute(
            text(
                "SELECT r.rolname, m.rolname FROM pg_auth_members am JOIN pg_roles r ON r.oid = am.roleid "
                "JOIN pg_roles m ON m.oid = am.member WHERE r.rolname = ANY(:roles) OR m.rolname = ANY(:roles) "
                "ORDER BY 1, 2"
            ),
            {"roles": roles},
        )
    ]
    problems += [
        f"{member} is a member of {role}"
        for role, member in memberships
        if (role, member) != (OWNER_ROLE, MIGRATE_ROLE)
    ]
    if OWNER_ROLE in found and MIGRATE_ROLE in found:
        if (OWNER_ROLE, MIGRATE_ROLE) not in memberships:
            problems.append(f"{MIGRATE_ROLE} isn't a member of {OWNER_ROLE}")
        if _role_setting(conn, MIGRATE_ROLE, "role") != OWNER_ROLE:
            problems.append(f"{MIGRATE_ROLE} doesn't run as {OWNER_ROLE}")

    return problems


def bootstrap_cluster(conn: Connection) -> None:
    """Part 1: create the four roles, their one membership and their settings. Run once per cluster as the master user.

    Roles that already exist keep their passwords; a new login role has none until an operator sets one. Each
    statement runs only when its effect is missing: roles belong to the whole cluster, and rewriting a role that
    is already right still collides with anyone else altering it at the same moment.

    Raises:
        RuntimeError: If a role that already existed has a privilege the design doesn't give it, such as
            ``BYPASSRLS`` or another role's membership. The bootstrap doesn't strip privileges it didn't grant;
            remove them by hand and run it again.
    """
    existing = set(conn.execute(text("SELECT rolname FROM pg_roles")).scalars())

    for role, login in ((OWNER_ROLE, False), (MIGRATE_ROLE, True), (WORKER_ROLE, True), (API_ROLE, True)):
        if role not in existing:
            _execute(conn, f"CREATE ROLE {role} {'LOGIN' if login else 'NOLOGIN'}")

    if conn.execute(text("SELECT rolcanlogin FROM pg_roles WHERE rolname = :r"), {"r": OWNER_ROLE}).scalar_one():
        _execute(conn, f"ALTER ROLE {OWNER_ROLE} NOLOGIN")

    is_member = conn.execute(
        text("SELECT pg_has_role(:member, :role, 'MEMBER')"), {"member": MIGRATE_ROLE, "role": OWNER_ROLE}
    ).scalar_one()
    if not is_member:
        _execute(conn, f"GRANT {OWNER_ROLE} TO {MIGRATE_ROLE}")

    # A new object belongs to the role that created it, not to a role its creator is a member of. Without
    # this, every table a migration creates would belong to the migrate login, and the api couldn't read it.
    if _role_setting(conn, MIGRATE_ROLE, "role") != OWNER_ROLE:
        _execute(conn, f"ALTER ROLE {MIGRATE_ROLE} SET role = {OWNER_ROLE}")
    if _role_setting(conn, WORKER_ROLE, "lock_timeout") != WORKER_LOCK_TIMEOUT:
        _execute(conn, f"ALTER ROLE {WORKER_ROLE} SET lock_timeout = '{WORKER_LOCK_TIMEOUT}'")

    problems = role_problems(conn)
    if problems:
        raise RuntimeError(
            "These roles have privileges the bootstrap doesn't give them, so it won't trust them: "
            f"{'; '.join(problems)}. Remove them by hand and run it again."
        )


def _role_setting(conn: Connection, role: str, name: str) -> str | None:
    """Return the value ``ALTER ROLE role SET name`` stored for every database, or None if there is none."""
    settings = conn.execute(
        text(
            "SELECT s.setconfig FROM pg_db_role_setting s JOIN pg_roles r ON r.oid = s.setrole "
            "WHERE r.rolname = :role AND s.setdatabase = 0"
        ),
        {"role": role},
    ).scalar_one_or_none()
    for setting in settings or []:
        key, _, value = setting.partition("=")
        if key == name:
            return value

    return None


def schema_privilege_problems(conn: Connection) -> list[str]:
    """Return each way the api or the worker could create objects in this database, or an empty list.

    An object's owner skips its RLS policies and grants, so a role that can create one can make itself a table
    nobody guards.
    """
    problems = []
    for role in (API_ROLE, WORKER_ROLE):
        can_create = conn.execute(
            text(
                "SELECT has_schema_privilege(:role, 'public', 'CREATE'), "
                "has_database_privilege(:role, current_database(), 'CREATE')"
            ),
            {"role": role},
        ).one()
        if can_create[0]:
            problems.append(f"{role} can create objects in schema public")
        if can_create[1]:
            problems.append(f"{role} can create schemas in this database")
    return problems


def bootstrap_database(conn: Connection) -> None:
    """Part 2: schema grants, and new functions closed to ``PUBLIC``. Run once per database as the master user.

    These belong to each database, not the cluster. A database cloned from a template inherits them.

    Raises:
        RuntimeError: If the api or the worker can create objects here through a grant this didn't make.
    """
    with _privileges_of(conn, OWNER_ROLE):
        _execute(conn, "REVOKE CREATE ON SCHEMA public FROM PUBLIC")
        _execute(conn, f"GRANT CREATE, USAGE ON SCHEMA public TO {OWNER_ROLE}")
        _execute(conn, f"GRANT USAGE ON SCHEMA public TO {API_ROLE}, {WORKER_ROLE}")
        # Postgres lets PUBLIC execute every new function. A SECURITY DEFINER function runs as its owner, so
        # until part 3 ran, a new one would lend the owner's privileges to any caller.
        _execute(conn, f"ALTER DEFAULT PRIVILEGES FOR ROLE {OWNER_ROLE} REVOKE EXECUTE ON FUNCTIONS FROM PUBLIC")

    problems = schema_privilege_problems(conn)
    if problems:
        raise RuntimeError(f"{'; '.join(problems)}. Revoke these by hand and run it again.")


def setup(conn: Connection) -> None:
    """Parts 1 and 2, for a new database. Run as the master user, then migrate as ``mavedb_migrate``."""
    bootstrap_cluster(conn)
    bootstrap_database(conn)


def _database_is_set_up(conn: Connection) -> bool:
    """Return whether part 2 has run here, read from the default privileges it leaves on the owner's functions."""
    return conn.execute(
        text(
            "SELECT EXISTS (SELECT FROM pg_default_acl d JOIN pg_roles o ON o.oid = d.defaclrole "
            "WHERE o.rolname = :owner AND d.defaclnamespace = 0 AND d.defaclobjtype = 'f')"
        ),
        {"owner": OWNER_ROLE},
    ).scalar_one()


def apply_grants(conn: Connection) -> None:
    """Part 3: ownership and grants. ``alembic/env.py`` runs this after every upgrade; the test template runs it too.

    Moving ownership away from the api is needed once per database and does nothing afterwards. It needs the
    master user, and it locks each table exclusively, so set ``lock_timeout`` on the connection before calling
    this against a live database.

    Raises:
        RuntimeError: If part 2 hasn't run on this database, if the roles differ from what parts 1 and 2 set up,
            if anything in the schema belongs to a role other than the owner, or if the api role owns a database. ``REASSIGN OWNED`` would move that database too, which for the
            mapper's ``gene_normalizer`` database changes what its login may do. Change each such database's
            owner by hand first.
    """
    if not _database_is_set_up(conn):
        raise RuntimeError(f"This database has no roles set up. {SETUP_HINT}")

    problems = role_problems(conn) + schema_privilege_problems(conn)
    if problems:
        raise RuntimeError(f"The roles aren't as the bootstrap sets them up: {'; '.join(problems)}. {SETUP_HINT}")

    owned_databases = list(
        conn.execute(
            text("SELECT datname FROM pg_database WHERE datdba = (SELECT oid FROM pg_roles WHERE rolname = :role)"),
            {"role": API_ROLE},
        ).scalars()
    )
    if owned_databases:
        raise RuntimeError(f"{API_ROLE} owns databases {owned_databases}; change their owner before running this.")

    api_owns_objects = conn.execute(
        text(
            "SELECT EXISTS (SELECT FROM pg_shdepend d JOIN pg_database db ON db.oid = d.dbid "
            "WHERE db.datname = current_database() AND d.deptype = 'o' "
            "AND d.refobjid = (SELECT oid FROM pg_roles WHERE rolname = :role))"
        ),
        {"role": API_ROLE},
    ).scalar_one()

    with _privileges_of(conn, OWNER_ROLE, *([API_ROLE] if api_owns_objects else [])):
        if api_owns_objects:
            _execute(conn, f"REASSIGN OWNED BY {API_ROLE} TO {OWNER_ROLE}")

        # The grants below cover only the owner's objects. Anything else in the schema keeps whatever its owner
        # gives it, and its owner skips its RLS policies.
        strays = _objects_not_owned_by_the_owner(conn)
        if strays:
            raise RuntimeError(
                f"These belong to a role other than {OWNER_ROLE}: {'; '.join(strays)}. Change their owner by hand, "
                "or drop them, and run this again."
            )

        _grant_relations(conn)
        _grant_functions(conn)


def _objects_not_owned_by_the_owner(conn: Connection) -> list[str]:
    """Return the relations and functions in ``public`` that the owner doesn't own, skipping extensions' own."""
    return [
        f"{name} (owned by {owner})"
        for name, owner in conn.execute(
            text(
                "SELECT c.oid::regclass::text, c.relowner::regrole::text FROM pg_class c "
                "JOIN pg_namespace n ON n.oid = c.relnamespace "
                f"WHERE n.nspname = 'public' AND c.relkind IN ('r', 'p', 'f', 'v', 'm', 'S') AND c.relowner <> {_OWNER_OID} "
                "AND NOT EXISTS (SELECT FROM pg_depend d WHERE d.classid = 'pg_class'::regclass "
                "AND d.objid = c.oid AND d.deptype = 'e') "
                "UNION ALL "
                "SELECT p.oid::regprocedure::text, p.proowner::regrole::text FROM pg_proc p "
                "JOIN pg_namespace n ON n.oid = p.pronamespace "
                f"WHERE n.nspname = 'public' AND p.proowner <> {_OWNER_OID} "
                "AND NOT EXISTS (SELECT FROM pg_depend d WHERE d.classid = 'pg_proc'::regclass "
                "AND d.objid = p.oid AND d.deptype = 'e') "
                "ORDER BY 1"
            ),
            {"owner": OWNER_ROLE},
        )
    ]


def _grantees(relation: Relation | None, kind: str = "r", security_invoker: bool = False) -> tuple[str, ...]:
    """Return the roles a relation of ``kind`` is granted to, given its entry.

    A guarded view reads its tables as its owner unless it's ``security_invoker``, and a materialized view's rows
    were read as the owner, so in either case the api would see rows its policies hide. It isn't granted them.
    """
    if relation is None:
        return ()
    api = relation.api is ApiAccess.CARVE_OUT or (
        relation.api is ApiAccess.GUARDED and (kind in _TABLE_KINDS or (kind == "v" and security_invoker))
    )
    return tuple(role for role, granted in ((API_ROLE, api), (WORKER_ROLE, relation.worker)) if granted)


def _revoke_all(conn: Connection, kind: str, objects: list[str]) -> None:
    if objects:
        _execute(conn, f"REVOKE ALL ON {kind} {', '.join(objects)} FROM PUBLIC, {API_ROLE}, {WORKER_ROLE}")


def _grant_relations(conn: Connection) -> None:
    """Grant each of the owner's relations and sequences to exactly the roles ``RELATIONS`` lists, and no others.

    A relation missing from ``RELATIONS`` gets nothing, and the api gets no guarded view that isn't
    ``security_invoker`` and no guarded materialized view. A sequence goes to the roles that can write the table
    that owns it, and nowhere if no table does.
    """
    relations = conn.execute(
        text(
            "SELECT c.relname, c.oid::regclass::text, c.relkind, coalesce((SELECT o.option_value::boolean "
            "FROM pg_options_to_table(c.reloptions) o WHERE o.option_name = 'security_invoker'), false) "
            "FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace "
            f"WHERE n.nspname = 'public' AND c.relkind IN ('r', 'p', 'f', 'v', 'm') AND c.relowner = {_OWNER_OID} "
            "ORDER BY 1"
        ),
        {"owner": OWNER_ROLE},
    ).all()
    sequences = conn.execute(
        text(
            "SELECT s.oid::regclass::text, t.relname FROM pg_class s "
            "JOIN pg_namespace n ON n.oid = s.relnamespace "
            "LEFT JOIN pg_depend d ON d.classid = 'pg_class'::regclass AND d.objid = s.oid "
            "AND d.refclassid = 'pg_class'::regclass AND d.deptype IN ('a', 'i') "
            "LEFT JOIN pg_class t ON t.oid = d.refobjid "
            f"WHERE n.nspname = 'public' AND s.relkind = 'S' AND s.relowner = {_OWNER_OID} ORDER BY 1"
        ),
        {"owner": OWNER_ROLE},
    ).all()

    # Grouped by what's granted to whom, so each group is one statement.
    grants: dict[tuple[str, str, tuple[str, ...]], list[str]] = defaultdict(list)
    for name, relation, kind, security_invoker in relations:
        privileges = "SELECT, INSERT, UPDATE, DELETE" if kind in _TABLE_KINDS else "SELECT"
        entry = RELATIONS.get(name)
        if entry and entry.api is ApiAccess.GUARDED and API_ROLE not in _grantees(entry, kind, security_invoker):
            logger.warning(f"Not granting {name} to {API_ROLE}: it's guarded, but its rows skip RLS.")
        if grantees := _grantees(entry, kind, security_invoker):
            grants[("TABLE", privileges, grantees)].append(relation)
    for sequence, table in sequences:
        if grantees := _grantees(RELATIONS.get(table)):
            grants[("SEQUENCE", "USAGE, SELECT", grantees)].append(sequence)

    _revoke_all(conn, "TABLE", [relation for _, relation, _, _ in relations])
    _revoke_all(conn, "SEQUENCE", [sequence for sequence, _ in sequences])
    for (kind, privileges, grantees), objects in sorted(grants.items()):
        _execute(conn, f"GRANT {privileges} ON {kind} {', '.join(objects)} TO {', '.join(grantees)}")


def _grant_functions(conn: Connection) -> None:
    """Let exactly the roles ``FUNCTIONS`` lists execute each of the owner's functions, and no others.

    A function missing from ``FUNCTIONS`` gets nothing, and so does one whose ``SECURITY DEFINER`` flag differs
    from its entry, since it isn't the function that was reviewed. So does a ``SECURITY DEFINER`` function that
    doesn't pin its ``search_path``: a caller could put objects of their own ahead of the ones it means, and it
    would run them as the owner. Extensions' functions belong to the role that created the extension, so
    they're left alone.
    """
    functions = conn.execute(
        text(
            "SELECT p.proname, p.oid::regprocedure::text, p.prosecdef, p.proconfig FROM pg_proc p "
            "JOIN pg_namespace n ON n.oid = p.pronamespace "
            f"WHERE n.nspname = 'public' AND p.proowner = {_OWNER_OID} ORDER BY 2"
        ),
        {"owner": OWNER_ROLE},
    ).all()

    _revoke_all(conn, "ROUTINE", [signature for _, signature, _, _ in functions])
    for name, signature, security_definer, config in functions:
        function = FUNCTIONS.get(name)
        if not function or not function.executors or function.security_definer != security_definer:
            continue
        if security_definer and not any(setting.startswith("search_path=") for setting in config or []):
            logger.warning(f"Not granting {signature}: it's SECURITY DEFINER without a pinned search_path.")
            continue
        _execute(conn, f"GRANT EXECUTE ON ROUTINE {signature} TO {', '.join(sorted(function.executors))}")
