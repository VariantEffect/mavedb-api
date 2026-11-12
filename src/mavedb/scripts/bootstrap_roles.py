"""Set up the database roles, or reapply their grants (#827). Dry run unless ``--commit``.

Connects with the ``DB_*`` environment variables.

- ``setup``: once for each new database, as the master user. Creates the roles if the cluster lacks them,
  checks the ones it finds, then sets up this database for them.
- ``grants``: reapplies ownership and grants. Every ``alembic upgrade`` already does this, so it's needed by
  hand only the first time, as the master user, to move the tables the api's login owns.

None of it sets a password. A new environment runs::

    DB_USERNAME=<master user> ... python -m mavedb.scripts.bootstrap_roles setup --commit
    DB_USERNAME=mavedb_migrate ... alembic upgrade head
"""

import logging

import asyncclick as click
from sqlalchemy import text
from sqlalchemy.orm import Session

from mavedb.db.bootstrap import apply_grants, setup
from mavedb.scripts.environment import script_environment, with_database_session

logger = logging.getLogger(__name__)


@script_environment.command()
@with_database_session
@click.argument("part", type=click.Choice(["setup", "grants"]))
@click.option(
    "--lock-timeout",
    default="5s",
    show_default=True,
    help="How long to wait for each table's lock. Moving ownership locks every table; on a timeout, re-run.",
)
def bootstrap_roles(db: Session, part: str, lock_timeout: str) -> None:
    conn = db.connection()
    conn.execute(text("SELECT set_config('lock_timeout', :timeout, true)"), {"timeout": lock_timeout})
    logger.info(f"Running {part!r} as {conn.execute(text('SELECT current_user')).scalar_one()}")

    if part == "setup":
        setup(conn)
    else:
        apply_grants(conn)


if __name__ == "__main__":
    bootstrap_roles()
