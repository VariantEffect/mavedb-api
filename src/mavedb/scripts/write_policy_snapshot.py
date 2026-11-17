"""Write ``src/mavedb/db/policies.sql`` from a database at alembic head (#809).

Connects with the ``DB_*`` environment variables and only reads. Run it after a migration that changes a policy,
against a database upgraded to head, and commit the result::

    poetry run alembic upgrade head
    poetry run python -m mavedb.scripts.write_policy_snapshot
"""

import logging

from sqlalchemy.orm import Session

from mavedb.db.policy_snapshot import SNAPSHOT_PATH, render_policy_snapshot
from mavedb.scripts.environment import script_environment, with_database_session

logger = logging.getLogger(__name__)


@script_environment.command()
@with_database_session
def write_policy_snapshot(db: Session) -> None:
    SNAPSHOT_PATH.write_text(render_policy_snapshot(db.connection()))
    logger.info(f"Wrote {SNAPSHOT_PATH}")


if __name__ == "__main__":
    write_policy_snapshot()
