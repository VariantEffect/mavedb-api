"""
Environment setup for scripts.
"""

import enum
import logging
import click
from functools import wraps


from sqlalchemy.orm import configure_mappers

from mavedb import deps
from mavedb.models import *  # noqa: F403


logger = logging.getLogger(__name__)


@enum.unique
class DatabaseSessionAction(enum.Enum):
    """
    Enum representing the database session transaction action selected for a
    command decorated by :py:func:`.with_database_session`.

    You will not need to use this class unless you provide ``pass_action =
    True`` to :py:func:`.with_database_session`.
    """

    DRY_RUN = "rollback"
    PROMPT = "prompt"
    COMMIT = "commit"


@click.group()
def script_environment():
    """
    Set up the environment for a script that may be run from the command line and does not necessarily depend on the
    FastAPI framework.

    Features:
    - Configures logging for the script.
    - Loads the SQLAlchemy data model.
    """

    logging.basicConfig()

    # Un-comment this line to log all database queries:
    logging.getLogger("__main__").setLevel(logging.INFO)
    # logging.getLogger("sqlalchemy.engine").setLevel(logging.INFO)

    # Scan all our model classes and create backref attributes. Otherwise, these attributes only get added to classes once
    # an instance of the related class has been created.
    configure_mappers()


def with_database_session(command=None, *, pass_action: bool = False):
    """
    Decorator to provide database session and error handling for a *command*.

    The *command* callable must be a :py:class:`click.Command` instance.

    The decorated *command* is called with a ``db`` keyword argument to provide
    a :class:`~id3c.db.session.DatabaseSession` object.  The call happens
    within an exception handler that commits or rollsback the database
    transaction, possibly interactively.  Three new options are added to the
    *command* (``--dry-run``, ``--prompt``, and ``--commit``) to control this
    behaviour.

    >>> @click.command
    ... @with_database_session
    ... def cmd(db: DatabaseSession):
    ...     pass

    If the optional, keyword-only argument *pass_action* is ``True``, then the
    :py:class:`.DatabaseSessionAction` selected by the CLI options above is
    passed as an additional ``action`` argument to the decorated *command*.

    >>> @click.command
    ... @with_database_session(pass_action = True)
    ... def cmd(db: DatabaseSession, action: DatabaseSessionAction):
    ...     pass

    One example where this is useful is when the *command* accesses
    non-database resources and wants to extend dry run mode to them as well.
    """

    def decorator(command):
        @click.option(
            "--dry-run",
            "action",
            help="Only go through the motions of changing the database (default)",
            flag_value=DatabaseSessionAction("rollback"),
            type=DatabaseSessionAction,
            default=True,
        )
        @click.option(
            "--prompt",
            "action",
            help="Ask if changes to the database should be saved",
            flag_value=DatabaseSessionAction("prompt"),
            type=DatabaseSessionAction,
        )
        @click.option(
            "--commit",
            "action",
            help="Save changes to the database",
            flag_value=DatabaseSessionAction("commit"),
            type=DatabaseSessionAction,
        )
        @wraps(command)
        def decorated(*args, action, **kwargs):
            db = next(deps.get_db())

            kwargs["db"] = db

            if pass_action:
                kwargs["action"] = action

            processed_without_error = None

            try:
                command(*args, **kwargs)

            except Exception as error:
                processed_without_error = False

                logger.error(f"Aborting with error: {error}")
                raise error from None

            else:
                processed_without_error = True

            finally:
                if action is DatabaseSessionAction.PROMPT:
                    ask_to_commit = (
                        "Commit all changes?"
                        if processed_without_error
                        else "Commit successfully processed records up to this point?"
                    )

                    commit = click.confirm(ask_to_commit)
                else:
                    commit = action is DatabaseSessionAction.COMMIT

                if commit:
                    logger.info(
                        "Committing all changes"
                        if processed_without_error
                        else "Committing successfully processed records up to this point"
                    )
                    db.commit()

                else:
                    logger.info("Rolling back all changes; the database will not be modified")
                    db.rollback()

        return decorated

    return decorator(command) if command else decorator
