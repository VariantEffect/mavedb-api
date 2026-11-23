"""Writing versioned rows in test setup the way production does."""

from datetime import datetime
from typing import Any, Iterable, Optional

from sqlalchemy.orm import Session

from mavedb.models.mixins.valid_time import _BLOCK_AT, ValidTime, superseding


def add_versioned(session: Session, *rows: Any) -> None:
    """Add ``rows`` and commit, inside a superseding block for the versioned tables among them."""
    models = {type(row) for row in rows if isinstance(row, ValidTime)}
    if not models:
        session.add_all(rows)
        session.commit()
        return

    with superseding(session, *models):
        session.add_all(rows)


def retire_versioned(session: Session, *rows: ValidTime, at: Optional[datetime] = None) -> None:
    """Retire ``rows`` and their ``__retire_cascade__`` children through ``supersede``, and commit.

    ``at`` backdates the block's time, so a history test can place the retirement at a fixed instant.
    """
    models = {type(row) for row in rows}
    with superseding(session, *models):
        if at is not None:
            session.info[_BLOCK_AT] = at
        for model in models:
            model.supersede(session, retiring=[row for row in rows if type(row) is model])


def update_versioned(session: Session, rows: Iterable[ValidTime], **values: Any) -> None:
    """Set ``values`` on versioned ``rows`` and commit, without cascading: for building states production never
    writes, like a live link on a retired record."""
    rows = list(rows)
    with superseding(session, *{type(row) for row in rows}):
        for row in rows:
            for name, value in values.items():
                setattr(row, name, value)
