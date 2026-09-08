"""
SQLAlchemy model for URNs that publication has retired.
"""

from datetime import datetime

from sqlalchemy import DateTime, Integer, String, func
from sqlalchemy.orm import Mapped, mapped_column

from mavedb.db.base import Base


class UrnRedirect(Base):
    """
    Records that a record's URN was replaced, and by what.

    Publishing a dataset overwrites the ``tmp:<uuid>`` URN it was created with, so every link already
    shared to the unpublished record stops resolving. One row is written here per URN publication
    retires, and requests naming a retired URN are forwarded to its replacement.

    Only experiment sets, experiments and score sets get rows. A variant's URN is
    ``{score_set_urn}#{n}``, so forwarding replaces the retired URN wherever it appears in a request
    path and a variant follows its score set without a row of its own.
    """

    __tablename__ = "urn_redirects"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)

    old_urn: Mapped[str] = mapped_column(String(64), nullable=False, unique=True, index=True)
    new_urn: Mapped[str] = mapped_column(String(64), nullable=False)

    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())

    def __repr__(self) -> str:
        return f"<UrnRedirect(old_urn='{self.old_urn}', new_urn='{self.new_urn}')>"
