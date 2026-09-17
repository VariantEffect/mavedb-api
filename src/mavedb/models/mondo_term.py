"""SQLAlchemy model for MONDO disease terms.

A ``MondoTerm`` is a single controlled-vocabulary entry (a MONDO code + its label) referenced by
calibrations as their disease/disorder context. The columns are deliberately the generic coding
columns (``system``/``code``/``system_version``/``label``) rather than MONDO-specific ones: this table
is the narrow, first consumer of a shared concept model, and matching the column shape of
``controlled_keywords`` (and the eventual ``mappable_concepts`` table) keeps a later promotion additive
rather than a reshape. On the wire a term is served as a GA4GH ``MappableConcept``.
"""

from datetime import date
from typing import Optional

from sqlalchemy import Date, Integer, String, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column

from mavedb.db.base import Base


class MondoTerm(Base):
    __tablename__ = "mondo_terms"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)

    # The coding: ``code`` is the MONDO CURIE (e.g. "MONDO:0015263"), ``system`` its terminology
    # identifier. ``system`` is constant for MONDO today but stored explicitly so a promotion to a
    # multi-system concept table needs no backfill. ``UNIQUE(system, code)`` dedupes shared terms.
    code: Mapped[str] = mapped_column(String, nullable=False)
    system: Mapped[str] = mapped_column(String, nullable=False)
    system_version: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    label: Mapped[str] = mapped_column(String, nullable=False)

    creation_date: Mapped[date] = mapped_column(Date, nullable=False, default=date.today)
    modification_date: Mapped[date] = mapped_column(Date, nullable=False, default=date.today, onupdate=date.today)

    __table_args__ = (UniqueConstraint("system", "code", name="uq_mondo_terms_system_code"),)

    def __repr__(self) -> str:  # pragma: no cover - repr utility
        return f"<MondoTerm id={self.id} code={self.code!r} label={self.label!r}>"
