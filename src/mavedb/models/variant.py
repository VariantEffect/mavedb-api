from datetime import date
from typing import TYPE_CHECKING, List, Optional

from sqlalchemy import Column, ColumnElement, Date, ForeignKey, Index, Integer, String, cast, func
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy.orm import Mapped, relationship

from mavedb.db.base import Base

if TYPE_CHECKING:
    from .mapped_variant import MappedVariant
    from .mapping_record import MappingRecord
    from .score_set import ScoreSet


class Variant(Base):
    __tablename__ = "variants"

    id: Mapped[int] = Column(Integer, primary_key=True)

    urn = Column(String(64), index=True, nullable=True, unique=True)
    data = Column(JSONB, nullable=False)

    score_set_id: Mapped[int] = Column("scoreset_id", Integer, ForeignKey("scoresets.id"), index=True, nullable=False)
    # TODO examine if delete-orphan is necessary, explore cascade
    score_set: Mapped["ScoreSet"] = relationship(back_populates="variants")

    hgvs_nt = Column(String, nullable=True)
    hgvs_pro = Column(String, nullable=True)
    hgvs_splice = Column(String, nullable=True)

    creation_date = Column(Date, nullable=False, default=date.today)
    modification_date = Column(Date, nullable=False, default=date.today, onupdate=date.today)

    mapped_variants: Mapped[List["MappedVariant"]] = relationship(
        back_populates="variant", cascade="all, delete-orphan"
    )

    mapping_records: Mapped[List["MappingRecord"]] = relationship(
        back_populates="variant", cascade="all, delete-orphan"
    )

    # Bidirectional relationship with ScoreCalibrationFunctionalClassification is left
    # purposefully undefined for performance reasons.

    __table_args__ = (
        # Target of mapping_records' composite (variant_id, score_set_id) foreign key. The records
        # carry their score set so an RLS policy can check it without joining through variants (#833).
        Index("uq_variants_id_scoreset", id, score_set_id, unique=True),
    )

    @hybrid_property
    def variant_number(self) -> Optional[int]:
        """The integer after '#' in the URN, which orders a score set's variants."""
        return int(self.urn.split("#")[1]) if self.urn else None

    @variant_number.inplace.expression
    @classmethod
    def _variant_number_expression(cls) -> ColumnElement[int]:
        # Order by this expression rather than restating it: the index below serves only an identical one.
        return cast(func.split_part(cls.urn, "#", 2), Integer)


# Serves ordering a score set's variants by number, so deep pages of a large score set skip the full sort.
Index("ix_variants_scoreset_number", Variant.score_set_id, Variant.variant_number, Variant.id)
