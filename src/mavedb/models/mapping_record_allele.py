from typing import TYPE_CHECKING

from sqlalchemy import Boolean, Column, ForeignKey, Index, Integer, text
from sqlalchemy.orm import Mapped, relationship

from mavedb.db.base import Base
from mavedb.db.mixins import ValidTime

if TYPE_CHECKING:
    from .allele import Allele
    from .mapping_record import MappingRecord


class MappingRecordAllele(ValidTime, Base):
    __tablename__ = "mapping_record_alleles"

    id: Mapped[int] = Column(Integer, primary_key=True)
    mapping_record_id: Mapped[int] = Column(
        Integer,
        ForeignKey("mapping_records.id", ondelete="CASCADE"),
        nullable=False,
    )
    allele_id: Mapped[int] = Column(
        Integer,
        ForeignKey("alleles.id", ondelete="RESTRICT"),
        nullable=False,
    )
    # True for the assay's actual measured allele (from vrs_map); False for translator-derived
    # alleles. Lives on the link, not Allele, since the same allele can be authoritative for one
    # mapping record and derived for another.
    is_authoritative: Mapped[bool] = Column(Boolean, nullable=False, default=False)
    # Pairs a link with its coding/genomic counterpart within one mapping record (set by the
    # reverse-translation job). NULL for unpaired links. Local to this record, not a cross-record
    # identity — see the RT job for group-assignment logic.
    projection_group = Column(Integer, nullable=True)

    mapping_record: Mapped["MappingRecord"] = relationship("MappingRecord", back_populates="allele_links")
    allele: Mapped["Allele"] = relationship("Allele", back_populates="mapping_record_links")

    __table_args__ = (
        Index(
            "ix_mapping_record_alleles_mapping_record_id",
            "mapping_record_id",
        ),
        Index(
            "ix_mapping_record_alleles_allele_id",
            "allele_id",
        ),
        # At most one live link per (mapping_record, allele); retired links (valid_to set) are
        # exempt so history is preserved.
        Index(
            "uq_mapping_record_alleles_live",
            "mapping_record_id",
            "allele_id",
            unique=True,
            postgresql_where=text("valid_to IS NULL"),
        ),
        # At most one live authoritative link per mapping record. lib/score_set_variants relies on
        # this 1:1 invariant (inner join on the authoritative link); a second one would silently
        # duplicate the variant and double-count its score.
        Index(
            "uq_mapping_record_alleles_live_authoritative",
            "mapping_record_id",
            unique=True,
            postgresql_where=text("is_authoritative AND valid_to IS NULL"),
        ),
    )
