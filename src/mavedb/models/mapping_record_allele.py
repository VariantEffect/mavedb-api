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
    # Provenance: ``True`` when this allele was the assay's actual measurement
    # for this mapping record (sourced from vrs_map, with real alignment QC),
    # ``False`` when it was derived by the cross-level or same-level
    # synonymous translator. The flag lives on the link rather than on
    # ``Allele`` because the same VRS allele can be authoritative for one
    # mapping record and translator-derived for another.
    is_authoritative = Column(Boolean, nullable=False, default=False)

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
        # At most one live link per (mapping_record, allele). The job retires (closes valid_to)
        # rather than deletes, so superseded links remain for point-in-time queries; only the
        # live row participates in this constraint. A derived link is never written when an
        # authoritative one already exists for the pair, so is_authoritative is intentionally
        # absent from the key — a record links a given allele once.
        Index(
            "uq_mapping_record_alleles_live",
            "mapping_record_id",
            "allele_id",
            unique=True,
            postgresql_where=text("valid_to IS NULL"),
        ),
    )
