from datetime import date
from typing import TYPE_CHECKING, Any, Optional

from sqlalchemy import Boolean, Column, Date, Enum, ForeignKey, Index, Integer, String, func, text
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy.orm import Mapped, relationship

from mavedb.db.base import Base
from mavedb.db.mixins import ValidTime
from mavedb.lib.hgvs import extract_accession
from mavedb.models.enums.annotation_layer import AnnotationLayer

if TYPE_CHECKING:
    from .mapping_record_allele import MappingRecordAllele
    from .target_gene_mapping import TargetGeneMapping
    from .variant import Variant


class MappingRecord(ValidTime, Base):
    __tablename__ = "mapping_records"

    id: Mapped[int] = Column(Integer, primary_key=True)

    variant_id = Column(Integer, ForeignKey("variants.id"), nullable=False)
    variant: Mapped["Variant"] = relationship("Variant", back_populates="mapping_records")

    # Digest of the pre-mapped (assayed-level) VRS representation, indexed for
    # cross-score-set dedup lookups.
    vrs_digest = Column(String, nullable=True)
    pre_mapped: Optional[Any] = Column(JSONB(none_as_null=True), nullable=True)

    # Level at which the variant was *assayed* — distinct from alignment_level (QC).
    assay_level = Column(String(length=16), nullable=False)
    hgvs_assay_level = Column(String, nullable=True)

    @hybrid_property
    def transcript(self) -> str:
        """Reference accession of the assay-level HGVS (derived, not stored).

        Derived rather than stored so it cannot drift from hgvs_assay_level, which already
        carries the accession.
        """
        return extract_accession(self.hgvs_assay_level or "")

    @transcript.inplace.expression
    @classmethod
    def _transcript_expression(cls):
        return func.split_part(cls.hgvs_assay_level, ":", 1)

    mapping_api_version = Column(String, nullable=False)
    # Domain data: the date the mapping was performed (from the dcd-mapping run), surfaced to
    # users — distinct from valid_from, which is when this version became the live record.
    mapped_date = Column(Date, nullable=False, default=date.today)
    vrs_version = Column(String, nullable=True)

    # valid_from / valid_to and the derived `current` come from ValidTime: a re-map retires the
    # prior live record (closes its valid_to) and inserts a new live one, so mapping history is
    # retained and point-in-time queries are a single predicate.

    # Per-mapping QC fields from dcd-mapping.
    alignment_level = Column(
        Enum(AnnotationLayer, create_constraint=True, length=16, native_enum=False, validate_strings=True),
        nullable=True,
    )
    at_mismatched_locus = Column(Boolean, nullable=True)
    near_gap = Column(Boolean, nullable=True)

    target_gene_mapping_id = Column(Integer, ForeignKey("target_gene_mappings.id"), index=True, nullable=True)
    target_gene_mapping: Mapped[Optional["TargetGeneMapping"]] = relationship(
        "TargetGeneMapping", back_populates="mapping_records"
    )

    allele_links: Mapped[list["MappingRecordAllele"]] = relationship(
        "MappingRecordAllele",
        back_populates="mapping_record",
        cascade="all, delete-orphan",
    )

    # Retiring a record retires its live allele links too (both the authoritative link and any
    # derived links a reverse-translation run attached). See ValidTime.__retire_cascade__.
    __retire_cascade__ = ("allele_links",)

    __table_args__ = (
        Index("ix_mapping_records_vrs_digest", "vrs_digest"),
        Index("ix_mapping_records_variant_id", "variant_id"),
        # At most one live mapping record per variant — promotes to the database the invariant the
        # mapping job enforces in app code (it retires the prior live record before inserting a new
        # one). Superseded versions remain with a closed valid_to for point-in-time queries.
        Index(
            "uq_mapping_records_current",
            "variant_id",
            unique=True,
            postgresql_where=text("valid_to IS NULL"),
        ),
    )
