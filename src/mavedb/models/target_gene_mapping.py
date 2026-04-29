"""SQLAlchemy model for per-(target gene, alignment level) mapping QC and provenance.

A ``TargetGeneMapping`` row records the QC and provenance for one alignment of one
target gene at one annotation layer (protein / cDNA / genomic).  The fields mirror
the ``TargetMapping`` model produced by the dcd-mapping QC API so the worker can
deserialize directly with minimal transformation.

``tool_parameters`` and ``alignment_metadata`` are aligner-specific JSONB blobs;
see the dcd-mapping ``TargetMapping`` schema for their shape.
"""

from __future__ import annotations

from datetime import date
from typing import TYPE_CHECKING

from sqlalchemy import Boolean, Column, Date, Enum, Float, ForeignKey, Index, Integer, String
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, relationship

from mavedb.db.base import Base
from mavedb.models.enums.annotation_layer import AnnotationLayer

if TYPE_CHECKING:
    from mavedb.models.mapped_variant import MappedVariant
    from mavedb.models.target_gene import TargetGene


class TargetGeneMapping(Base):
    __tablename__ = "target_gene_mappings"

    id = Column(Integer, primary_key=True)

    target_gene_id = Column(Integer, ForeignKey("target_genes.id"), nullable=False, index=True)
    target_gene: Mapped["TargetGene"] = relationship("TargetGene", back_populates="target_gene_mappings")

    alignment_level = Column(
        Enum(AnnotationLayer, create_constraint=True, length=16, native_enum=False, validate_strings=True),
        nullable=False,
    )
    preferred = Column(Boolean, nullable=False, default=False, server_default="false")

    # Coordinate frame of the mapping result; null for alignments with no genomic frame
    # (e.g. protein-vs-protein).
    reference_assembly = Column(String, nullable=True)
    reference_accession = Column(String, nullable=True)
    reference_sequence_id = Column(String, nullable=True)

    alignment_score = Column(Float, nullable=True)
    next_best_alignment_score = Column(Float, nullable=True)
    alignment_length = Column(Integer, nullable=True)
    alignment_string = Column(String, nullable=True)
    mismatch_count = Column(Integer, nullable=True)
    gap_count = Column(Integer, nullable=True)
    percent_identity = Column(Float, nullable=True)

    total_variants = Column(Integer, nullable=True)
    variants_failed = Column(Integer, nullable=True)
    variants_with_alignment_warnings = Column(Integer, nullable=True)
    variants_mapped_cleanly = Column(Integer, nullable=True)

    tool_name = Column(String, nullable=False, default="dcd-mapping", server_default="dcd-mapping")
    tool_version = Column(String, nullable=False)
    # Aligner-specific configuration; shape varies based on mapping method.
    tool_parameters = Column(JSONB(none_as_null=True), nullable=True)
    # Aligner-specific structured details (e.g. CIGAR, per-base mismatch records).
    alignment_metadata = Column(JSONB(none_as_null=True), nullable=True)

    vrs_version = Column(String, nullable=True)

    # When dcd-mapping produced this mapping. Distinct from ``creation_date`` /
    # ``modification_date``, which track this row's lifecycle in MaveDB.
    mapped_date = Column(Date, nullable=True)

    creation_date = Column(Date, nullable=False, default=date.today)
    modification_date = Column(Date, nullable=False, default=date.today, onupdate=date.today)

    mapped_variants: Mapped[list["MappedVariant"]] = relationship(
        "MappedVariant",
        back_populates="target_gene_mapping",
    )

    __table_args__ = (Index("ix_target_gene_mappings_target_alignment", "target_gene_id", "alignment_level"),)
