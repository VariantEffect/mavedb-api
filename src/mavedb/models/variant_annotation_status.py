"""
SQLAlchemy models for variant annotation status.
"""

from datetime import datetime
from typing import TYPE_CHECKING, Any, Dict, Optional

from sqlalchemy import CheckConstraint, DateTime, ForeignKey, Index, Integer, String, Text, func
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column, relationship

from mavedb.db.base import Base
from mavedb.models.enums.job_pipeline import AnnotationStatus

if TYPE_CHECKING:
    from mavedb.models.job_run import JobRun
    from mavedb.models.variant import Variant


class VariantAnnotationStatus(Base):
    """
    Tracks annotation status for individual variants.

    Allows us to see which variants failed annotation and why.
    """

    __tablename__ = "variant_annotation_status"

    # Primary key
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)

    # Composite primary key
    variant_id: Mapped[int] = mapped_column(Integer, ForeignKey("variants.id", ondelete="CASCADE"), primary_key=True)
    annotation_type: Mapped[str] = mapped_column(
        String(50), primary_key=True, comment="Type of annotation: vrs, clinvar, gnomad, etc."
    )

    # Source version
    version: Mapped[Optional[str]] = mapped_column(
        String(50), nullable=True, comment="Version of the annotation source used (if applicable)"
    )

    # Status tracking
    status: Mapped[AnnotationStatus] = mapped_column(String(50), nullable=False, comment="success, failed, skipped")

    # Error information
    error_message: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    failure_category: Mapped[Optional[str]] = mapped_column(String(100), nullable=True)

    # Success data (flexible JSONB for annotation results)
    success_data: Mapped[Optional[Dict[str, Any]]] = mapped_column(
        JSONB, nullable=True, comment="Annotation results when successful"
    )

    # Current flag
    current: Mapped[bool] = mapped_column(
        nullable=False,
        server_default="true",
        comment="Whether this is the current status for the variant and annotation type",
    )

    # Job tracking
    job_run_id: Mapped[Optional[str]] = mapped_column(
        String(255), ForeignKey("job_runs.id", ondelete="SET NULL"), nullable=True
    )

    # Timestamps
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now(), onupdate=func.now()
    )

    # Relationships
    variant: Mapped["Variant"] = relationship("Variant")
    job_run: Mapped[Optional["JobRun"]] = relationship("JobRun")

    # Indexes
    __table_args__ = (
        Index("ix_variant_annotation_status_variant_id", "variant_id"),
        Index("ix_variant_annotation_status_annotation_type", "annotation_type"),
        Index("ix_variant_annotation_status_status", "status"),
        Index("ix_variant_annotation_status_job_run_id", "job_run_id"),
        Index("ix_variant_annotation_status_created_at", "created_at"),
        # Composite index for common queries
        Index("ix_variant_annotation_type_status", "annotation_type", "status"),
        Index("ix_variant_annotation_status_current", "current"),
        Index("ix_variant_annotation_status_version", "version"),
        Index(
            "ix_variant_annotation_status_variant_type_version_current",
            "variant_id",
            "annotation_type",
            "version",
            "current",
        ),
        CheckConstraint(
            "annotation_type IN ('vrs_mapping', 'clingen_allele_id', 'mapped_hgvs', 'variant_translation', 'gnomad_allele_frequency', 'clinvar_control', 'vep_functional_consequence', 'ldh_submission')",
            name="ck_variant_annotation_type_valid",
        ),
        CheckConstraint(
            "status IN ('success', 'failed', 'skipped')",
            name="ck_variant_annotation_status_valid",
        ),
        ## Although un-enforced at the DB level, we should ensure only one 'current' record per (variant_id, annotation_type, version)
    )

    def __repr__(self) -> str:
        return f"<VariantAnnotationStatus(variant_id={self.variant_id}, type='{self.annotation_type}', status='{self.status}')>"
