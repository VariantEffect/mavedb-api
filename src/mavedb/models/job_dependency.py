"""
SQLAlchemy models for job dependencies.
"""

from datetime import datetime
from typing import TYPE_CHECKING, Any, Dict, Optional

from sqlalchemy import CheckConstraint, DateTime, ForeignKey, Index, String, func
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column, relationship

from mavedb.db.base import Base
from mavedb.models.enums import DependencyType

if TYPE_CHECKING:
    from mavedb.models.job_run import JobRun
    from mavedb.models.pipeline import Pipeline


class JobDependency(Base):
    """
    Defines dependencies between jobs within a pipeline.

    This table maps jobs to their pipeline and defines execution order.
    """

    __tablename__ = "job_dependencies"

    # The job being defined (references job_runs.id)
    id: Mapped[str] = mapped_column(String(255), ForeignKey("job_runs.id", ondelete="CASCADE"), primary_key=True)

    # Pipeline this job belongs to
    pipeline_id: Mapped[str] = mapped_column(
        String(255), ForeignKey("pipelines.id", ondelete="CASCADE"), nullable=False
    )

    # Job this depends on (nullable for jobs with no dependencies)
    depends_on_job_id: Mapped[Optional[str]] = mapped_column(
        String(255), ForeignKey("job_runs.id", ondelete="CASCADE"), nullable=True
    )

    # Type of dependency
    dependency_type: Mapped[Optional[DependencyType]] = mapped_column(String(50), nullable=True)

    # Timestamps
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())

    # Flexible metadata
    metadata_: Mapped[Optional[Dict[str, Any]]] = mapped_column("metadata", JSONB, nullable=True)

    # Relationships
    pipeline: Mapped["Pipeline"] = relationship("Pipeline", back_populates="job_dependencies")
    job_run: Mapped["JobRun"] = relationship("JobRun", back_populates="job_dependency", foreign_keys=[id])
    depends_on_job: Mapped[Optional["JobRun"]] = relationship(
        "JobRun", foreign_keys=[depends_on_job_id], remote_side="JobRun.id"
    )

    # Indexes
    __table_args__ = (
        Index("ix_job_dependencies_pipeline_id", "pipeline_id"),
        Index("ix_job_dependencies_depends_on_job_id", "depends_on_job_id"),
        Index("ix_job_dependencies_created_at", "created_at"),
        CheckConstraint(
            "dependency_type IS NULL OR dependency_type IN ('success_required', 'completion_required')",
            name="ck_job_dependencies_type_valid",
        ),
    )

    def __repr__(self) -> str:
        return (
            f"<JobDependency(id='{self.id}', pipeline_id='{self.pipeline_id}', depends_on='{self.depends_on_job_id}')>"
        )
