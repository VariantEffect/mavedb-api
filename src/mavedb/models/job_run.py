"""
SQLAlchemy models for job runs.
"""

from datetime import datetime
from typing import TYPE_CHECKING, Any, Dict, Optional

from sqlalchemy import CheckConstraint, DateTime, ForeignKey, Index, Integer, String, Text, func
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.mutable import MutableDict
from sqlalchemy.orm import Mapped, mapped_column, relationship

from mavedb.db.base import Base
from mavedb.lib.urns import generate_job_run_urn
from mavedb.models.enums import JobStatus

if TYPE_CHECKING:
    from mavedb.models.job_dependency import JobDependency
    from mavedb.models.pipeline import Pipeline


class JobRun(Base):
    """
    Represents a single execution of a job.

    Jobs can be retried, so there may be multiple JobRun records for the same logical job.

    NOTE: JSONB fields are automatically tracked as mutable objects in this class via MutableDict.
          This tracker only works for top-level mutations. If you mutate nested objects, you must call
          `flag_modified(instance, "metadata_")` to ensure changes are persisted.
    """

    __tablename__ = "job_runs"

    # Primary identification
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    urn: Mapped[str] = mapped_column(String(255), nullable=True, unique=True, default=generate_job_run_urn)

    # Job definition
    job_type: Mapped[str] = mapped_column(String(100), nullable=False)
    job_function: Mapped[str] = mapped_column(String(255), nullable=False)
    job_params: Mapped[Optional[Dict[str, Any]]] = mapped_column(MutableDict.as_mutable(JSONB), nullable=True)

    # Execution tracking
    status: Mapped[JobStatus] = mapped_column(String(50), nullable=False, default=JobStatus.PENDING)

    # Pipeline association
    pipeline_id: Mapped[Optional[int]] = mapped_column(
        Integer, ForeignKey("pipelines.id", ondelete="SET NULL"), nullable=True
    )

    # Priority and scheduling
    priority: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    max_retries: Mapped[int] = mapped_column(Integer, nullable=False, default=3)
    retry_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    retry_delay_seconds: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)

    # Timing
    scheduled_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())
    started_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    finished_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())

    # Error handling
    error_message: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    error_traceback: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    failure_category: Mapped[Optional[str]] = mapped_column(String(100), nullable=True)

    # Progress tracking
    progress_current: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    progress_total: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    progress_message: Mapped[Optional[str]] = mapped_column(String(500), nullable=True)

    # Correlation for tracing
    correlation_id: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)

    # Flexible metadata
    metadata_: Mapped[Dict[str, Any]] = mapped_column(
        "metadata", MutableDict.as_mutable(JSONB), nullable=False, server_default="{}"
    )

    # Version tracking
    mavedb_version: Mapped[Optional[str]] = mapped_column(String(50), nullable=True)

    # Relationships
    job_dependencies: Mapped[list["JobDependency"]] = relationship(
        "JobDependency", back_populates="job_run", uselist=True, foreign_keys="[JobDependency.id]"
    )
    pipeline: Mapped[Optional["Pipeline"]] = relationship(
        "Pipeline", back_populates="job_runs", foreign_keys="[JobRun.pipeline_id]"
    )

    # Indexes
    __table_args__ = (
        Index("ix_job_runs_status", "status"),
        Index("ix_job_runs_job_type", "job_type"),
        Index("ix_job_runs_pipeline_id", "pipeline_id"),
        Index("ix_job_runs_scheduled_at", "scheduled_at"),
        Index("ix_job_runs_created_at", "created_at"),
        Index("ix_job_runs_correlation_id", "correlation_id"),
        Index("ix_job_runs_status_scheduled", "status", "scheduled_at"),
        CheckConstraint(
            "status IN ('pending', 'queued', 'running', 'succeeded', 'failed', 'cancelled', 'skipped')",
            name="ck_job_runs_status_valid",
        ),
        CheckConstraint("priority >= 0", name="ck_job_runs_priority_positive"),
        CheckConstraint("max_retries >= 0", name="ck_job_runs_max_retries_positive"),
        CheckConstraint("retry_count >= 0", name="ck_job_runs_retry_count_positive"),
    )

    def __repr__(self) -> str:
        return f"<JobRun(id='{self.id}', job_type='{self.job_type}', status='{self.status}')>"
