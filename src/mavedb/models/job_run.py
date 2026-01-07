"""
SQLAlchemy models for job runs.
"""

from datetime import datetime
from typing import TYPE_CHECKING, Any, Dict, Optional

from sqlalchemy import CheckConstraint, DateTime, Index, Integer, String, Text, func
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy.orm import Mapped, mapped_column, relationship

from mavedb.db.base import Base
from mavedb.models.enums import JobStatus

if TYPE_CHECKING:
    from mavedb.models.job_dependency import JobDependency


class JobRun(Base):
    """
    Represents a single execution of a job.

    Jobs can be retried, so there may be multiple JobRun records for the same logical job.
    """

    __tablename__ = "job_runs"

    # Primary identification
    id: Mapped[str] = mapped_column(String(255), primary_key=True)

    # Job definition
    job_type: Mapped[str] = mapped_column(String(100), nullable=False, index=True)
    job_function: Mapped[str] = mapped_column(String(255), nullable=False)
    job_params: Mapped[Optional[Dict[str, Any]]] = mapped_column(JSONB, nullable=True)

    # Execution tracking
    status: Mapped[JobStatus] = mapped_column(String(50), nullable=False, default=JobStatus.PENDING)

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
    correlation_id: Mapped[Optional[str]] = mapped_column(String(255), nullable=True, index=True)

    # Flexible metadata
    metadata_: Mapped[Optional[Dict[str, Any]]] = mapped_column("metadata", JSONB, nullable=True)

    # Version tracking
    mavedb_version: Mapped[Optional[str]] = mapped_column(String(50), nullable=True)

    # Relationships
    job_dependency: Mapped[Optional["JobDependency"]] = relationship(
        "JobDependency", back_populates="job_run", uselist=False, foreign_keys="[JobDependency.id]"
    )

    # Indexes
    __table_args__ = (
        Index("ix_job_runs_status", "status"),
        Index("ix_job_runs_job_type", "job_type"),
        Index("ix_job_runs_scheduled_at", "scheduled_at"),
        Index("ix_job_runs_created_at", "created_at"),
        Index("ix_job_runs_correlation_id", "correlation_id"),
        Index("ix_job_runs_status_scheduled", "status", "scheduled_at"),
        CheckConstraint(
            "status IN ('pending', 'running', 'completed', 'failed', 'cancelled', 'retrying')",
            name="ck_job_runs_status_valid",
        ),
        CheckConstraint("priority >= 0", name="ck_job_runs_priority_positive"),
        CheckConstraint("max_retries >= 0", name="ck_job_runs_max_retries_positive"),
        CheckConstraint("retry_count >= 0", name="ck_job_runs_retry_count_positive"),
    )

    def __repr__(self) -> str:
        return f"<JobRun(id='{self.id}', job_type='{self.job_type}', status='{self.status}')>"

    @hybrid_property
    def duration_seconds(self) -> Optional[int]:
        """Calculate job duration in seconds."""
        if self.started_at and self.finished_at:
            return int((self.finished_at - self.started_at).total_seconds())
        return None

    @hybrid_property
    def progress_percentage(self) -> Optional[float]:
        """Calculate progress as percentage."""
        if self.progress_total and self.progress_total > 0:
            return (self.progress_current or 0) / self.progress_total * 100
        return None

    @property
    def can_retry(self) -> bool:
        """Check if job can be retried."""
        return self.status == JobStatus.FAILED and self.retry_count < self.max_retries
