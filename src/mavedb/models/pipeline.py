"""
SQLAlchemy models for job pipelines.
"""

from datetime import datetime
from typing import TYPE_CHECKING, Any, Dict, List, Optional

from sqlalchemy import CheckConstraint, DateTime, ForeignKey, Index, Integer, String, Text, func
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.mutable import MutableDict
from sqlalchemy.orm import Mapped, mapped_column, relationship

from mavedb.db.base import Base
from mavedb.lib.urns import generate_pipeline_urn
from mavedb.models.enums import PipelineStatus
from mavedb.models.job_run import JobRun

if TYPE_CHECKING:
    from mavedb.models.user import User


class Pipeline(Base):
    """
    Represents a high-level workflow that groups related jobs.

    Examples:
    - Processing a score set upload
    - Batch re-annotation of variants
    - Database migration workflows

    NOTE: JSONB fields are automatically tracked as mutable objects in this class via MutableDict.
          This tracker only works for top-level mutations. If you mutate nested objects, you must call
          `flag_modified(instance, "metadata_")` to ensure changes are persisted.
    """

    __tablename__ = "pipelines"

    # Primary identification
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    urn: Mapped[str] = mapped_column(String(255), nullable=True, unique=True, default=generate_pipeline_urn)
    name: Mapped[str] = mapped_column(String(500), nullable=False)
    description: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

    # Status and lifecycle
    status: Mapped[PipelineStatus] = mapped_column(String(50), nullable=False, default=PipelineStatus.CREATED)

    # Correlation for end-to-end tracing
    correlation_id: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)

    # Flexible metadata storage
    metadata_: Mapped[Dict[str, Any]] = mapped_column(
        "metadata",
        MutableDict.as_mutable(JSONB),
        nullable=False,
        comment="Flexible metadata storage for pipeline-specific data",
        server_default="{}",
    )

    # Timestamps
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())
    started_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    finished_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)

    # User tracking
    created_by_user_id: Mapped[Optional[int]] = mapped_column(
        Integer, ForeignKey("users.id", ondelete="SET NULL"), nullable=True
    )

    # Version tracking
    mavedb_version: Mapped[Optional[str]] = mapped_column(String(50), nullable=True)

    # Relationships
    job_runs: Mapped[List["JobRun"]] = relationship("JobRun", back_populates="pipeline", cascade="all, delete-orphan")
    created_by_user: Mapped[Optional["User"]] = relationship("User", foreign_keys=[created_by_user_id])

    # Indexes
    __table_args__ = (
        Index("ix_pipelines_status", "status"),
        Index("ix_pipelines_created_at", "created_at"),
        Index("ix_pipelines_correlation_id", "correlation_id"),
        Index("ix_pipelines_created_by_user_id", "created_by_user_id"),
        CheckConstraint(
            "status IN ('created', 'running', 'succeeded', 'failed', 'cancelled', 'paused', 'partial')",
            name="ck_pipelines_status_valid",
        ),
    )

    def __repr__(self) -> str:
        return f"<Pipeline(id='{self.id}', name='{self.name}', status='{self.status}')>"
