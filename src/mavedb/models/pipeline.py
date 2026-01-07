"""
SQLAlchemy models for job pipelines.
"""

from datetime import datetime
from typing import TYPE_CHECKING, Any, Dict, List, Optional

from sqlalchemy import CheckConstraint, DateTime, ForeignKey, Index, Integer, String, Text, func
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy.orm import Mapped, mapped_column, relationship

from mavedb.db.base import Base
from mavedb.models.enums import PipelineStatus

if TYPE_CHECKING:
    from mavedb.models.job_dependency import JobDependency
    from mavedb.models.user import User


class Pipeline(Base):
    """
    Represents a high-level workflow that groups related jobs.

    Examples:
    - Processing a score set upload
    - Batch re-annotation of variants
    - Database migration workflows
    """

    __tablename__ = "pipelines"

    # Primary identification
    id: Mapped[str] = mapped_column(String(255), primary_key=True)
    name: Mapped[str] = mapped_column(String(500), nullable=False)
    description: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

    # Status and lifecycle
    status: Mapped[PipelineStatus] = mapped_column(String(50), nullable=False, default=PipelineStatus.CREATED)

    # Correlation for end-to-end tracing
    correlation_id: Mapped[Optional[str]] = mapped_column(String(255), nullable=True, index=True)

    # Flexible metadata storage
    metadata_: Mapped[Optional[Dict[str, Any]]] = mapped_column(
        "metadata", JSONB, nullable=True, comment="Flexible metadata storage for pipeline-specific data"
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
    job_dependencies: Mapped[List["JobDependency"]] = relationship(
        "JobDependency", back_populates="pipeline", cascade="all, delete-orphan"
    )
    created_by_user: Mapped[Optional["User"]] = relationship("User", foreign_keys=[created_by_user_id])

    # Indexes
    __table_args__ = (
        Index("ix_pipelines_status", "status"),
        Index("ix_pipelines_created_at", "created_at"),
        Index("ix_pipelines_correlation_id", "correlation_id"),
        Index("ix_pipelines_created_by_user_id", "created_by_user_id"),
        CheckConstraint(
            "status IN ('created', 'running', 'completed', 'failed', 'cancelled')", name="ck_pipelines_status_valid"
        ),
    )

    def __repr__(self) -> str:
        return f"<Pipeline(id='{self.id}', name='{self.name}', status='{self.status}')>"

    @hybrid_property
    def duration_seconds(self) -> Optional[int]:
        """Calculate pipeline duration in seconds."""
        if self.started_at and self.finished_at:
            return int((self.finished_at - self.started_at).total_seconds())

        return None
