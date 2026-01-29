"""
SQLAlchemy models for job dependencies.
"""

from datetime import datetime
from typing import TYPE_CHECKING, Any, Dict, Optional

from sqlalchemy import CheckConstraint, DateTime, ForeignKey, Index, Integer, String, func
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.mutable import MutableDict
from sqlalchemy.orm import Mapped, mapped_column, relationship

from mavedb.db.base import Base
from mavedb.models.enums import DependencyType

if TYPE_CHECKING:
    from mavedb.models.job_run import JobRun


class JobDependency(Base):
    """
    Defines dependencies between jobs within a pipeline.

    This table maps jobs to their pipeline and defines execution order.

    NOTE: JSONB fields are automatically tracked as mutable objects in this class via MutableDict.
          This tracker only works for top-level mutations. If you mutate nested objects, you must call
          `flag_modified(instance, "metadata_")` to ensure changes are persisted.
    """

    __tablename__ = "job_dependencies"

    # The job being defined (references job_runs.id). Composite primary key with the dependency we are defining.
    id: Mapped[int] = mapped_column(Integer, ForeignKey("job_runs.id", ondelete="CASCADE"), primary_key=True)
    depends_on_job_id: Mapped[int] = mapped_column(
        Integer, ForeignKey("job_runs.id", ondelete="CASCADE"), nullable=False, primary_key=True
    )

    # Type of dependency
    dependency_type: Mapped[Optional[DependencyType]] = mapped_column(String(50), nullable=False)

    # Timestamps
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())

    # Flexible metadata
    metadata_: Mapped[Optional[Dict[str, Any]]] = mapped_column(
        "metadata", MutableDict.as_mutable(JSONB), nullable=True
    )

    # Relationships
    job_run: Mapped["JobRun"] = relationship("JobRun", back_populates="job_dependencies", foreign_keys=[id])
    depends_on_job: Mapped["JobRun"] = relationship("JobRun", foreign_keys=[depends_on_job_id], remote_side="JobRun.id")

    # Indexes
    __table_args__ = (
        Index("ix_job_dependencies_depends_on_job_id", "depends_on_job_id"),
        Index("ix_job_dependencies_created_at", "created_at"),
        CheckConstraint(
            "dependency_type IS NULL OR dependency_type IN ('success_required', 'completion_required')",
            name="ck_job_dependencies_type_valid",
        ),
    )

    def __repr__(self) -> str:
        return f"<JobDependency(id='{self.id}', depends_on='{self.depends_on_job_id}', dependency_type='{self.dependency_type}')>"
