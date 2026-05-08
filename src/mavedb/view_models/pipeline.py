from datetime import datetime
from typing import Any, Optional

from pydantic import Field

from mavedb.models.enums.job_pipeline import PipelineStatus
from mavedb.view_models.base.base import BaseModel


class PipelineBase(BaseModel):
    """Base view model for pipelines."""

    urn: Optional[str] = None
    name: str
    description: Optional[str] = None
    status: PipelineStatus
    correlation_id: Optional[str] = None
    mavedb_version: Optional[str] = None


class SavedPipeline(PipelineBase):
    """View model for a saved pipeline record."""

    id: int
    # Read from the ORM's `metadata_` attribute (field name). Serialize under JSON key
    # `metadata` for operator readability. We cannot use `alias="metadata"` because the
    # SQLAlchemy Base exposes a class-level `metadata` attribute (MetaData) that would
    # otherwise shadow the mapped column when Pydantic reads attributes.
    metadata_: dict[str, Any] = Field(default_factory=dict, serialization_alias="metadata")
    created_at: datetime
    started_at: Optional[datetime] = None
    finished_at: Optional[datetime] = None
    created_by_user_id: Optional[int] = None

    class Config:
        from_attributes = True
        populate_by_name = True


class PipelineProgress(BaseModel):
    """Pipeline progress statistics returned by PipelineManager.get_pipeline_progress()."""

    total_jobs: int
    completed_jobs: int
    successful_jobs: int
    failed_jobs: int
    running_jobs: int
    pending_jobs: int
    completion_percentage: float
    duration: int
    status_counts: dict[str, int]


class PipelineDetail(SavedPipeline):
    """Single-pipeline detail response including progress statistics."""

    progress: PipelineProgress
