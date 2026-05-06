from datetime import datetime
from typing import Any, Optional

from pydantic import Field

from mavedb.models.enums.job_pipeline import JobStatus
from mavedb.view_models.base.base import BaseModel


class JobRunBase(BaseModel):
    """Base view model for job runs."""

    urn: Optional[str] = None
    job_type: str
    job_function: str
    status: JobStatus
    correlation_id: Optional[str] = None
    pipeline_id: Optional[int] = None
    failure_category: Optional[str] = None
    error_message: Optional[str] = None
    mavedb_version: Optional[str] = None


class SavedJobRun(JobRunBase):
    """View model for a saved job run record."""

    id: int
    job_params: Optional[dict[str, Any]] = None
    # Read from the ORM's `metadata_` attribute (field name). Serialize under JSON key
    # `metadata` for operator readability. We cannot use `alias="metadata"` because the
    # SQLAlchemy Base exposes a class-level `metadata` attribute (MetaData) that would
    # otherwise shadow the mapped column when Pydantic reads attributes.
    metadata_: dict[str, Any] = Field(default_factory=dict, serialization_alias="metadata")

    max_retries: int
    retry_count: int
    retry_delay_seconds: Optional[int] = None

    scheduled_at: datetime
    started_at: Optional[datetime] = None
    finished_at: Optional[datetime] = None
    created_at: datetime

    progress_current: Optional[int] = None
    progress_total: Optional[int] = None
    progress_message: Optional[str] = None

    class Config:
        from_attributes = True
        populate_by_name = True


class JobRunDetail(SavedJobRun):
    """Single-job-run detail response including the error traceback."""

    error_traceback: Optional[str] = None
