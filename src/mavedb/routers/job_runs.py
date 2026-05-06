"""Admin-only observability endpoints for job run inspection.

These endpoints expose job run status, progress, and error details to operators
for diagnosing stuck or failing jobs. Permissions are currently admin-only;
finer-grained access checks can be added later when user-facing UI consumes
this data.
"""

import logging
from datetime import datetime
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import select
from sqlalchemy.orm import Session

from mavedb import deps
from mavedb.lib.authorization import RoleRequirer
from mavedb.lib.logging import LoggedRoute
from mavedb.lib.logging.context import logging_context, save_to_logging_context
from mavedb.lib.types.authentication import UserData
from mavedb.models.enums.job_pipeline import JobStatus
from mavedb.models.enums.user_role import UserRole
from mavedb.models.job_run import JobRun
from mavedb.routers.shared import ACCESS_CONTROL_ERROR_RESPONSES, PUBLIC_ERROR_RESPONSES, ROUTER_BASE_PREFIX
from mavedb.view_models import job_run as job_run_view

TAG_NAME = "Job Runs"

router = APIRouter(
    prefix=f"{ROUTER_BASE_PREFIX}/job-runs",
    tags=[TAG_NAME],
    responses={**PUBLIC_ERROR_RESPONSES},
    route_class=LoggedRoute,
)

metadata = {
    "name": TAG_NAME,
    "description": "Operator observability for background job executions.",
}

logger = logging.getLogger(__name__)


@router.get(
    "/",
    status_code=200,
    response_model=list[job_run_view.SavedJobRun],
    responses={**ACCESS_CONTROL_ERROR_RESPONSES},
    summary="List job runs",
)
def list_job_runs(
    *,
    db: Session = Depends(deps.get_db),
    _: UserData = Depends(RoleRequirer([UserRole.admin])),
    status: Optional[JobStatus] = Query(None, description="Filter by job run status."),
    job_type: Optional[str] = Query(None, description="Filter by job type."),
    job_function: Optional[str] = Query(None, description="Filter by job function name."),
    correlation_id: Optional[str] = Query(None, description="Filter by correlation id."),
    pipeline_id: Optional[int] = Query(None, description="Filter by parent pipeline id."),
    created_after: Optional[datetime] = Query(None, description="Only return job runs created at or after this time."),
    created_before: Optional[datetime] = Query(
        None, description="Only return job runs created at or before this time."
    ),
    limit: int = Query(50, ge=1, le=500),
    offset: int = Query(0, ge=0),
) -> list[JobRun]:
    """List job runs with optional filters. Admin only."""
    query = select(JobRun)
    if status is not None:
        query = query.where(JobRun.status == status)
    if job_type is not None:
        query = query.where(JobRun.job_type == job_type)
    if job_function is not None:
        query = query.where(JobRun.job_function == job_function)
    if correlation_id is not None:
        query = query.where(JobRun.correlation_id == correlation_id)
    if pipeline_id is not None:
        query = query.where(JobRun.pipeline_id == pipeline_id)
    if created_after is not None:
        query = query.where(JobRun.created_at >= created_after)
    if created_before is not None:
        query = query.where(JobRun.created_at <= created_before)

    query = query.order_by(JobRun.created_at.desc()).limit(limit).offset(offset)
    return list(db.scalars(query).all())


@router.get(
    "/{urn}",
    status_code=200,
    response_model=job_run_view.JobRunDetail,
    responses={**ACCESS_CONTROL_ERROR_RESPONSES},
    summary="Show job run with full error details",
)
def show_job_run(
    *,
    urn: str,
    db: Session = Depends(deps.get_db),
    _: UserData = Depends(RoleRequirer([UserRole.admin])),
) -> JobRun:
    """Fetch a single job run by URN, including error traceback. Admin only."""
    save_to_logging_context({"requested_job_run_urn": urn})
    job_run = db.scalars(select(JobRun).where(JobRun.urn == urn)).one_or_none()
    if job_run is None:
        logger.warning(msg="Could not show job run; job run does not exist.", extra=logging_context())
        raise HTTPException(status_code=404, detail=f"job run with URN {urn} not found")

    return job_run
