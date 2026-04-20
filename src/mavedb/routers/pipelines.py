"""Admin-only observability endpoints for pipeline inspection.

These endpoints expose pipeline status, progress, and listings to operators so
they can diagnose stuck or failing pipelines without direct database access.
Permissions are currently admin-only; finer-grained access checks can be added
later when user-facing UI consumes this data.
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
from mavedb.models.enums.job_pipeline import PipelineStatus
from mavedb.models.enums.user_role import UserRole
from mavedb.models.pipeline import Pipeline
from mavedb.routers.shared import ACCESS_CONTROL_ERROR_RESPONSES, PUBLIC_ERROR_RESPONSES, ROUTER_BASE_PREFIX
from mavedb.view_models import pipeline as pipeline_view
from mavedb.worker.lib.managers.exceptions import DatabaseConnectionError, PipelineStateError
from mavedb.worker.lib.managers.pipeline_manager import PipelineManager

TAG_NAME = "Pipelines"

router = APIRouter(
    prefix=f"{ROUTER_BASE_PREFIX}/pipelines",
    tags=[TAG_NAME],
    responses={**PUBLIC_ERROR_RESPONSES},
    route_class=LoggedRoute,
)

metadata = {
    "name": TAG_NAME,
    "description": "Operator observability for background pipeline executions.",
}

logger = logging.getLogger(__name__)


@router.get(
    "/",
    status_code=200,
    response_model=list[pipeline_view.SavedPipeline],
    responses={**ACCESS_CONTROL_ERROR_RESPONSES},
    summary="List pipelines",
)
def list_pipelines(
    *,
    db: Session = Depends(deps.get_db),
    _: UserData = Depends(RoleRequirer([UserRole.admin])),
    status: Optional[PipelineStatus] = Query(None, description="Filter by pipeline status."),
    name: Optional[str] = Query(None, description="Filter by pipeline name (exact match)."),
    correlation_id: Optional[str] = Query(None, description="Filter by correlation id."),
    created_by_user_id: Optional[int] = Query(None, description="Filter by creating user id."),
    created_after: Optional[datetime] = Query(None, description="Only return pipelines created at or after this time."),
    created_before: Optional[datetime] = Query(
        None, description="Only return pipelines created at or before this time."
    ),
    limit: int = Query(50, ge=1, le=500),
    offset: int = Query(0, ge=0),
) -> list[Pipeline]:
    """List pipelines with optional filters. Admin only."""
    query = select(Pipeline)
    if status is not None:
        query = query.where(Pipeline.status == status)
    if name is not None:
        query = query.where(Pipeline.name == name)
    if correlation_id is not None:
        query = query.where(Pipeline.correlation_id == correlation_id)
    if created_by_user_id is not None:
        query = query.where(Pipeline.created_by_user_id == created_by_user_id)
    if created_after is not None:
        query = query.where(Pipeline.created_at >= created_after)
    if created_before is not None:
        query = query.where(Pipeline.created_at <= created_before)

    query = query.order_by(Pipeline.created_at.desc()).limit(limit).offset(offset)
    return list(db.scalars(query).all())


@router.get(
    "/{urn}",
    status_code=200,
    response_model=pipeline_view.PipelineDetail,
    responses={**ACCESS_CONTROL_ERROR_RESPONSES},
    summary="Show pipeline with progress",
)
def show_pipeline(
    *,
    urn: str,
    db: Session = Depends(deps.get_db),
    _: UserData = Depends(RoleRequirer([UserRole.admin])),
) -> pipeline_view.PipelineDetail:
    """Fetch a single pipeline by URN including job progress statistics. Admin only."""
    save_to_logging_context({"requested_pipeline_urn": urn})
    pipeline = db.scalars(select(Pipeline).where(Pipeline.urn == urn)).one_or_none()
    if pipeline is None:
        logger.warning(msg="Could not show pipeline; pipeline does not exist.", extra=logging_context())
        raise HTTPException(status_code=404, detail=f"pipeline with URN {urn} not found")

    # PipelineManager is reused here rather than duplicating progress aggregation logic.
    # Redis is not required for read-only progress aggregation, so None is acceptable if somewhat hacky.
    manager = PipelineManager(db=db, redis=None, pipeline_id=pipeline.id)  # type: ignore[arg-type]
    try:
        progress = manager.get_pipeline_progress()
    except (DatabaseConnectionError, PipelineStateError) as exc:
        logger.exception(msg="Failed to compute pipeline progress.", extra=logging_context())
        raise HTTPException(status_code=500, detail=str(exc))

    saved = pipeline_view.SavedPipeline.model_validate(pipeline)
    return pipeline_view.PipelineDetail(
        **saved.model_dump(by_alias=False),
        progress=pipeline_view.PipelineProgress(**progress),
    )
