"""Create and enqueue a named pipeline for a single score set.

The one shared kickoff seam: build the ``Pipeline`` / ``JobRun`` / ``JobDependency`` records via
:class:`PipelineFactory`, then enqueue the ``start_pipeline`` entrypoint in ARQ. Both the ad-hoc
``run_pipeline`` script and the batch ``enrich_backfilled_score_sets`` driver go through here so the
enqueue contract (entrypoint id + :func:`arq_job_id`) lives in exactly one place.
"""

from __future__ import annotations

import datetime
import logging
from typing import Any, Optional

from arq.connections import ArqRedis
from arq.jobs import Job
from sqlalchemy.orm import Session

from mavedb.lib.logging.context import correlation_id_for_context, logging_context, save_to_logging_context
from mavedb.lib.workflow.pipeline_factory import PipelineFactory
from mavedb.models.pipeline import Pipeline
from mavedb.models.score_set import ScoreSet
from mavedb.models.user import User
from mavedb.worker.lib.managers.utils import arq_job_id

logger = logging.getLogger(__name__)


async def enqueue_pipeline_for_score_set(
    db: Session,
    redis: ArqRedis,
    *,
    pipeline_name: str,
    score_set: ScoreSet,
    user: User,
    extra_params: Optional[dict[str, Any]] = None,
) -> tuple[Pipeline, Optional[Job]]:
    """Create the pipeline records for ``score_set`` and enqueue its ``start_pipeline`` entrypoint.

    Returns the created :class:`Pipeline` and the enqueued ARQ :class:`Job` (``None`` when ARQ
    deduplicated an already-queued job with the same id). Raises ``KeyError`` for an unknown
    ``pipeline_name`` and ``ValueError`` for an invalid pipeline definition, surfaced from
    :meth:`PipelineFactory.create_pipeline`. If the ARQ enqueue itself raises, the just-created
    pipeline records are discarded (via :meth:`PipelineFactory.discard_pipeline`) before the
    exception is re-raised, so a failed enqueue never leaves an orphaned pipeline behind.
    """
    correlation_id = (
        correlation_id_for_context()
        or f"{pipeline_name}_{score_set.urn}_{user.id}_{datetime.datetime.now().isoformat()}"
    )
    params: dict[str, Any] = {
        "correlation_id": correlation_id,
        "score_set_id": score_set.id,
        "updater_id": user.id,
    }
    if extra_params:
        params.update(extra_params)

    factory = PipelineFactory(session=db)
    pipeline, entrypoint = factory.create_pipeline(
        pipeline_name=pipeline_name, creating_user=user, pipeline_params=params
    )
    job_id = arq_job_id(entrypoint)

    try:
        job = await redis.enqueue_job(entrypoint.job_function, entrypoint.id, _job_id=job_id)
    except Exception:
        logger.error(
            "Failed to enqueue pipeline %s for score set %s; discarding pipeline %s",
            pipeline_name,
            score_set.urn,
            pipeline.id,
            extra=logging_context(),
        )
        factory.discard_pipeline(pipeline)
        raise

    save_to_logging_context({"pipeline_id": pipeline.id, "job_id": job_id})

    if not job:
        logger.info(
            "Pipeline %s for score set %s already enqueued (job id: %s)",
            pipeline_name,
            score_set.urn,
            job_id,
            extra=logging_context(),
        )
    else:
        logger.info(
            "Enqueued pipeline %s for score set %s (job id: %s)",
            pipeline_name,
            score_set.urn,
            job_id,
            extra=logging_context(),
        )
    return pipeline, job
