"""Create and enqueue a named pipeline for a single score set.

The one shared kickoff seam: build the ``Pipeline`` / ``JobRun`` / ``JobDependency`` records via
:class:`PipelineFactory`, then enqueue the ``start_pipeline`` entrypoint in ARQ. Both the ad-hoc
``run_pipeline`` script and the batch ``enrich_backfilled_score_sets`` driver go through here so the
enqueue contract (entrypoint id + :func:`arq_job_id` + :func:`arq_queue_name`) lives in exactly one place.

API routes run in FastAPI's thread pool and call :func:`enqueue_pipeline_for_score_set_from_thread`, which
writes the records in the calling thread and hands only the ARQ call to the event loop. Async callers
use :func:`enqueue_pipeline_for_score_set`.
"""

from __future__ import annotations

import datetime
import functools
import logging
from typing import Any, Optional

import anyio.from_thread
from arq.connections import ArqRedis
from arq.jobs import Job
from sqlalchemy.orm import Session

from mavedb.lib.logging.context import correlation_id_for_context, logging_context, save_to_logging_context
from mavedb.lib.workflow.pipeline_factory import PipelineFactory
from mavedb.models.job_run import JobRun
from mavedb.models.pipeline import Pipeline
from mavedb.models.score_set import ScoreSet
from mavedb.models.user import User
from mavedb.worker.lib.managers.utils import arq_job_id, arq_queue_name

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
    factory, pipeline, entrypoint = _create_pipeline(db, pipeline_name, score_set, user, extra_params)

    try:
        job = await redis.enqueue_job(*_enqueue_args(entrypoint), **_enqueue_kwargs(entrypoint))
    except Exception:
        _discard_after_failed_enqueue(factory, pipeline, pipeline_name, score_set)
        raise

    _log_enqueued(pipeline, entrypoint, job, pipeline_name, score_set)
    return pipeline, job


def enqueue_pipeline_for_score_set_from_thread(
    db: Session,
    redis: ArqRedis,
    *,
    pipeline_name: str,
    score_set: ScoreSet,
    user: User,
    extra_params: Optional[dict[str, Any]] = None,
) -> tuple[Pipeline, Optional[Job]]:
    """Do what :func:`enqueue_pipeline_for_score_set` does, from a thread started by AnyIO.

    Writes the pipeline records in the calling thread and runs only the ARQ enqueue on the event loop,
    so a route in FastAPI's thread pool never runs database work on the loop. Must be called from a
    worker thread that AnyIO started, such as a ``def`` route or a function passed to
    ``run_in_threadpool``; ``redis`` must belong to that loop. Returns and raises as
    :func:`enqueue_pipeline_for_score_set` does.
    """
    factory, pipeline, entrypoint = _create_pipeline(db, pipeline_name, score_set, user, extra_params)

    try:
        job = anyio.from_thread.run(
            functools.partial(redis.enqueue_job, *_enqueue_args(entrypoint), **_enqueue_kwargs(entrypoint))
        )
    except Exception:
        _discard_after_failed_enqueue(factory, pipeline, pipeline_name, score_set)
        raise

    _log_enqueued(pipeline, entrypoint, job, pipeline_name, score_set)
    return pipeline, job


def _create_pipeline(
    db: Session,
    pipeline_name: str,
    score_set: ScoreSet,
    user: User,
    extra_params: Optional[dict[str, Any]],
) -> tuple[PipelineFactory, Pipeline, JobRun]:
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
    return factory, pipeline, entrypoint


def _enqueue_args(entrypoint: JobRun) -> tuple[Any, ...]:
    return (entrypoint.job_function, entrypoint.id)


def _enqueue_kwargs(entrypoint: JobRun) -> dict[str, Any]:
    return {"_job_id": arq_job_id(entrypoint), "_queue_name": arq_queue_name(entrypoint)}


def _discard_after_failed_enqueue(
    factory: PipelineFactory, pipeline: Pipeline, pipeline_name: str, score_set: ScoreSet
) -> None:
    logger.error(
        "Failed to enqueue pipeline %s for score set %s; discarding pipeline %s",
        pipeline_name,
        score_set.urn,
        pipeline.id,
        extra=logging_context(),
    )
    factory.discard_pipeline(pipeline)


def _log_enqueued(
    pipeline: Pipeline, entrypoint: JobRun, job: Optional[Job], pipeline_name: str, score_set: ScoreSet
) -> None:
    job_id = arq_job_id(entrypoint)
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
