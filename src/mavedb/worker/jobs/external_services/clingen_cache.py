"""ClinGen cache pre-warming job.

Pre-fetches ClinGen allele data into the Redis cache before downstream annotation
jobs fan out. Without this, 40+ concurrent ClinVar refresh jobs all miss the cache
simultaneously and stampede the ClinGen API, causing large payloads to contend for
Redis write slots and triggering timeouts.

Fetches are made concurrently up to CLINGEN_CACHE_WARMING_CONCURRENCY (default 5)
to balance speed against ClinGen API and Redis write pool load.
"""

import asyncio
import logging

from sqlalchemy import select

from mavedb.lib.clingen.allele_registry import get_clingen_allele_data
from mavedb.lib.clingen.constants import CLINGEN_CACHE_WARMING_CONCURRENCY
from mavedb.lib.types.workflow import JobExecutionOutcome
from mavedb.models.mapped_variant import MappedVariant
from mavedb.models.score_set import ScoreSet
from mavedb.models.variant import Variant
from mavedb.worker.jobs.utils.setup import validate_job_params
from mavedb.worker.lib.decorators.pipeline_management import with_pipeline_management
from mavedb.worker.lib.managers.job_manager import JobManager

logger = logging.getLogger(__name__)


@with_pipeline_management
async def warm_clingen_cache(ctx: dict, job_id: int, job_manager: JobManager) -> JobExecutionOutcome:
    """Pre-warm the ClinGen allele data cache for all mapped variants in a score set.

    Queries all distinct ClinGen allele IDs from mapped variants, then fetches each
    one via `get_clingen_allele_data()` (which populates the aiocache Redis cache),
    with up to CLINGEN_CACHE_WARMING_CONCURRENCY requests in-flight at a time.
    Downstream jobs that depend on this step will see 100% cache hits.
    """
    job = job_manager.get_job()

    _job_required_params = ["score_set_id", "correlation_id"]
    validate_job_params(_job_required_params, job)

    score_set = job_manager.db.scalars(select(ScoreSet).where(ScoreSet.id == job.job_params["score_set_id"])).one()  # type: ignore
    correlation_id = job.job_params["correlation_id"]  # type: ignore

    job_manager.save_to_context(
        {
            "application": "mavedb-worker",
            "function": "warm_clingen_cache",
            "resource": score_set.urn,
            "correlation_id": correlation_id,
        }
    )
    job_manager.update_progress(0, 100, "Starting ClinGen cache pre-warming.")
    logger.info("Starting ClinGen cache pre-warming", extra=job_manager.logging_context())

    # Get distinct clingen_allele_ids for this score set's current mapped variants
    allele_ids = job_manager.db.scalars(
        select(MappedVariant.clingen_allele_id)
        .join(Variant)
        .where(
            Variant.score_set_id == score_set.id,
            MappedVariant.current.is_(True),
            MappedVariant.clingen_allele_id.isnot(None),
            # Exclude multi-variant IDs (comma-separated) — they can't be fetched individually
            MappedVariant.clingen_allele_id.not_like("%,%"),
        )
        .distinct()
    ).all()

    total = len(allele_ids)
    job_manager.save_to_context({"total_allele_ids_to_warm": total})
    logger.info(f"Found {total} distinct ClinGen allele IDs to pre-warm", extra=job_manager.logging_context())

    if total == 0:
        job_manager.db.flush()
        return JobExecutionOutcome.succeeded(data={"warmed": 0, "failed": 0})

    # Fetch alleles concurrently up to CLINGEN_CACHE_WARMING_CONCURRENCY in-flight at a time.
    # get_clingen_allele_data() is decorated with @cached, so each call populates Redis.
    semaphore = asyncio.Semaphore(CLINGEN_CACHE_WARMING_CONCURRENCY)

    async def fetch_one(allele_id: str) -> tuple[str, bool, BaseException | None]:
        async with semaphore:
            try:
                await get_clingen_allele_data(allele_id)
                return allele_id, True, None
            except Exception as exc:
                return allele_id, False, exc

    warmed = 0
    failed = 0
    for index, completed_task in enumerate(asyncio.as_completed([fetch_one(a) for a in allele_ids if a])):
        allele_id, success, exc = await completed_task
        if success:
            warmed += 1
        else:
            failed += 1
            logger.warning(
                f"Failed to warm cache for allele {allele_id}",
                extra=job_manager.logging_context(),
                exc_info=exc,
            )

        if total > 0 and index % max(total // 20, 1) == 0:
            job_manager.save_to_context({"warmed_alleles": warmed, "failed_alleles": failed})
            job_manager.update_progress(
                int((index / total) * 100),
                100,
                f"Warming ClinGen cache ({index}/{total}).",
            )
            logger.info(
                f"Warming ClinGen cache: {index}/{total} allele IDs processed. Warmed: {warmed}, failed: {failed}.",
                extra=job_manager.logging_context(),
            )

    logger.info(
        f"ClinGen cache pre-warming complete. Warmed: {warmed}, failed: {failed}.",
        extra=job_manager.logging_context(),
    )

    job_manager.db.flush()
    return JobExecutionOutcome.succeeded(data={"warmed": warmed, "failed": failed, "total": total})
