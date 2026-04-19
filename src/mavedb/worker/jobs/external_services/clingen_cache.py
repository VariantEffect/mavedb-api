"""ClinGen cache pre-warming job.

Pre-fetches ClinGen allele data into the Redis cache before downstream annotation
jobs fan out. Without this, 40+ concurrent ClinVar refresh jobs all miss the cache
simultaneously and stampede the ClinGen API, causing large payloads to contend for
Redis write slots and triggering timeouts.
"""

import logging

from sqlalchemy import select

from mavedb.lib.clingen.allele_registry import get_clingen_allele_data
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
    one serially via `get_clingen_allele_data()` (which populates the aiocache Redis
    cache). Downstream jobs that depend on this step will see 100% cache hits.
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
        job_manager.update_progress(100, 100, "No ClinGen allele IDs to warm.")
        return JobExecutionOutcome.succeeded(data={"warmed": 0, "failed": 0})

    # Fetch each allele serially to avoid stampeding the ClinGen API.
    # get_clingen_allele_data() is decorated with @cached, so each call populates Redis.
    warmed = 0
    failed = 0
    for index, allele_id in enumerate(allele_ids):
        try:
            await get_clingen_allele_data(allele_id)
            warmed += 1
        except Exception:
            failed += 1
            logger.warning(
                f"Failed to warm cache for allele {allele_id}",
                extra=job_manager.logging_context(),
                exc_info=True,
            )

        if total > 0 and index % max(total // 20, 1) == 0:
            job_manager.update_progress(
                int((index / total) * 100),
                100,
                f"Warming ClinGen cache ({index}/{total}).",
            )

    job_manager.update_progress(100, 100, f"Cache warming complete. Warmed: {warmed}, failed: {failed}.")
    logger.info(
        f"ClinGen cache pre-warming complete. Warmed: {warmed}, failed: {failed}.",
        extra=job_manager.logging_context(),
    )

    return JobExecutionOutcome.succeeded(data={"warmed": warmed, "failed": failed, "total": total})
