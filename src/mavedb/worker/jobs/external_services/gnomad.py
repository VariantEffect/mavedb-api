"""gnomAD variant linking jobs for population frequency annotation.

This module handles linking of mapped variants to gnomAD (Genome Aggregation Database)
variants to provide population frequency and other genomic context information.
This enrichment helps researchers understand the clinical significance and
rarity of variants in their datasets.
"""

import logging
from typing import Sequence

from sqlalchemy import select

from mavedb.db import athena
from mavedb.lib.annotation_status_manager import AnnotationStatusManager
from mavedb.lib.gnomad import (
    GNOMAD_DATA_VERSION,
    gnomad_variant_data_for_caids,
    link_gnomad_variants_to_mapped_variants,
)
from mavedb.models.enums.annotation_type import AnnotationType
from mavedb.models.enums.job_pipeline import AnnotationStatus
from mavedb.models.mapped_variant import MappedVariant
from mavedb.models.score_set import ScoreSet
from mavedb.models.variant import Variant
from mavedb.worker.jobs.utils.setup import validate_job_params
from mavedb.worker.lib.decorators.pipeline_management import with_pipeline_management
from mavedb.worker.lib.managers.job_manager import JobManager
from mavedb.worker.lib.managers.types import JobResultData

logger = logging.getLogger(__name__)


@with_pipeline_management
async def link_gnomad_variants(ctx: dict, job_id: int, job_manager: JobManager) -> JobResultData:
    """
    Link mapped variants to gnomAD variants based on ClinGen Allele IDs (CAIDs).
    This job fetches mapped variants associated with a given score set that have CAIDs,
    retrieves corresponding gnomAD variant data, and establishes links between them
    in the database.

    Job Parameters:
        - score_set_id (int): The ID of the ScoreSet containing mapped variants to process.
        - correlation_id (str): Correlation ID for tracing requests across services.

    Args:
        ctx (dict): The job context dictionary.
        job_id (int): The ID of the job being executed.
        job_manager (JobManager): The job manager instance for database and logging operations.

    Side Effects:
        - Updates MappedVariant records to link to gnomAD variants.

    Returns:
        dict: Result indicating success and any exception details
    """
    # Get the job definition we are working on
    job = job_manager.get_job()

    _job_required_params = ["score_set_id", "correlation_id"]
    validate_job_params(_job_required_params, job)

    # Fetch required resources based on param inputs. Safely ignore mypy warnings here, as they were checked above.
    score_set = job_manager.db.scalars(select(ScoreSet).where(ScoreSet.id == job.job_params["score_set_id"])).one()  # type: ignore
    correlation_id = job.job_params["correlation_id"]  # type: ignore

    # Setup initial context and progress
    job_manager.save_to_context(
        {
            "application": "mavedb-worker",
            "function": "link_gnomad_variants",
            "resource": score_set.urn,
            "correlation_id": correlation_id,
        }
    )
    job_manager.update_progress(0, 100, "Starting gnomAD mapped resource linkage.")
    logger.info(msg="Started gnomAD mapped resource linkage", extra=job_manager.logging_context())

    # We filter out mapped variants that do not have a CAID, so this query is typed # as a Sequence[str]. Ignore MyPy's type checking here.
    variant_caids: Sequence[str] = job_manager.db.scalars(
        select(MappedVariant.clingen_allele_id)
        .join(Variant)
        .join(ScoreSet)
        .where(
            ScoreSet.urn == score_set.urn,
            MappedVariant.current.is_(True),
            MappedVariant.clingen_allele_id.is_not(None),
        )
    ).all()  # type: ignore

    num_variant_caids = len(variant_caids)
    job_manager.save_to_context({"num_variants_to_link_gnomad": num_variant_caids})

    if not variant_caids:
        job_manager.update_progress(100, 100, "No variants with CAIDs found to link to gnomAD variants. Nothing to do.")
        logger.warning(
            msg="No current mapped variants with CAIDs were found for this score set. Skipping gnomAD linkage (nothing to do).",
            extra=job_manager.logging_context(),
        )
        return {"status": "ok", "data": {}, "exception": None}

    job_manager.update_progress(10, 100, f"Found {num_variant_caids} variants with CAIDs to link to gnomAD variants.")
    logger.info(
        msg="Found current mapped variants with CAIDs for this score set. Attempting to link them to gnomAD variants.",
        extra=job_manager.logging_context(),
    )

    # Fetch gnomAD variant data for the CAIDs
    with athena.engine.connect() as athena_session:
        logger.debug("Fetching gnomAD variants from Athena.")
        gnomad_variant_data = gnomad_variant_data_for_caids(athena_session, variant_caids)

    num_gnomad_variants_with_caid_match = len(gnomad_variant_data)

    # NOTE: Proceed intentionally with linking even if no matches were found, to record skipped annotations.

    job_manager.save_to_context({"num_gnomad_variants_with_caid_match": num_gnomad_variants_with_caid_match})
    job_manager.update_progress(75, 100, f"Found {num_gnomad_variants_with_caid_match} gnomAD variants matching CAIDs.")

    # Link mapped variants to gnomAD variants
    logger.info(msg="Attempting to link mapped variants to gnomAD variants.", extra=job_manager.logging_context())
    num_linked_gnomad_variants = link_gnomad_variants_to_mapped_variants(job_manager.db, gnomad_variant_data)
    job_manager.db.flush()

    # For variants which are not linked, create annotation status records indicating skipped linkage
    mapped_variants_with_caids = job_manager.db.scalars(
        select(MappedVariant)
        .join(Variant)
        .join(ScoreSet)
        .where(
            ScoreSet.urn == score_set.urn,
            MappedVariant.current.is_(True),
            MappedVariant.clingen_allele_id.is_not(None),
        )
    ).all()
    annotation_manager = AnnotationStatusManager(job_manager.db)
    for mapped_variant in mapped_variants_with_caids:
        if not mapped_variant.gnomad_variants:
            annotation_manager.add_annotation(
                variant_id=mapped_variant.variant_id,  # type: ignore
                annotation_type=AnnotationType.GNOMAD_ALLELE_FREQUENCY,
                version=GNOMAD_DATA_VERSION,
                status=AnnotationStatus.SKIPPED,
                annotation_data={
                    "error_message": "No gnomAD variant could be linked for this mapped variant.",
                    "failure_category": "not_found",
                },
                current=True,
            )

    # Save final context and progress
    job_manager.save_to_context({"num_mapped_variants_linked_to_gnomad_variants": num_linked_gnomad_variants})
    job_manager.update_progress(100, 100, f"Linked {num_linked_gnomad_variants} mapped variants to gnomAD variants.")
    logger.info(msg="Done linking gnomAD variants to mapped variants.", extra=job_manager.logging_context())
    return {"status": "ok", "data": {}, "exception": None}
