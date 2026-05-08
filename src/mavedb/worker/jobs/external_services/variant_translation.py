"""ClinGen allele variant translation jobs for mapping PA<->CA allele relationships.

This module populates the variant_translations table with relationships between
protein allele (PA) and nucleotide allele (CA) ClinGen IDs. For CA alleles, it
looks up MANE canonical PA IDs and their matching registered transcript CA IDs.
For PA alleles, it looks up matching registered transcript CA IDs directly.
"""

import logging

import requests
from sqlalchemy import select

from mavedb.lib.annotation_status_manager import AnnotationStatusManager
from mavedb.lib.clingen.allele_registry import (
    expand_allele_ids,
    get_canonical_pa_ids,
    get_matching_registered_ca_ids,
)
from mavedb.lib.types.workflow import JobExecutionOutcome
from mavedb.lib.variant_translations import upsert_variant_translations
from mavedb.models.enums.annotation_type import AnnotationType
from mavedb.models.enums.job_pipeline import AnnotationFailureCategory, AnnotationStatus, FailureCategory
from mavedb.models.mapped_variant import MappedVariant
from mavedb.models.score_set import ScoreSet
from mavedb.models.variant import Variant
from mavedb.worker.jobs.utils.setup import validate_job_params
from mavedb.worker.lib.decorators.pipeline_management import with_pipeline_management
from mavedb.worker.lib.managers.job_manager import JobManager

logger = logging.getLogger(__name__)


@with_pipeline_management
async def populate_variant_translations_for_score_set(
    ctx: dict, job_id: int, job_manager: JobManager
) -> JobExecutionOutcome:
    """Populate variant translations (PA<->CA relationships) for a score set.

    Queries the ClinGen Allele Registry to discover relationships between protein
    allele (PA) and nucleotide allele (CA) ClinGen IDs, then stores them in the
    variant_translations table. Each unique allele ID is processed once even if
    shared across multiple mapped variants.

    Required job_params in the JobRun:
        - score_set_id (int): ID of the ScoreSet to process
        - correlation_id (str): Correlation ID for tracking
    """
    job = job_manager.get_job()

    _job_required_params = ["score_set_id", "correlation_id"]
    validate_job_params(_job_required_params, job)

    score_set = job_manager.db.scalars(select(ScoreSet).where(ScoreSet.id == job.job_params["score_set_id"])).one()  # type: ignore
    correlation_id = job.job_params["correlation_id"]  # type: ignore

    job_manager.save_to_context(
        {
            "application": "mavedb-worker",
            "function": "populate_variant_translations_for_score_set",
            "resource": score_set.urn,
            "correlation_id": correlation_id,
        }
    )
    job_manager.update_progress(0, 100, "Starting variant translation population.")
    logger.info(msg="Started variant translation population.", extra=job_manager.logging_context())

    # Fetch all current mapped variants with their ClinGen allele IDs
    variant_rows = job_manager.db.execute(
        select(Variant.id, MappedVariant.clingen_allele_id)
        .join(MappedVariant, MappedVariant.variant_id == Variant.id)
        .join(ScoreSet, Variant.score_set_id == ScoreSet.id)
        .where(ScoreSet.id == score_set.id)
        .where(MappedVariant.current.is_(True))
    ).all()

    if not variant_rows:
        logger.warning(
            msg="No current mapped variants found for this score set.",
            extra=job_manager.logging_context(),
        )
        job_manager.db.flush()
        return JobExecutionOutcome.succeeded(
            data={"translations_created": 0, "alleles_skipped": 0, "alleles_failed": 0}
        )

    # Deduplicate: multiple mapped variants can share the same allele ID, but we only
    # need to query the ClinGen API once per unique ID. Track which variants map to each
    # allele so we can record annotations for all of them after a single lookup.
    allele_to_variants: dict[str, list[int]] = {}
    for variant_id, clingen_allele_id in variant_rows:
        if not clingen_allele_id:
            continue

        for individual_id in expand_allele_ids([clingen_allele_id]):
            allele_to_variants.setdefault(individual_id, []).append(variant_id)

    unique_allele_ids = list(allele_to_variants.keys())
    total_alleles = len(unique_allele_ids)
    job_manager.save_to_context({"total_variants": len(variant_rows), "unique_allele_ids": total_alleles})

    if not unique_allele_ids:
        logger.warning(
            msg="No ClinGen allele IDs found on mapped variants.",
            extra=job_manager.logging_context(),
        )
        job_manager.db.flush()
        return JobExecutionOutcome.succeeded(
            data={"translations_created": 0, "alleles_skipped": 0, "alleles_failed": 0}
        )

    job_manager.update_progress(5, 100, f"Processing {total_alleles} unique allele IDs for variant translations.")
    logger.info(
        "Processing %s unique allele IDs for variant translations.",
        total_alleles,
        extra=job_manager.logging_context(),
    )

    total_created = 0
    total_skipped = 0
    total_failed = 0
    annotation_manager = AnnotationStatusManager(job_manager.db, job_run_id=job_manager.job_id)

    for index, allele_id in enumerate(unique_allele_ids):
        if total_alleles > 0 and index % max(total_alleles // 20, 1) == 0:
            progress = 5 + int((index / total_alleles) * 90)
            job_manager.update_progress(progress, 100, f"Processing allele {index + 1}/{total_alleles}.")
            logger.info(
                "Processing allele %s/%s: %s",
                index + 1,
                total_alleles,
                allele_id,
                extra=job_manager.logging_context(),
            )

        job_manager.save_to_context(
            {
                "current_allele_id": allele_id,
                "progress_index": index,
            }
        )

        variant_ids = allele_to_variants[allele_id]

        if allele_id.startswith("CA"):
            # CA (nucleotide) alleles: look up the MANE canonical protein alleles (PAs) for
            # this CA, then for each PA discover all registered transcript-level CAs. This
            # CA -> PA -> CA expansion builds the full translation graph so we can link
            # nucleotide variants to their protein equivalents and vice versa.
            try:
                canonical_pa_ids = await get_canonical_pa_ids(allele_id)
            except requests.exceptions.RequestException as exc:
                logger.error(
                    "ClinGen API request failed for canonical PA lookup of %s.",
                    allele_id,
                    extra=job_manager.logging_context(),
                    exc_info=exc,
                )
                for vid in variant_ids:
                    annotation_manager.add_annotation(
                        variant_id=vid,
                        annotation_type=AnnotationType.VARIANT_TRANSLATION,
                        version=None,
                        status=AnnotationStatus.FAILED,
                        failure_category=AnnotationFailureCategory.EXTERNAL_API_ERROR,
                        annotation_data={
                            "error_message": f"ClinGen API error looking up PA IDs for {allele_id}: {exc}",
                        },
                        current=True,
                    )
                total_failed += len(variant_ids)
                continue

            if not canonical_pa_ids:
                # Noncoding variants won't have protein alleles — this is expected and not an error.
                logger.debug(
                    "No canonical PA IDs found for %s (may be noncoding).",
                    allele_id,
                    extra=job_manager.logging_context(),
                )
                for vid in variant_ids:
                    annotation_manager.add_annotation(
                        variant_id=vid,
                        annotation_type=AnnotationType.VARIANT_TRANSLATION,
                        version=None,
                        status=AnnotationStatus.SKIPPED,
                        failure_category=AnnotationFailureCategory.NO_LINKED_ALLELE,
                        annotation_data={
                            "error_message": f"No canonical PA IDs for {allele_id}.",
                        },
                        current=True,
                    )
                total_skipped += len(variant_ids)
                continue

            created = 0
            failed = 0
            translation_pairs: set[tuple[str, str]] = set()
            for pa_id in canonical_pa_ids:
                # Record the direct PA <-> original CA relationship.
                translation_pairs.add((pa_id, allele_id))

                # Then expand: find all other CAs registered under this PA so we capture
                # alternate transcript-level representations of the same protein change.
                try:
                    ca_ids = await get_matching_registered_ca_ids(pa_id)
                except requests.exceptions.RequestException as exc:
                    logger.error(
                        "ClinGen API request failed for registered CA lookup of %s.",
                        pa_id,
                        extra=job_manager.logging_context(),
                        exc_info=exc,
                    )
                    failed += 1
                    continue

                for ca_id in ca_ids:
                    translation_pairs.add((pa_id, ca_id))

            created, existing = upsert_variant_translations(job_manager.db, list(translation_pairs))
            for vid in variant_ids:
                annotation_manager.add_annotation(
                    variant_id=vid,
                    annotation_type=AnnotationType.VARIANT_TRANSLATION,
                    version=None,
                    status=AnnotationStatus.FAILED if failed > 0 else AnnotationStatus.SUCCESS,
                    annotation_data={
                        "annotation_metadata": {
                            "allele_id": allele_id,
                            "translation_pairs": [[pa, ca] for pa, ca in translation_pairs],
                            "translations_new": created,
                            "translations_existing": existing,
                            "pa_lookups_failed": failed,
                            "pa_lookups_total": len(canonical_pa_ids),
                        },
                    },
                    current=True,
                )

            total_created += created
            total_failed += failed

        elif allele_id.startswith("PA"):
            # PA (protein) alleles: directly look up all registered transcript-level CAs.
            # This is simpler than the CA path since we already have the protein allele.
            try:
                ca_ids = await get_matching_registered_ca_ids(allele_id)
            except requests.exceptions.RequestException as exc:
                logger.error(
                    "ClinGen API request failed for registered CA lookup of %s.",
                    allele_id,
                    extra=job_manager.logging_context(),
                    exc_info=exc,
                )
                for vid in variant_ids:
                    annotation_manager.add_annotation(
                        variant_id=vid,
                        annotation_type=AnnotationType.VARIANT_TRANSLATION,
                        version=None,
                        status=AnnotationStatus.FAILED,
                        failure_category=AnnotationFailureCategory.EXTERNAL_API_ERROR,
                        annotation_data={
                            "error_message": f"ClinGen API error for {allele_id}: {exc}",
                        },
                        current=True,
                    )
                total_failed += len(variant_ids)
                continue

            if not ca_ids:
                logger.warning(
                    "No matching registered transcript CA IDs for PA allele %s. This is unexpected.",
                    allele_id,
                    extra=job_manager.logging_context(),
                )
                for vid in variant_ids:
                    annotation_manager.add_annotation(
                        variant_id=vid,
                        annotation_type=AnnotationType.VARIANT_TRANSLATION,
                        version=None,
                        status=AnnotationStatus.SKIPPED,
                        failure_category=AnnotationFailureCategory.NO_LINKED_ALLELE,
                        annotation_data={
                            "error_message": f"No registered transcript CA IDs for {allele_id}.",
                        },
                        current=True,
                    )
                total_skipped += len(variant_ids)
                continue

            translation_pairs = set([(allele_id, ca_id) for ca_id in ca_ids])
            created, existing = upsert_variant_translations(job_manager.db, list(translation_pairs))
            for vid in variant_ids:
                annotation_manager.add_annotation(
                    variant_id=vid,
                    annotation_type=AnnotationType.VARIANT_TRANSLATION,
                    version=None,
                    status=AnnotationStatus.SUCCESS,
                    annotation_data={
                        "annotation_metadata": {
                            "allele_id": allele_id,
                            "translation_pairs": [[pa, ca] for pa, ca in translation_pairs],
                            "translations_new": created,
                            "translations_existing": existing,
                        },
                    },
                    current=True,
                )

            total_created += created

        else:
            logger.warning(
                "Unrecognized ClinGen allele ID format: %s. Skipping.",
                allele_id,
                extra=job_manager.logging_context(),
            )
            for vid in variant_ids:
                annotation_manager.add_annotation(
                    variant_id=vid,
                    annotation_type=AnnotationType.VARIANT_TRANSLATION,
                    version=None,
                    status=AnnotationStatus.SKIPPED,
                    failure_category=AnnotationFailureCategory.UNSUPPORTED_IDENTIFIER,
                    annotation_data={
                        "error_message": f"Unrecognized allele ID format: {allele_id}",
                    },
                    current=True,
                )
            total_skipped += len(variant_ids)

    annotation_manager.flush()
    job_manager.db.flush()

    job_manager.save_to_context(
        {
            "translations_created": total_created,
            "alleles_skipped": total_skipped,
            "alleles_failed": total_failed,
        }
    )
    logger.info(
        "Completed variant translation population: %s created, %s skipped, %s failed.",
        total_created,
        total_skipped,
        total_failed,
        extra=job_manager.logging_context(),
    )

    if total_failed > 0 and total_created == 0:
        error_message = f"All {total_failed} variant translation lookups failed for score set {score_set.urn}. Possible ClinGen API outage."
        logger.error(error_message, extra=job_manager.logging_context())
        job_manager.db.flush()
        return JobExecutionOutcome.failed(
            reason=error_message,
            data={
                "translations_created": 0,
                "alleles_skipped": total_skipped,
                "alleles_failed": total_failed,
            },
            failure_category=FailureCategory.DEPENDENCY_FAILURE,
        )

    job_manager.db.flush()
    return JobExecutionOutcome.succeeded(
        data={
            "translations_created": total_created,
            "alleles_skipped": total_skipped,
            "alleles_failed": total_failed,
        }
    )
