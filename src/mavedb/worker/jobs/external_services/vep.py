"""VEP functional consequence jobs for variant effect prediction.

This module links deduplicated alleles to their Ensembl VEP functional consequence. Submission is
batched against the VEP API, with a Variant Recoder fallback for HGVS strings VEP cannot resolve
directly (notably protein HGVS).
"""

import asyncio
import logging
import os
from datetime import date

from sqlalchemy import select

from mavedb.lib.annotation_status_manager import AnnotationStatusManager
from mavedb.lib.clingen.alleles import ScoreSetAlleleRow, get_alleles_for_score_set, group_alleles_for_annotation
from mavedb.lib.types.workflow import JobExecutionOutcome
from mavedb.lib.utils import batched
from mavedb.lib.vep import (
    VEP_CONSEQUENCES,
    get_ensembl_release,
    get_functional_consequence,
    link_vep_consequences_to_alleles,
    run_variant_recoder,
)
from mavedb.models.enums.annotation_type import AnnotationType
from mavedb.models.enums.job_pipeline import AnnotationFailureCategory, AnnotationStatus
from mavedb.models.score_set import ScoreSet
from mavedb.models.vep_allele_consequence import VepAlleleConsequence
from mavedb.worker.jobs.utils.setup import validate_job_params
from mavedb.worker.lib.decorators.pipeline_management import with_pipeline_management
from mavedb.worker.lib.managers.job_manager import JobManager

logger = logging.getLogger(__name__)

_VEP_BATCH_SIZE = 200
_RECODER_BATCH_SIZE = int(os.getenv("RECODER_BATCH_SIZE", "25"))
_RECODER_CONCURRENCY = int(os.getenv("RECODER_CONCURRENCY", "5"))


def _annotate_vep(
    annotation_manager: AnnotationStatusManager,
    variant_ids: list[int],
    status: AnnotationStatus,
    *,
    failure_category: AnnotationFailureCategory | None = None,
    error_message: str | None = None,
    metadata: dict | None = None,
) -> None:
    """Fan a VEP_FUNCTIONAL_CONSEQUENCE annotation out to every variant served by an allele.

    AAS migration seam: the single choke point for VEP's per-variant VAS writes. At migration it
    becomes an allele-keyed event writer; the per-variant fan-out goes away, and the variant
    association narrows to provenance (which variant's mapping drove the linkage). See
    docs/design/allele-annotation-status.md.

    VEP carries no version string, so the VAS row is written with ``version=None`` (unlike gnomAD,
    which keys on its data version).
    """
    annotation_data: dict = {"annotation_metadata": metadata or {}}
    if error_message is not None:
        annotation_data["error_message"] = error_message

    for variant_id in variant_ids:
        annotation_manager.add_annotation(
            variant_id=variant_id,
            annotation_type=AnnotationType.VEP_FUNCTIONAL_CONSEQUENCE,
            status=status,
            failure_category=failure_category,
            annotation_data=annotation_data,
            current=True,
        )


def _vep_hgvs_payload(row: ScoreSetAlleleRow) -> str | None:
    """Build VEP's HGVS input for an allele: each allele will only have one of these, so the first
    non-null is the one to submit.
    """
    return row.hgvs_g or row.hgvs_c or row.hgvs_p or None


async def _resolve_consequences(unique_hgvs: list[str], job_manager: JobManager) -> dict[str, str]:
    """Resolve a set of HGVS strings to their most-severe VEP consequence.

    Phase 1 submits the HGVS strings to VEP. Phase 2 runs Variant Recoder on the misses (a VEP entry
    with a null consequence is treated as a miss — VEP knew the variant but could not classify it).
    Phase 3 re-submits the recoded genomic strings to VEP and maps the most-severe consequence back to
    the original HGVS. Returns only the HGVS strings that resolved to a consequence; absent keys are
    failures the caller treats as no-result.
    """
    all_consequences: dict[str, str] = {}
    batches = list(batched(unique_hgvs, _VEP_BATCH_SIZE))

    # --- Phase 1: initial VEP pass ---
    all_missing_hgvs: set[str] = set()
    for batch_idx, batch in enumerate(batches):
        consequences = await get_functional_consequence(list(batch))
        hit_consequences = {h: c for h, c in consequences.items() if c is not None}
        all_consequences.update(hit_consequences)
        all_missing_hgvs.update(set(batch) - set(hit_consequences.keys()))

        job_manager.update_progress(
            int((batch_idx + 1) / len(batches) * 33),
            100,
            f"Processed initial VEP batch {batch_idx + 1}/{len(batches)}",
        )

    logger.info(
        msg=f"Completed initial VEP processing. {len(all_missing_hgvs)} HGVS strings require Variant Recoder fallback.",
        extra=job_manager.logging_context(),
    )

    if not all_missing_hgvs:
        return all_consequences

    # --- Phase 2: Variant Recoder fallback for HGVS strings VEP could not resolve ---
    recoder_batch_list = list(batched(list(all_missing_hgvs), _RECODER_BATCH_SIZE))
    semaphore = asyncio.Semaphore(_RECODER_CONCURRENCY)
    completed_recoder_batches = 0

    async def _recoder_with_semaphore(batch: list[str], total: int) -> dict[str, list[str]]:
        nonlocal completed_recoder_batches
        async with semaphore:
            result = await run_variant_recoder(batch)
            completed_recoder_batches += 1
            job_manager.update_progress(
                33 + int(completed_recoder_batches / total * 33),
                100,
                f"Completed Variant Recoder batch {completed_recoder_batches}/{total}",
            )
            return result

    total_recoder_batches = len(recoder_batch_list)
    recoder_results = await asyncio.gather(
        *[_recoder_with_semaphore(list(b), total_recoder_batches) for b in recoder_batch_list],
        return_exceptions=True,
    )

    first_exception = next((r for r in recoder_results if isinstance(r, BaseException)), None)
    if first_exception is not None:
        successful_batches = sum(1 for r in recoder_results if not isinstance(r, BaseException))
        logger.error(
            msg=f"Variant Recoder error ({successful_batches}/{total_recoder_batches} batches succeeded): {first_exception}",
            extra=job_manager.logging_context(),
        )
        raise first_exception

    hgvs_to_genomic: dict[str, list[str]] = {}
    for result in recoder_results:
        hgvs_to_genomic.update(result)  # type: ignore[arg-type]

    logger.info(
        msg=f"Completed Variant Recoder processing. {len(hgvs_to_genomic)} HGVS strings successfully recoded.",
        extra=job_manager.logging_context(),
    )

    # --- Phase 3: VEP pass on the recoded genomic HGVS strings ---
    all_recoded_genomic_hgvs = list({g for genomic_list in hgvs_to_genomic.values() for g in genomic_list})
    recoded_vep_batch_list = list(batched(all_recoded_genomic_hgvs, _VEP_BATCH_SIZE))
    all_recoded_consequences: dict[str, str | None] = {}

    for recoded_idx, recoded_batch in enumerate(recoded_vep_batch_list):
        all_recoded_consequences.update(await get_functional_consequence(list(recoded_batch)))
        job_manager.update_progress(
            66 + int((recoded_idx + 1) / len(recoded_vep_batch_list) * 33),
            100,
            f"Processed recoded VEP batch {recoded_idx + 1}/{len(recoded_vep_batch_list)}",
        )

    # Map the most-severe consequence from the recoded genomic strings back to the original HGVS.
    for original_hgvs, recoded_hgvs_list in hgvs_to_genomic.items():
        recoded_consequences = [c for h in recoded_hgvs_list if (c := all_recoded_consequences.get(h))]
        most_severe = next((c for c in VEP_CONSEQUENCES if c in recoded_consequences), None)
        if most_severe:
            all_consequences[original_hgvs] = most_severe

    return all_consequences


@with_pipeline_management
async def populate_vep_for_score_set(ctx: dict, job_id: int, job_manager: JobManager) -> JobExecutionOutcome:
    """Link deduplicated alleles to their VEP functional consequence.

    Runs over the score set's current alleles (authoritative and RT-derived), submits each allele's
    HGVS to VEP (with Variant Recoder fallback), and stores the most-severe consequence in a valid-time
    :class:`VepAlleleConsequence`, superseding only on change.

    Job Parameters:
        - score_set_id (int): The ID of the ScoreSet whose alleles to process.
        - correlation_id (str): Correlation ID for tracing requests across services.
        - force (bool, optional): Bypass the current-release skip and re-query every HGVS-bearing
          allele. The linker still supersedes only on a value change, so a forced re-run of unchanged
          data writes no new rows. Use for re-ingestion, to heal suspected corruption, or after editing
          the VEP_CONSEQUENCES severity ordering (a change the release version cannot see).

    Args:
        ctx (dict): The job context dictionary.
        job_id (int): The ID of the job being executed.
        job_manager (JobManager): Manager for job lifecycle and DB operations.

    Returns:
        JobExecutionOutcome: outcome with per-allele created/preexisting/skipped counts.
    """
    job = job_manager.get_job()

    _job_required_params = ["score_set_id", "correlation_id"]
    validate_job_params(_job_required_params, job)

    # Safely ignore mypy warnings here, as params were checked above.
    score_set = job_manager.db.scalars(select(ScoreSet).where(ScoreSet.id == job.job_params["score_set_id"])).one()  # type: ignore
    correlation_id = job.job_params["correlation_id"]  # type: ignore
    force = bool(job.job_params.get("force", False))  # type: ignore[union-attr]

    job_manager.save_to_context(
        {
            "application": "mavedb-worker",
            "function": "populate_vep_for_score_set",
            "resource": score_set.urn,
            "correlation_id": correlation_id,
        }
    )
    job_manager.update_progress(0, 100, "Starting VEP consequence linkage.")
    logger.info(msg="Started VEP consequence linkage", extra=job_manager.logging_context())

    # One work-unit per allele (payload = HGVS; alleles without one are skipped). Covers ALL the score
    # set's alleles — authoritative and RT-derived — since the genomic allele VEP is most reliable on
    # is often the RT-derived one; VAS still fans only to authoritative_variant_ids (the bandaid seam).
    allele_data = group_alleles_for_annotation(
        get_alleles_for_score_set(job_manager.db, score_set.id),
        payload=_vep_hgvs_payload,
    )

    num_alleles_with_hgvs = len(allele_data)
    job_manager.save_to_context({"num_alleles_to_link_vep": num_alleles_with_hgvs})

    if not allele_data:
        logger.warning(
            msg="No current alleles with HGVS were found for this score set. Skipping VEP linkage (nothing to do).",
            extra=job_manager.logging_context(),
        )
        job_manager.db.flush()
        return JobExecutionOutcome.succeeded(
            data={"created_allele_count": 0, "preexisting_allele_count": 0, "skipped_allele_count": 0}
        )

    all_allele_ids = set(allele_data.keys())

    # The Ensembl release version-keys the run (coordinated software + transcript set + vocabulary). It
    # is load-bearing for the skip below, so a failure here aborts the job rather than mis-versioning
    # writes (the exception propagates to the job decorators).
    ensembl_release = await get_ensembl_release()
    job_manager.save_to_context({"ensembl_release": ensembl_release})

    def alleles_at_current_release(allele_ids: set[int]) -> set[int]:
        """Allele ids (within the given set) holding a live VEP consequence at the current Ensembl release."""
        if not allele_ids:
            return set()
        return set(
            job_manager.db.scalars(
                select(VepAlleleConsequence.allele_id)
                .where(VepAlleleConsequence.allele_id.in_(allele_ids))
                .where(VepAlleleConsequence.current)
                .where(VepAlleleConsequence.functional_consequence.isnot(None))
                .where(VepAlleleConsequence.source_version == ensembl_release)
            ).all()
        )

    # Cost: skip alleles already resolved at the current Ensembl release (they cannot change without a
    # release bump). force re-queries all — including alleles unchanged upstream but whose VEP_CONSEQUENCES
    # severity ordering we have since edited; the linker still supersedes only on a value change, so a
    # forced no-op writes nothing.
    already_current = set() if force else alleles_at_current_release(all_allele_ids)
    hgvs_by_allele = {aid: allele_data[aid].payload for aid in allele_data if aid not in already_current}
    unique_hgvs = sorted(set(hgvs_by_allele.values()))
    job_manager.save_to_context(
        {
            "num_alleles_already_current": len(already_current),
            "num_hgvs_to_query": len(unique_hgvs),
            "force": force,
        }
    )

    changed_allele_ids: set[int] = set()
    if unique_hgvs:
        job_manager.update_progress(10, 100, f"Querying VEP for {len(unique_hgvs)} HGVS strings.")
        consequences_by_hgvs = await _resolve_consequences(unique_hgvs, job_manager)
        consequence_by_allele_id = {aid: consequences_by_hgvs.get(hgvs) for aid, hgvs in hgvs_by_allele.items()}
        changed_allele_ids = link_vep_consequences_to_alleles(
            job_manager.db, consequence_by_allele_id, source_version=ensembl_release, access_date=date.today()
        )
        job_manager.db.flush()
    else:
        logger.info(
            msg="All HGVS-bearing alleles are already resolved at the current Ensembl release; skipping VEP query.",
            extra=job_manager.logging_context(),
        )
        job_manager.update_progress(99, 100, "All alleles already current at this Ensembl release.")

    # Status is an audit event, written every run: SUCCESS/created if linked this run, SUCCESS/preexisting
    # if already current or re-confirmed unchanged, SKIPPED if no live consequence (VEP had no result).
    live_now = job_manager.db.execute(
        select(VepAlleleConsequence.allele_id, VepAlleleConsequence.functional_consequence)
        .where(VepAlleleConsequence.allele_id.in_(all_allele_ids))
        .where(VepAlleleConsequence.current)
        .where(VepAlleleConsequence.functional_consequence.isnot(None))
    ).all()
    consequence_now = {r.allele_id: r.functional_consequence for r in live_now}

    annotation_manager = AnnotationStatusManager(job_manager.db, job_run_id=job_manager.job_id)
    created_count = preexisting_count = skipped_count = 0
    for allele_id, entry in allele_data.items():
        if allele_id in consequence_now:
            action = "created" if allele_id in changed_allele_ids else "preexisting"
            if action == "created":
                created_count += 1
            else:
                preexisting_count += 1
            _annotate_vep(
                annotation_manager,
                entry.authoritative_variant_ids,
                AnnotationStatus.SUCCESS,
                metadata={
                    "functional_consequence": consequence_now[allele_id],
                    "hgvs": entry.payload,
                    "action": action,
                },
            )
        else:
            skipped_count += 1
            _annotate_vep(
                annotation_manager,
                entry.authoritative_variant_ids,
                AnnotationStatus.SKIPPED,
                failure_category=AnnotationFailureCategory.EXTERNAL_REFERENCE_NOT_FOUND,
                error_message="VEP could not determine a functional consequence for this allele, even after Variant Recoder fallback.",
                metadata={"hgvs": entry.payload},
            )

    annotation_manager.flush()

    outcome_data = {
        "created_allele_count": created_count,
        "preexisting_allele_count": preexisting_count,
        "skipped_allele_count": skipped_count,
    }
    job_manager.save_to_context(outcome_data)
    job_manager.update_progress(
        100,
        100,
        f"Completed VEP linkage: {created_count} created, {preexisting_count} preexisting, {skipped_count} skipped.",
    )
    logger.info(msg="Done linking VEP consequences to alleles.", extra=job_manager.logging_context())
    job_manager.db.flush()
    return JobExecutionOutcome.succeeded(data=outcome_data)
