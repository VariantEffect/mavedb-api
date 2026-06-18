"""ClinGen integration jobs for variant submission and linking.

This module contains jobs for submitting mapped variants to ClinGen services:
- ClinGen Allele Registry (CAR) for allele registration
- ClinGen Linked Data Hub (LDH) for data submission

These jobs enable integration with the ClinGen ecosystem for clinical
variant interpretation and data sharing.
"""

import asyncio
import functools
import logging
from dataclasses import dataclass, field

from sqlalchemy import select

from mavedb.lib.annotation_status_manager import AnnotationStatusManager
from mavedb.lib.clingen.alleles import get_alleles_for_score_set
from mavedb.lib.clingen.constants import (
    CAR_SUBMISSION_ENDPOINT,
    CLIN_GEN_SUBMISSION_ENABLED,
    DEFAULT_LDH_SUBMISSION_BATCH_SIZE,
    LDH_SUBMISSION_ENDPOINT,
)
from mavedb.lib.clingen.content_constructors import construct_ldh_submission
from mavedb.lib.clingen.services import (
    ClinGenAlleleRegistryService,
    ClinGenLdhService,
)
from mavedb.lib.types.workflow import JobExecutionOutcome
from mavedb.lib.variants import get_hgvs_from_post_mapped
from mavedb.models.allele import Allele as AlleleModel
from mavedb.models.enums.annotation_type import AnnotationType
from mavedb.models.enums.job_pipeline import AnnotationFailureCategory, AnnotationStatus, FailureCategory
from mavedb.models.mapping_record import MappingRecord
from mavedb.models.mapping_record_allele import MappingRecordAllele
from mavedb.models.score_set import ScoreSet
from mavedb.models.variant import Variant
from mavedb.worker.jobs.utils.setup import validate_job_params
from mavedb.worker.lib.decorators.pipeline_management import with_pipeline_management
from mavedb.worker.lib.managers.job_manager import JobManager

logger = logging.getLogger(__name__)


@dataclass
class _AlleleEntry:
    post_mapped: dict | None
    existing_caid: str | None
    # Variants for which THIS allele is the authoritative measurement — the only ones that receive a
    # per-variant VAS row. INTERIM BANDAID (do not deploy as final): keying clingen's per-variant
    # status to the single authoritative link sidesteps the multiple "current" rows a full allele
    # fan-out would write for one variant. Durable fix is an allele-level event log; rationale and
    # migration seam in docs/design/allele-annotation-status.md.
    authoritative_variant_ids: list[int] = field(default_factory=list)


def _annotate_caid(
    annotation_manager: AnnotationStatusManager,
    variant_ids: list[int],
    status: AnnotationStatus,
    *,
    failure_category: AnnotationFailureCategory | None = None,
    error_message: str | None = None,
    metadata: dict | None = None,
) -> None:
    """Fan a CLINGEN_ALLELE_ID annotation out to every variant served by an allele.

    AAS migration seam: the single choke point for clingen's per-variant VAS writes. At migration it
    becomes an allele-keyed event writer; the per-variant fan-out goes away, and the variant
    association narrows to provenance (who caused the registration). See
    docs/design/allele-annotation-status.md.
    """
    annotation_data: dict = {"annotation_metadata": metadata or {}}
    if error_message is not None:
        annotation_data["error_message"] = error_message

    for variant_id in variant_ids:
        annotation_manager.add_annotation(
            variant_id=variant_id,
            annotation_type=AnnotationType.CLINGEN_ALLELE_ID,
            version=None,
            status=status,
            failure_category=failure_category,
            annotation_data=annotation_data,
            current=True,
        )


@with_pipeline_management
async def submit_score_set_mappings_to_car(ctx: dict, job_id: int, job_manager: JobManager) -> JobExecutionOutcome:
    """
    Submit mapped variants for a score set to the ClinGen Allele Registry (CAR).

    This job registers mapped variants with CAR, assigns ClinGen Allele IDs (CAIDs),
    and updates the database with the results. Progress is tracked throughout the submission.

    Required job_params in the JobRun:
        - score_set_id (int): ID of the ScoreSet to process
        - correlation_id (str): Correlation ID for tracking

    Args:
        ctx (dict): Worker context containing DB and Redis connections
        job_manager (JobManager): Manager for job lifecycle and DB operations

    Side Effects:
        - Updates Allele records with ClinGen Allele IDs
        - Submits data to ClinGen Allele Registry

    Returns:
        JobExecutionOutcome: outcome with per-allele counts (submitted/registered/already-registered/failed).
    """
    # Get the job definition we are working on
    job = job_manager.get_job()

    _job_required_params = ["score_set_id", "correlation_id"]
    validate_job_params(_job_required_params, job)

    score_set = job_manager.db.scalars(select(ScoreSet).where(ScoreSet.id == job.job_params["score_set_id"])).one()  # type: ignore
    correlation_id = job.job_params["correlation_id"]  # type: ignore
    force_reregister = bool(job.job_params.get("force_reregister", False))  # type: ignore[union-attr]

    job_manager.save_to_context(
        {
            "application": "mavedb-worker",
            "function": "submit_score_set_mappings_to_car",
            "resource": score_set.urn,
            "correlation_id": correlation_id,
        }
    )
    job_manager.update_progress(0, 100, "Starting CAR mapped resource submission.")
    logger.info(msg="Started CAR mapped resource submission", extra=job_manager.logging_context())

    if not CLIN_GEN_SUBMISSION_ENABLED:
        logger.warning(
            msg="ClinGen submission is disabled via configuration, skipping submission of mapped variants to CAR.",
            extra=job_manager.logging_context(),
        )
        job_manager.db.flush()
        return JobExecutionOutcome.skipped(data={"reason": "ClinGen submission disabled"})

    if not CAR_SUBMISSION_ENDPOINT:
        logger.warning(
            msg="ClinGen Allele Registry submission is disabled (no submission endpoint), unable to complete submission of mapped variants to CAR.",
            extra=job_manager.logging_context(),
        )
        job_manager.db.flush()
        return JobExecutionOutcome.failed(
            reason="ClinGen Allele Registry submission endpoint is not configured.",
            failure_category=FailureCategory.CONFIGURATION_ERROR,
        )

    allele_rows = get_alleles_for_score_set(job_manager.db, score_set.id)
    job_manager.save_to_context({"total_allele_variant_pairs": len(allele_rows)})
    if not allele_rows:
        logger.warning(
            msg="No current alleles found for this score set. Skipping CAR submission.",
            extra=job_manager.logging_context(),
        )
        job_manager.db.flush()
        return JobExecutionOutcome.succeeded(
            data={
                "submitted_allele_count": 0,
                "registered_allele_count": 0,
                "already_registered_allele_count": 0,
                "failed_allele_count": 0,
            }
        )

    # Group by allele_id: one allele may serve multiple variants (cross-score-set dedup).
    allele_data: dict[int, _AlleleEntry] = {}
    for row in allele_rows:
        if row.allele_id not in allele_data:
            allele_data[row.allele_id] = _AlleleEntry(post_mapped=row.post_mapped, existing_caid=row.clingen_allele_id)

        if row.is_authoritative:
            allele_data[row.allele_id].authoritative_variant_ids.append(row.variant_id)

    annotation_manager = AnnotationStatusManager(job_manager.db, job_run_id=job_manager.job_id)

    # Track outcomes by distinct allele_id. clingen_allele_id is an allele-level fact (the CAID
    # lives on the Allele) and CAR's operation is per-allele, so the reported counts are in allele
    # units — and they cover every allele submitted, including the RT-derived ones that produce no
    # per-variant status row. Each allele has exactly one outcome (submitted once → one response), so
    # these sets are disjoint by construction. (Per-variant VAS rows are still written via the
    # authoritative link below — that is the interim bandaid, separate from these operation counts.)
    linked_allele_ids: set[int] = set()
    preexisting_allele_ids: set[int] = set()
    failed_allele_ids: set[int] = set()

    # Pre-existing CAIDs: record success without re-submitting unless force_reregister is set.
    preexisting = [aid for aid, entry in allele_data.items() if entry.existing_caid] if not force_reregister else []
    for allele_id in preexisting:
        entry = allele_data[allele_id]

        preexisting_allele_ids.add(allele_id)
        _annotate_caid(
            annotation_manager,
            entry.authoritative_variant_ids,
            AnnotationStatus.SUCCESS,
            metadata={"clingen_allele_id": entry.existing_caid, "registration_source": "preexisting"},
        )

    # Alleles that need CAR submission: new ones, or all when force_reregister=True.
    pending_allele_ids = [aid for aid, entry in allele_data.items() if force_reregister or not entry.existing_caid]
    job_manager.update_progress(10, 100, f"Preparing {len(pending_allele_ids)} alleles for CAR submission.")

    # Build HGVS → [allele_ids] map.  Multi-variant cis-phased blocks produce no HGVS
    # (combine_cis defaults to False); those alleles are annotated as failures immediately.
    hgvs_to_allele_ids: dict[str, list[int]] = {}
    for allele_id in pending_allele_ids:
        entry = allele_data[allele_id]
        hgvs = get_hgvs_from_post_mapped(entry.post_mapped)

        if hgvs:
            hgvs_to_allele_ids.setdefault(hgvs, []).append(allele_id)

        # Allele is registered but post_mapped can no longer produce HGVS — data
        # regression worth surfacing, but the CAID is still valid so treat it as
        # preexisting rather than failing the variant.
        elif entry.existing_caid:
            preexisting_allele_ids.add(allele_id)
            logger.warning(
                msg=(
                    f"Could not construct HGVS for allele {allele_id} during force re-registration "
                    f"(existing CAID: {entry.existing_caid!r}). Reconfirmation skipped; existing CAID retained."
                ),
                extra=job_manager.logging_context(),
            )
            _annotate_caid(
                annotation_manager,
                entry.authoritative_variant_ids,
                AnnotationStatus.SUCCESS,
                metadata={"clingen_allele_id": entry.existing_caid, "registration_source": "reconfirmation_skipped"},
            )

        # No HGVS-- un-submittable.
        else:
            failed_allele_ids.add(allele_id)
            logger.warning(
                msg=f"Could not construct HGVS for allele {allele_id}. Skipping CAR submission.",
                extra=job_manager.logging_context(),
            )
            _annotate_caid(
                annotation_manager,
                entry.authoritative_variant_ids,
                AnnotationStatus.FAILED,
                failure_category=AnnotationFailureCategory.MISSING_IDENTIFIER,
                error_message="Could not extract a valid HGVS string from post-mapped allele data.",
            )

    job_manager.save_to_context({"unique_hgvs_to_submit_car": len(hgvs_to_allele_ids)})

    # Distinct alleles actually sent to CAR this run (pending alleles that yielded HGVS).
    submitted_allele_ids: set[int] = {
        allele_id for allele_ids in hgvs_to_allele_ids.values() for allele_id in allele_ids
    }

    def _outcome_data() -> dict[str, int]:
        return {
            "submitted_allele_count": len(submitted_allele_ids),
            "registered_allele_count": len(linked_allele_ids),
            "already_registered_allele_count": len(preexisting_allele_ids),
            "failed_allele_count": len(failed_allele_ids),
        }

    # All pending alleles failed HGVS extraction; annotations already written above.
    if not hgvs_to_allele_ids:
        annotation_manager.flush()
        job_manager.db.flush()
        if preexisting_allele_ids:
            return JobExecutionOutcome.succeeded(data=_outcome_data())

        return JobExecutionOutcome.failed(
            reason=f"No submittable alleles for score set {score_set.urn}.",
            data=_outcome_data(),
            failure_category=FailureCategory.DEPENDENCY_FAILURE,
        )

    job_manager.update_progress(15, 100, "Submitting alleles to CAR.")
    car_service = ClinGenAlleleRegistryService(url=CAR_SUBMISSION_ENDPOINT)
    hgvs_list = list(hgvs_to_allele_ids.keys())
    registered_alleles = car_service.dispatch_submissions(hgvs_list)
    job_manager.update_progress(60, 100, "Processing registered alleles from CAR.")

    # Bulk-load every allele that could be linked in one query, keyed by id, rather than
    # issuing a SELECT per CAID inside the response loop.
    alleles_by_id = {
        allele.id: allele
        for allele in job_manager.db.scalars(select(AlleleModel).where(AlleleModel.id.in_(submitted_allele_ids))).all()
    }

    # CAR's contract is one result per submitted HGVS, in order — that is what makes the
    # positional zip below valid. A different count (including the empty list dispatch returns
    # on request failure) means alignment cannot be trusted for ANY position, so we register
    # nothing and fail the whole batch rather than risk writing a CAID to the wrong allele.
    aligned = len(registered_alleles) == len(hgvs_list)
    if not aligned:
        logger.error(
            msg=(
                f"CAR returned {len(registered_alleles)} results for {len(hgvs_list)} submitted HGVS; "
                "positional alignment cannot be trusted. Failing the entire batch."
            ),
            extra=job_manager.logging_context(),
        )

    for hgvs_string, response in zip(hgvs_list, registered_alleles if aligned else []):
        allele_ids_for_hgvs = hgvs_to_allele_ids[hgvs_string]

        if "errorType" in response:
            logger.warning(
                msg=f"CAR rejected HGVS '{hgvs_string}' ({response.get('errorType', 'unknown')}): {response.get('message', 'unknown')}",
                extra=job_manager.logging_context(),
            )
            for allele_id in allele_ids_for_hgvs:
                failed_allele_ids.add(allele_id)
                _annotate_caid(
                    annotation_manager,
                    allele_data[allele_id].authoritative_variant_ids,
                    AnnotationStatus.FAILED,
                    failure_category=AnnotationFailureCategory.EXTERNAL_SERVICE_REJECTED,
                    error_message="Failed to register allele with ClinGen Allele Registry.",
                    metadata={
                        "submitted_hgvs": hgvs_string,
                        "car_error_type": response.get("errorType"),
                        "car_error_message": response.get("message"),
                    },
                )

            continue

        # A response that is neither an error nor a registration (no "@id") is malformed.
        caid_iri = response.get("@id")
        if not caid_iri:
            logger.error(
                msg=f"CAR returned a response for HGVS '{hgvs_string}' with neither an error nor an allele identifier.",
                extra=job_manager.logging_context(),
            )
            for allele_id in allele_ids_for_hgvs:
                failed_allele_ids.add(allele_id)
                _annotate_caid(
                    annotation_manager,
                    allele_data[allele_id].authoritative_variant_ids,
                    AnnotationStatus.FAILED,
                    failure_category=AnnotationFailureCategory.EXTERNAL_SERVICE_REJECTED,
                    error_message="ClinGen Allele Registry returned a malformed response with no allele identifier.",
                    metadata={"submitted_hgvs": hgvs_string},
                )

            continue

        caid = caid_iri.split("/")[-1]
        for allele_id in allele_ids_for_hgvs:
            entry = allele_data[allele_id]
            prior_caid = entry.existing_caid

            # CAID is immutable — a different value returned by CAR is a hard invariant
            # violation.  Do not overwrite; record a failure with full audit context.
            if prior_caid and prior_caid != caid:
                logger.error(
                    msg=(
                        f"CAR returned a different CAID for allele {allele_id}: "
                        f"stored={prior_caid!r}, returned={caid!r}. "
                        "Not overwriting. Investigate immediately."
                    ),
                    extra=job_manager.logging_context(),
                )

                failed_allele_ids.add(allele_id)
                _annotate_caid(
                    annotation_manager,
                    entry.authoritative_variant_ids,
                    AnnotationStatus.FAILED,
                    failure_category=AnnotationFailureCategory.EXTERNAL_SERVICE_REJECTED,
                    error_message="CAR returned a CAID that conflicts with the stored value.",
                    metadata={
                        "clingen_allele_id": prior_caid,
                        "conflicting_caid": caid,
                        "submitted_hgvs": hgvs_string,
                    },
                )

            # CAID is new or matches the stored value — link it to the allele and record success.
            else:
                linked_allele_ids.add(allele_id)
                allele = alleles_by_id[allele_id]
                allele.clingen_allele_id = caid

                registration_source = "reconfirmed" if prior_caid else "this_run"
                if prior_caid:
                    logger.info(
                        msg=f"Force re-registration confirmed same CAID {caid!r} for allele {allele_id}.",
                        extra=job_manager.logging_context(),
                    )

                _annotate_caid(
                    annotation_manager,
                    entry.authoritative_variant_ids,
                    AnnotationStatus.SUCCESS,
                    metadata={"clingen_allele_id": caid, "registration_source": registration_source},
                )

    # Submitted HGVS with no trustworthy response: the truncated tail when the counts line up
    # (network drop, service-side omission), or the entire batch when the response count
    # violated CAR's one-result-per-input contract and we rejected it above.
    unattributed_hgvs = hgvs_list if not aligned else hgvs_list[len(registered_alleles) :]
    for hgvs_string in unattributed_hgvs:
        for allele_id in hgvs_to_allele_ids[hgvs_string]:
            failed_allele_ids.add(allele_id)
            _annotate_caid(
                annotation_manager,
                allele_data[allele_id].authoritative_variant_ids,
                AnnotationStatus.FAILED,
                failure_category=AnnotationFailureCategory.EXTERNAL_API_ERROR,
                error_message="Failed to register allele with ClinGen Allele Registry.",
                metadata={"submitted_hgvs": hgvs_string},
            )

    annotation_manager.flush()

    outcome_data = _outcome_data()

    # When no allele ended up with a CAID (none linked this run, none already registered), the
    # pipeline cannot continue — downstream jobs need CAIDs to function.
    if not linked_allele_ids and not preexisting_allele_ids:
        error_message = (
            f"CAR submission failed for all {outcome_data['submitted_allele_count']} "
            f"submitted alleles in score set {score_set.urn}."
        )
        logger.error(msg=error_message, extra=job_manager.logging_context())
        job_manager.db.flush()
        return JobExecutionOutcome.failed(
            reason=error_message,
            data=outcome_data,
            failure_category=FailureCategory.DEPENDENCY_FAILURE,
        )

    if outcome_data["failed_allele_count"] > 0:
        logger.warning(
            msg=f"CAR submission failed for {outcome_data['failed_allele_count']} alleles in score set {score_set.urn}.",
            extra=job_manager.logging_context(),
        )

    logger.info(msg="Completed CAR mapped resource submission", extra=job_manager.logging_context())
    job_manager.db.flush()
    return JobExecutionOutcome.succeeded(data=outcome_data)


@with_pipeline_management
async def submit_score_set_mappings_to_ldh(ctx: dict, job_id: int, job_manager: JobManager) -> JobExecutionOutcome:
    """
    Submit mapped variants for a score set to the ClinGen Linked Data Hub (LDH).

    This job submits mapped variant data to LDH for a given score set, handling authentication,
    submission batching, and error reporting. Progress and errors are logged and reported to Slack.

    Required job_params in the JobRun:
        - score_set_id (int): ID of the ScoreSet to process
        - correlation_id (str): Correlation ID for tracking

    Args:
        ctx (dict): Worker context containing DB and Redis connections
        job_manager (JobManager): Manager for job lifecycle and DB operations

    Side Effects:
        - Submits data to ClinGen Linked Data Hub

    Returns:
        JobExecutionOutcome: outcome with per-variant submitted/failed counts.
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
            "function": "submit_score_set_mappings_to_ldh",
            "resource": score_set.urn,
            "correlation_id": correlation_id,
        }
    )
    job_manager.update_progress(0, 100, "Starting LDH mapped resource submission.")
    logger.info(msg="Started LDH mapped resource submission", extra=job_manager.logging_context())

    # Connect to LDH service
    ldh_service = ClinGenLdhService(url=LDH_SUBMISSION_ENDPOINT)
    ldh_service.authenticate()

    # Fetch each variant's authoritative allele for the score set. Post-mapped data and HGVS
    # come from the Allele; pre-mapped data and the mapping API version come from the
    # MappingRecord. RT-derived equivalence alleles are intentionally excluded — LDH links each
    # MaveDB score to its canonical mapped variant, not to every equivalent allele (unlike CAR,
    # which registers a CAID per allele).
    variant_objects = job_manager.db.execute(
        select(Variant, MappingRecord, AlleleModel)
        .join(MappingRecord, MappingRecord.variant_id == Variant.id)
        .join(MappingRecordAllele, MappingRecordAllele.mapping_record_id == MappingRecord.id)
        .join(AlleleModel, AlleleModel.id == MappingRecordAllele.allele_id)
        .where(Variant.score_set_id == score_set.id)
        .where(MappingRecord.current)
        .where(MappingRecordAllele.current)
        .where(MappingRecordAllele.is_authoritative.is_(True))
        .where(AlleleModel.post_mapped.is_not(None))
    ).all()

    # Track total variants to submit
    job_manager.save_to_context({"total_variants_to_submit_ldh": len(variant_objects)})
    if not variant_objects:
        logger.warning(
            msg="No current mapped variants with post mapped metadata were found for this score set. Skipping LDH submission.",
            extra=job_manager.logging_context(),
        )
        job_manager.db.flush()
        return JobExecutionOutcome.succeeded(data={"submitted_count": 0, "failed_count": 0})

    job_manager.update_progress(10, 100, f"Submitting {len(variant_objects)} mapped variants to LDH.")

    # Build submission content
    variant_content = []
    variant_for_urn = {}
    for variant, mapping_record, allele in variant_objects:
        # See the note above: cis-phased blocks are skipped here pending ClinGen guidance
        # (https://github.com/VariantEffect/mavedb-api/issues/764).
        variation = get_hgvs_from_post_mapped(allele.post_mapped)

        if not variation:
            logger.warning(
                msg=f"Could not construct a valid HGVS string for allele {allele.id} (variant {variant.urn}). Skipping submission of this variant.",
                extra=job_manager.logging_context(),
            )
            continue

        variant_content.append((variation, variant, mapping_record, allele))
        variant_for_urn[variant.urn] = variant

    if not variant_content:
        logger.warning(
            msg="No valid mapped variants with post mapped metadata were found for this score set. Skipping LDH submission.",
            extra=job_manager.logging_context(),
        )
        job_manager.db.flush()
        return JobExecutionOutcome.succeeded(data={"submitted_count": 0, "failed_count": 0})

    job_manager.save_to_context({"unique_variants_to_submit_ldh": len(variant_content)})
    job_manager.update_progress(30, 100, f"Dispatching submissions for {len(variant_content)} unique variants to LDH.")
    submission_content = construct_ldh_submission(variant_content)

    blocking = functools.partial(
        ldh_service.dispatch_submissions, submission_content, DEFAULT_LDH_SUBMISSION_BATCH_SIZE
    )
    loop = asyncio.get_running_loop()
    submission_successes, submission_failures = await loop.run_in_executor(ctx["pool"], blocking)
    job_manager.update_progress(90, 100, "Finalizing LDH mapped resource submission.")
    job_manager.save_to_context(
        {
            "ldh_submission_successes": len(submission_successes),
            "ldh_submission_failures": len(submission_failures),
        }
    )

    # TODO prior to finalizing: Verify typing of ClinGen submission responses. See https://reg.clinicalgenome.org/doc/AlleleRegistry_1.01.xx_api_v1.pdf
    annotation_manager = AnnotationStatusManager(job_manager.db, job_run_id=job_manager.job_id)
    submitted_variant_urns = set()
    for success in submission_successes:
        logger.debug(
            msg=f"Successfully submitted mapped variant to LDH: {success}",
            extra=job_manager.logging_context(),
        )

        submitted_urn = success["data"]["entId"]
        submitted_variant = variant_for_urn.get(submitted_urn)
        if submitted_variant is None:
            # LDH echoed back an entId we never submitted — record it for investigation rather
            # than crashing the whole job mid-batch.
            logger.warning(
                msg=f"LDH returned an unrecognized entId not in this submission: {submitted_urn!r}.",
                extra=job_manager.logging_context(),
            )
            continue

        annotation_manager.add_annotation(
            variant_id=submitted_variant.id,
            annotation_type=AnnotationType.LDH_SUBMISSION,
            version=None,
            status=AnnotationStatus.SUCCESS,
            annotation_data={
                "annotation_metadata": {"ldh_iri": success["data"]["ldhIri"], "ldh_id": success["data"]["ldhId"]},
            },
            current=True,
        )
        submitted_variant_urns.add(submitted_urn)

    # It isn't trivial to map individual failures back to their corresponding variants,
    # especially when submission occurred in batch. Save all failures generically here.
    # Note that failures may not be present in the submission failures list, but they are
    # guaranteed to be absent from the successes list.
    failed_variant_urns = set(variant_for_urn.keys()) - submitted_variant_urns
    for failure_urn in failed_variant_urns:
        logger.error(
            msg=f"Failed to submit mapped variant to LDH: {failure_urn}",
            extra=job_manager.logging_context(),
        )

        failed_variant = variant_for_urn[failure_urn]

        annotation_manager.add_annotation(
            variant_id=failed_variant.id,
            annotation_type=AnnotationType.LDH_SUBMISSION,
            version=None,
            status=AnnotationStatus.FAILED,
            failure_category=AnnotationFailureCategory.EXTERNAL_API_ERROR,
            annotation_data={
                "error_message": "Failed to submit variant to ClinGen Linked Data Hub.",
            },
            current=True,
        )

    annotation_manager.flush()

    # Report per-variant counts (matching the annotations written above), not the per-batch
    # counts returned by the service — the two use different denominators.
    submitted_count = len(submitted_variant_urns)
    failed_count = len(failed_variant_urns)

    if submission_failures:
        logger.warning(
            msg=f"LDH mapped resource submission encountered {len(submission_failures)} batch failures "
            f"({failed_count} variants unconfirmed).",
            extra=job_manager.logging_context(),
        )

        if not submission_successes:
            error_message = f"All LDH submissions failed for score set {score_set.urn}."
            logger.error(
                msg=error_message,
                extra=job_manager.logging_context(),
            )

            job_manager.db.flush()
            return JobExecutionOutcome.failed(
                reason=error_message,
                data={"submitted_count": submitted_count, "failed_count": failed_count},
                failure_category=FailureCategory.DEPENDENCY_FAILURE,
            )

    logger.info(
        msg="Completed LDH mapped resource submission",
        extra=job_manager.logging_context(),
    )

    job_manager.db.flush()
    return JobExecutionOutcome.succeeded(data={"submitted_count": submitted_count, "failed_count": failed_count})
