"""Variant mapping jobs using VRS (Variant Representation Specification).

This module handles the mapping of variants to standardized genomic coordinates
using the VRS mapping service. It includes queue management, retry logic,
and coordination with downstream services like ClinGen and UniProt.
"""

import asyncio
import contextlib
import functools
import logging
from collections import Counter
from datetime import date, datetime, timezone
from typing import Any

from sqlalchemy import cast, func, null, select
from sqlalchemy.dialects.postgresql import JSONB

from mavedb.data_providers.services import vrs_mapper
from mavedb.lib.annotation_status_manager import AnnotationStatusManager
from mavedb.lib.exceptions import (
    NonexistentMappingReferenceError,
    NonexistentMappingResultsError,
    NonexistentMappingScoresError,
)
from mavedb.lib.logging.context import format_raised_exception_info_as_dict
from mavedb.lib.mapping import EXCLUDED_PREMAPPED_ANNOTATION_KEYS
from mavedb.lib.mapping.schema import MappingOutcome
from mavedb.lib.types.workflow import JobExecutionOutcome
from mavedb.lib.variant_translations import get_or_create_allele
from mavedb.lib.variants import get_hgvs_from_post_mapped
from mavedb.lib.vrs_utils import canonical_variation_document
from mavedb.models.allele import Allele as AlleleDbModel
from mavedb.models.enums.annotation_type import AnnotationType
from mavedb.models.enums.disposition import Disposition
from mavedb.models.enums.job_pipeline import FailureCategory
from mavedb.models.enums.mapping_state import MappingState
from mavedb.models.enums.sequence_level import SequenceLevel
from mavedb.models.mapping_record import MappingRecord
from mavedb.models.mapping_record_allele import MappingRecordAllele
from mavedb.models.score_set import ScoreSet
from mavedb.models.target_gene_mapping import TargetGeneMapping
from mavedb.models.user import User
from mavedb.models.variant import Variant
from mavedb.worker.jobs.utils.setup import validate_job_params
from mavedb.worker.lib.decorators.pipeline_management import with_pipeline_management
from mavedb.worker.lib.managers.job_manager import JobManager

logger = logging.getLogger(__name__)

# The score-set map is a single opaque blocking call to dcd-mapping: it cannot refresh the progress
# heartbeat mid-call, so `cleanup_stalled_jobs` (PROGRESS_STALL_MINUTES = 30) would reap a live worker
# on any set that maps for longer than that. Two guards bracket the call — a liveness keepalive that
# refreshes the heartbeat on a fixed cadence, and a size-aware wall-clock budget that bounds a wedged
# mapper to a sensible window instead of the 23.5h stall backstop. This is an explicit workaround for
# delegating opaque work; the real fix is for the mapper to report progress (or to chunk the call).
# See worker/best_practices.md, "Long external delegations". TODO: replace with real mapping progress.
MAP_PROGRESS_KEEPALIVE_SECONDS = 300  # heartbeat cadence; must stay well under PROGRESS_STALL_MINUTES (30m)
MAP_BUDGET_BASE_SECONDS = 30 * 60  # floor: overhead + small sets
MAP_BUDGET_PER_VARIANT_SECONDS = 1.0  # ~3x the observed ~0.3 s/variant map throughput, as slack
MAP_BUDGET_MAX_SECONDS = 22 * 60 * 60  # ceiling: stay under the 23.5h stall backstop / 24h ARQ ceiling


async def _keepalive_progress(job_manager: JobManager, interval_seconds: int) -> None:
    """Refresh the job's progress heartbeat on a fixed cadence during an opaque, un-checkpointable call.

    Liveness, not progress: the task runs in the worker's event loop, so it dies with a crashed worker
    and the heartbeat then correctly goes stale — only a *live* worker in a long delegation is kept from
    being falsely reaped. Best-effort; a failed refresh is logged and the loop continues.
    """
    while True:
        await asyncio.sleep(interval_seconds)
        try:
            job_manager.update_progress(
                30,
                100,
                f"Mapping variants using VRS mapping service (in progress, {datetime.now(timezone.utc):%H:%M:%SZ}).",
            )
        except Exception:  # noqa: BLE001 - the heartbeat is best-effort; a dropped tick is harmless
            logger.warning("Mapping progress keepalive tick failed; continuing.", extra=job_manager.logging_context())


def _map_budget_seconds(variant_count: int) -> int:
    """Total wall-clock budget for the opaque map call, sized to the score set.

    Bounds a wedged external mapper to a size-appropriate window instead of the 23.5h stall backstop,
    while leaving enough slack (the per-variant term is ~3x observed throughput) not to fail a slow but
    healthy map. Clamped to a ceiling below the ARQ job timeout.
    """
    budget = MAP_BUDGET_BASE_SECONDS + int(variant_count * MAP_BUDGET_PER_VARIANT_SECONDS)
    return min(budget, MAP_BUDGET_MAX_SECONDS)


@with_pipeline_management
async def map_variants_for_score_set(ctx: dict, job_id: int, job_manager: JobManager) -> JobExecutionOutcome:
    """Map variants for a given score set using VRS."""
    # Handle everything prior to score set fetch in an outer layer. Any issues prior to
    # fetching the score set should fail the job outright and we will be unable to set
    # a processing state on the score set itself.

    job = job_manager.get_job()

    _job_required_params = [
        "score_set_id",
        "correlation_id",
        "updater_id",
    ]
    validate_job_params(_job_required_params, job)

    # Fetch required resources based on param inputs. Safely ignore mypy warnings here, as they were checked above.
    score_set = job_manager.db.scalars(select(ScoreSet).where(ScoreSet.id == job.job_params["score_set_id"])).one()  # type: ignore

    # Handle everything within try/except to persist appropriate mapping state
    try:
        correlation_id = job.job_params["correlation_id"]  # type: ignore
        updater_id = job.job_params["updater_id"]  # type: ignore
        updated_by = job_manager.db.scalars(select(User).where(User.id == updater_id)).one()

        # Setup initial context and progress
        job_manager.save_to_context(
            {
                "application": "mavedb-worker",
                "function": "map_variants_for_score_set",
                "resource": score_set.urn,
                "correlation_id": correlation_id,
            }
        )
        job_manager.update_progress(0, 100, "Starting variant mapping job.")
        logger.info(msg="Started variant mapping job", extra=job_manager.logging_context())

        # TODO#372: non-nullable URNs
        if not score_set.urn:  # pragma: no cover
            raise ValueError("Score set URN is required for variant mapping.")

        # Setup score set state for mapping
        score_set.mapping_state = MappingState.processing
        score_set.mapping_errors = null()
        score_set.modified_by = updated_by
        score_set.modification_date = date.today()

        job_manager.db.add(score_set)
        job_manager.db.flush()

        job_manager.save_to_context({"mapping_state": score_set.mapping_state.name})
        job_manager.update_progress(10, 100, "Score set prepared for variant mapping.")
        logger.debug(msg="Score set prepared for variant mapping.", extra=job_manager.logging_context())

        # Do not block Worker event loop during mapping, see: https://arq-docs.helpmanual.io/#synchronous-jobs.
        vrs = vrs_mapper()
        blocking = functools.partial(vrs.map_score_set, score_set.urn)
        loop = asyncio.get_running_loop()

        mapping_results = None

        variant_count = job_manager.db.scalar(
            select(func.count(Variant.id)).where(Variant.score_set_id == score_set.id)
        )
        map_budget = _map_budget_seconds(variant_count or 0)

        logger.debug(msg="Mapping variants using VRS mapping service.", extra=job_manager.logging_context())
        job_manager.update_progress(30, 100, "Mapping variants using VRS mapping service.")
        # Keepalive heartbeat + size-aware budget around the opaque map call (see module docstring above).
        keepalive = asyncio.create_task(_keepalive_progress(job_manager, MAP_PROGRESS_KEEPALIVE_SECONDS))
        try:
            mapping_results = await asyncio.wait_for(loop.run_in_executor(ctx["pool"], blocking), timeout=map_budget)
        except asyncio.TimeoutError as e:
            raise TimeoutError(
                f"Mapping for {score_set.urn} exceeded its {map_budget}s budget ({variant_count} variants); "
                "the mapping service did not return in time."
            ) from e
        finally:
            keepalive.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await keepalive

        logger.debug(msg="Done mapping variants.", extra=job_manager.logging_context())
        job_manager.update_progress(80, 100, "Processing mapped variants.")

        ## Check our assumptions about mapping results and handle errors appropriately.

        # Ensure we have mapping results
        if not mapping_results:
            raise NonexistentMappingResultsError("Mapping results were not returned from VRS mapping service.")

        # Ensure we have mapped scores
        mapped_scores = mapping_results.get("mapped_scores")
        if not mapped_scores:
            internal_err = mapping_results.get(
                "error_message", "No variants were mapped and no error message was provided."
            )
            raise NonexistentMappingScoresError(internal_err)

        # Ensure we have reference metadata
        reference_metadata = mapping_results.get("reference_sequences")
        if not reference_metadata:
            raise NonexistentMappingReferenceError("Reference metadata missing from mapping results.")

        # Per-(target, alignment_level) QC records produced by the dcd-mapping API.
        # All records share the same tool_version because they come from a single run;
        # we use that as the global ``mapping_api_version`` carried on each MappingRecord.
        target_mappings_payload = mapping_results.get("target_mappings") or []
        tool_version = next(
            (tm.get("tool_version") for tm in target_mappings_payload if tm.get("tool_version")),
            None,
        )
        if not tool_version:
            raise NonexistentMappingResultsError(
                "Mapping results did not include any target_mappings with a tool_version."
            )

        # Process and store mapped variants
        # Index of (target_gene_identifier, alignment_level) -> persisted TargetGeneMapping row,
        # populated as we walk reference_metadata. Used to attach the right QC record to each
        # mapped variant in the score loop below.
        target_gene_mapping_by_key: dict[tuple[str, str], TargetGeneMapping] = {}

        for target_gene_identifier in reference_metadata:
            target_gene = next(
                (target_gene for target_gene in score_set.target_genes if target_gene.name == target_gene_identifier),
                None,
            )

            if not target_gene:
                raise ValueError(
                    f"Target gene {target_gene_identifier} not found in database for score set {score_set.urn}."
                )

            job_manager.save_to_context({"processing_target_gene": target_gene.id})
            logger.debug(f"Processing target gene {target_gene.name}.", extra=job_manager.logging_context())

            # allow for multiple annotation layers
            pre_mapped_metadata: dict[str, Any] = {}
            post_mapped_metadata: dict[str, Any] = {}

            # add gene-level info
            gene_info = reference_metadata[target_gene_identifier].get("gene_info")
            if gene_info:
                target_gene.mapped_hgnc_name = gene_info.get("hgnc_symbol")
                post_mapped_metadata["hgnc_name_selection_method"] = gene_info.get("selection_method")

                job_manager.save_to_context({"mapped_hgnc_name": target_gene.mapped_hgnc_name})
                logger.debug("Added mapped HGNC name to target gene.", extra=job_manager.logging_context())
            else:
                target_gene.mapped_hgnc_name = None
                logger.debug("No gene-level info found for target gene.", extra=job_manager.logging_context())

            # add annotation layer info
            for annotation_layer in reference_metadata[target_gene_identifier]["layers"]:
                # ``annotation_layer`` arrives as a dcd-mapping wire code (``p``/``c``/``g``);
                # we persist metadata under the corresponding full-name enum value.
                layer_name = SequenceLevel.from_wire(annotation_layer).value
                layer_premapped = reference_metadata[target_gene_identifier]["layers"][annotation_layer].get(
                    "computed_reference_sequence"
                )
                if layer_premapped:
                    pre_mapped_metadata[layer_name] = {
                        k: layer_premapped[k]
                        for k in set(list(layer_premapped.keys())) - EXCLUDED_PREMAPPED_ANNOTATION_KEYS
                    }
                    job_manager.save_to_context({"pre_mapped_layer_exists": True})

                layer_postmapped = reference_metadata[target_gene_identifier]["layers"][annotation_layer].get(
                    "mapped_reference_sequence"
                )
                if layer_postmapped:
                    post_mapped_metadata[layer_name] = layer_postmapped
                    job_manager.save_to_context({"post_mapped_layer_exists": True})

                logger.debug(
                    f"Added annotation layer mapping metadata for {annotation_layer}.",
                    extra=job_manager.logging_context(),
                )

            target_gene.pre_mapped_metadata = cast(pre_mapped_metadata, JSONB)
            target_gene.post_mapped_metadata = cast(post_mapped_metadata, JSONB)
            target_gene.uniprot_id_from_mapped_metadata = None

            job_manager.db.add(target_gene)
            logger.debug("Added mapping metadata to target gene.", extra=job_manager.logging_context())

            # Persist a TargetGeneMapping row per (target_gene, alignment_level) reported by
            # the dcd-mapping QC API. The match against ``target_mappings_payload`` is on
            # ``target_gene_identifier`` (must equal target_gene.name) and ``alignment_level``.
            for tm in target_mappings_payload:
                if tm.get("target_gene_identifier") != target_gene_identifier:
                    continue

                level_value = tm.get("alignment_level")
                if not level_value:
                    continue

                target_gene_mapping = TargetGeneMapping(
                    target_gene=target_gene,
                    alignment_level=SequenceLevel.from_wire(level_value),
                    preferred=bool(tm.get("preferred", False)),
                    reference_assembly=tm.get("reference_assembly"),
                    reference_accession=tm.get("reference_accession"),
                    reference_sequence_id=tm.get("reference_sequence_id"),
                    alignment_score=tm.get("alignment_score"),  # type: ignore[arg-type]
                    next_best_alignment_score=tm.get("next_best_alignment_score"),  # type: ignore[arg-type]
                    alignment_length=tm.get("alignment_length"),
                    alignment_string=tm.get("alignment_string"),
                    mismatch_count=tm.get("mismatch_count"),
                    gap_count=tm.get("gap_count"),
                    percent_identity=tm.get("percent_identity"),  # type: ignore[arg-type]
                    total_variants=tm.get("total_variants"),
                    variants_failed=tm.get("variants_failed"),
                    variants_with_alignment_warnings=tm.get("variants_with_alignment_warnings"),
                    variants_mapped_cleanly=tm.get("variants_mapped_cleanly"),
                    tool_name=tm.get("tool_name", "dcd-mapping"),
                    tool_version=tm["tool_version"],
                    tool_parameters=tm.get("tool_parameters"),
                    alignment_metadata=tm.get("alignment_metadata"),
                    vrs_version=tm.get("vrs_version"),
                    mapped_date=mapping_results["mapped_date"],
                    job_run_id=job_manager.job_id,
                )
                job_manager.db.add(target_gene_mapping)
                target_gene_mapping_by_key[(target_gene_identifier, level_value)] = target_gene_mapping

        # Flush so freshly inserted TargetGeneMapping rows have ids before we attach FKs
        # to mapped_variants below. We deliberately do NOT call update_progress() between
        # this flush and the mapped_variant loop -- update_progress commits as a checkpoint,
        # and a failure mid-loop would otherwise leave orphaned target_gene_mappings rows
        # committed without any mapped_variants referencing them.
        job_manager.db.flush()

        total_variants = len(mapped_scores)
        job_manager.save_to_context({"total_variants_to_process": total_variants})

        # Tally every record by its typed outcome; the mapped/failed/benign buckets are
        # derived from this after the loop. Keeping all four keys preserves the
        # intronic-vs-no-protein distinction in logs.
        outcome_counts: Counter[MappingOutcome] = Counter()
        logger.info(
            f"Processing {total_variants} mapped variants for score set {score_set.urn}.",
            extra=job_manager.logging_context(),
        )

        annotation_manager = AnnotationStatusManager(job_manager.db, job_run_id=job.id, score_set_id=score_set.id)
        for mapped_score in mapped_scores:
            variant_urn = mapped_score.get("mavedb_id")
            variant = job_manager.db.scalars(select(Variant).where(Variant.urn == variant_urn)).one()

            job_manager.save_to_context({"processing_variant": variant.id})
            logger.debug(f"Processing variant {variant.id}.", extra=job_manager.logging_context())

            # Only allow one live MappingRecord per variant. The prior live record (if any) is
            # superseded by the new version below via supersede_with, which retires it (cascading to
            # its allele links) and inserts the new record under one timestamp.
            existing_mapped_variant = (
                job_manager.db.query(MappingRecord)
                .filter(MappingRecord.variant_id == variant.id, MappingRecord.current)
                .one_or_none()
            )
            if existing_mapped_variant:
                job_manager.save_to_context({"existing_mapped_variant": existing_mapped_variant.id})

            # The typed outcome -- not allele presence -- decides success/benign/failure.
            # Absent outcome means an older or malformed payload; fail fast.
            raw_outcome = mapped_score.get("outcome")
            if not raw_outcome:
                raise NonexistentMappingResultsError(
                    f"ScoreAnnotation for variant {variant_urn!r} is missing its outcome."
                )
            outcome = MappingOutcome(raw_outcome)

            outcome_counts[outcome] += 1
            job_manager.save_to_context({"outcome_counts": {o.value: n for o, n in outcome_counts.items()}})

            # dcd-mapping guarantees both fields are set on every ScoreAnnotation,
            # including failed variants (the annotate step re-attributes failures to
            # preferred_layer_for_target before emitting). Absent fields indicate an
            # older or malformed payload and we fail fast rather than silently drop the FK.
            score_target = mapped_score.get("target_gene_identifier")
            score_alignment_level = mapped_score.get("alignment_level")
            if not score_target or not score_alignment_level:
                raise NonexistentMappingResultsError(
                    f"ScoreAnnotation for variant {variant_urn!r} is missing target_gene_identifier or alignment_level."
                )

            pre_mapped_allele: dict = mapped_score.get("pre_mapped") or {}
            post_mapped_allele: dict = mapped_score.get("post_mapped") or {}
            sequence_level = SequenceLevel.from_wire(score_alignment_level)
            assay_level_hgvs = get_hgvs_from_post_mapped(post_mapped_allele, combine_cis=True)

            # dcd-mapping guarantees every mapped score is attributable to a TargetGeneMapping
            # via (target_gene_identifier, alignment_level) -- including failed variants, which
            # it re-attributes to the target's preferred layer. A miss implies a malformed payload.
            target_gene_mapping_row = target_gene_mapping_by_key.get((score_target, score_alignment_level))
            if target_gene_mapping_row is None:
                raise NonexistentMappingResultsError(
                    f"ScoreAnnotation for variant {variant_urn!r} has no TargetGeneMapping for "
                    f"(target={score_target!r}, alignment_level={score_alignment_level!r})."
                )

            mapping_record = MappingRecord(
                variant_id=variant.id,
                vrs_digest=pre_mapped_allele.get("id"),
                pre_mapped=pre_mapped_allele or None,
                assay_level=sequence_level,
                hgvs_assay_level=assay_level_hgvs,
                mapped_date=mapping_results["mapped_date"],
                vrs_version=mapped_score.get("vrs_version", None),
                mapping_api_version=tool_version,
                target_gene_mapping_id=target_gene_mapping_row.id,
                alignment_level=sequence_level,
                at_mismatched_locus=mapped_score.get("at_mismatched_locus"),
                near_gap=mapped_score.get("near_gap"),
            )
            if existing_mapped_variant:
                # Retire the prior record (cascading to its allele links) and insert this one under a
                # single timestamp, so the prior valid_to equals the new valid_from with no gap.
                existing_mapped_variant.supersede_with(job_manager.db, mapping_record)
                logger.debug(
                    msg="Superseded prior mapping record and its allele links.", extra=job_manager.logging_context()
                )
            else:
                job_manager.db.add(mapping_record)

            # The mapper emits a MappingRecord for EVERY variant, so "a record exists" carries no
            # signal — the signal is whether a real allele resulted. MAPPED yields an authoritative
            # allele -> present. A benign absence (intronic, synonymous) produces a record but no
            # allele: an informative biological negative -> absent. FAILED -> failed. `reason` reuses
            # the MappingOutcome vocabulary so the intronic-vs-no-protein distinction survives.
            if outcome is MappingOutcome.MAPPED:
                disposition = Disposition.PRESENT
            elif outcome.is_benign_absence:
                disposition = Disposition.ABSENT
            else:
                disposition = Disposition.FAILED

            annotation_manager.record_event(
                AnnotationType.VRS_MAPPING,
                variant_id=variant.id,
                disposition=disposition,
                reason=outcome.value,
                source_version=tool_version,
                metadata={
                    "mapped_assay_level_hgvs": assay_level_hgvs,
                    "error_message": mapped_score.get("error_message"),
                },
            )

            # Only variants with a post-mapped representation yield an authoritative Allele;
            # failed and benign-absent variants get a MappingRecord but no linked allele.
            if post_mapped_allele:
                # Recompute rather than trust the incoming id, which may predate normalization for a
                # deletion/duplication. vrs_digest is the dedup key  the whole allele graph hangs off.
                canonical_post_mapped, allele_digest = canonical_variation_document(
                    post_mapped_allele, subject=f"variant {variant.urn}"
                )
                allele_draft = AlleleDbModel(
                    vrs_digest=allele_digest,
                    level=sequence_level,
                    hgvs_g=assay_level_hgvs if sequence_level == SequenceLevel.genomic else None,
                    hgvs_c=assay_level_hgvs if sequence_level == SequenceLevel.cdna else None,
                    hgvs_p=assay_level_hgvs if sequence_level == SequenceLevel.protein else None,
                    post_mapped=canonical_post_mapped,
                )
                authoritative_allele = get_or_create_allele(job_manager.db, allele_draft)
                job_manager.db.flush()

                # TODO#765: Mapping is not idempotent, so we must always create a new link.
                job_manager.db.add(
                    MappingRecordAllele(
                        mapping_record_id=mapping_record.id,
                        allele_id=authoritative_allele.id,
                        is_authoritative=True,
                    )
                )
                logger.debug(msg="Linked mapped variant to authoritative allele.", extra=job_manager.logging_context())

            logger.debug(msg="Added new mapped variant to session.", extra=job_manager.logging_context())
            job_manager.db.flush()

        annotation_manager.flush()

        # Collapse the per-outcome tally into the three buckets the rest of the job reasons about.
        mapped_count = outcome_counts[MappingOutcome.MAPPED]
        failed_count = outcome_counts[MappingOutcome.FAILED]
        skipped_count = sum(n for o, n in outcome_counts.items() if o.is_benign_absence)

        # State keys off genuine failures only: no failures -> complete (benign absences
        # don't count); failures with nothing mapped -> failed; otherwise incomplete.
        if failed_count == 0:
            score_set.mapping_state = MappingState.complete
        elif mapped_count == 0:
            score_set.mapping_state = MappingState.failed
            score_set.mapping_errors = {"error_message": "All variants failed to map."}
        else:
            score_set.mapping_state = MappingState.incomplete

        job_manager.save_to_context(
            {
                "mapped_count": mapped_count,
                "failed_count": failed_count,
                "skipped_count": skipped_count,
                "mapping_state": score_set.mapping_state.name,
                "mapping_errors": score_set.mapping_errors,
                "inserted_mapped_variants": len(mapped_scores),
            }
        )

        # Flush score set state; the decorator will commit on return via the success/return paths below.
        job_manager.db.add(score_set)
        job_manager.db.flush()

    except (NonexistentMappingResultsError, NonexistentMappingScoresError, NonexistentMappingReferenceError) as e:
        logging_context = {**job_manager.logging_context(), **format_raised_exception_info_as_dict(e)}
        logger.error(msg="Known error during variant mapping.", extra=logging_context)

        job_manager.db.rollback()

        score_set.mapping_state = MappingState.failed
        score_set.mapping_errors = {"error_message": str(e)}

        # Persist score set state to survive any decorator rollback.
        job_manager.db.add(score_set)
        job_manager.db.commit()
        return JobExecutionOutcome.failed(
            reason=str(e),
            data={"score_set_id": score_set.id, "mapped_count": 0, "total_count": 0},
            failure_category=FailureCategory.DATA_ERROR,
        )

    except Exception as e:
        logging_context = {**job_manager.logging_context(), **format_raised_exception_info_as_dict(e)}
        logger.error(msg="Encountered an unexpected error while parsing mapped variants.", extra=logging_context)

        job_manager.db.rollback()

        score_set.mapping_state = MappingState.failed
        if not score_set.mapping_errors:
            score_set.mapping_errors = {
                "error_message": f"Encountered an unexpected error while parsing mapped variants. This job will be retried up to {job.max_retries} times (this was attempt {job.retry_count})."
            }

        # Persist score set state to survive any decorator rollback.
        job_manager.db.add(score_set)
        job_manager.db.commit()

        raise

    logger.info(msg="Inserted mapped variants into db.", extra=job_manager.logging_context())

    # Fail the job only on genuine failure with nothing mapped; all-benign is a success.
    if mapped_count == 0 and failed_count > 0:
        logger.error(msg="No variants were successfully mapped.", extra=job_manager.logging_context())
        job_manager.db.flush()
        return JobExecutionOutcome.failed(
            reason="No variants were successfully mapped.",
            data={
                "score_set_id": score_set.id,
                "mapped_count": 0,
                "failed_count": failed_count,
                "skipped_count": skipped_count,
                "total_count": total_variants,
            },
            failure_category=FailureCategory.VRS_MAPPING_FAILED,
        )

    logger.info(
        msg=(
            f"Variant mapping job completed successfully: {mapped_count} mapped, "
            f"{failed_count} failed, {skipped_count} skipped (intronic / no protein consequence)."
        ),
        extra=job_manager.logging_context(),
    )
    job_manager.db.flush()
    return JobExecutionOutcome.succeeded(
        data={
            "score_set_id": score_set.id,
            "mapped_count": mapped_count,
            "failed_count": failed_count,
            "skipped_count": skipped_count,
            "total_count": total_variants,
        }
    )
