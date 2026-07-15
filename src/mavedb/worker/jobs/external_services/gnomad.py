"""gnomAD variant linking jobs for population frequency annotation.

This module handles linking of deduplicated alleles to gnomAD (Genome Aggregation Database)
variants to provide population frequency and other genomic context information.
This enrichment helps researchers understand the clinical significance and
rarity of variants in their datasets.
"""

import logging
from collections import Counter

from sqlalchemy import select

from mavedb.db import athena
from mavedb.lib.annotation_status_manager import AnnotationStatusManager
from mavedb.lib.clingen.alleles import get_alleles_for_score_set, group_alleles_for_annotation
from mavedb.lib.gnomad import (
    GNOMAD_DATA_VERSION,
    GnomadLinkVerdict,
    gnomad_variant_data_for_caids,
    link_gnomad_variants_to_alleles,
)
from mavedb.lib.types.workflow import JobExecutionOutcome
from mavedb.models.enums.annotation_type import AnnotationType
from mavedb.models.enums.disposition import Disposition
from mavedb.models.enums.event_reason import EventReason
from mavedb.models.enums.sequence_level import SequenceLevel
from mavedb.models.gnomad_allele_link import GnomadAlleleLink
from mavedb.models.gnomad_variant import GnomADVariant
from mavedb.models.score_set import ScoreSet
from mavedb.worker.jobs.utils.setup import validate_job_params
from mavedb.worker.lib.decorators.pipeline_management import with_pipeline_management
from mavedb.worker.lib.managers.job_manager import JobManager

logger = logging.getLogger(__name__)


def _annotate_gnomad(
    annotation_manager: AnnotationStatusManager,
    allele_id: int,
    disposition: Disposition,
    reason: EventReason,
    *,
    error_message: str | None = None,
    metadata: dict | None = None,
) -> None:
    """Record one GNOMAD_ALLELE_FREQUENCY event for an allele (frequency is an allele-level fact).

    The single choke point for gnomAD's status writes. One event per allele, stamped at the current
    gnomAD version; provenance (which variants drove the linkage) is derived from the live links
    as-of the event, not fanned out here.
    """
    meta = dict(metadata or {})
    if error_message is not None:
        meta["error_message"] = error_message

    annotation_manager.record_event(
        AnnotationType.GNOMAD_ALLELE_FREQUENCY,
        allele_id=allele_id,
        disposition=disposition,
        reason=reason,
        source_version=GNOMAD_DATA_VERSION,
        metadata=meta or None,
    )


@with_pipeline_management
async def link_gnomad_variants(ctx: dict, job_id: int, job_manager: JobManager) -> JobExecutionOutcome:
    """
    Link deduplicated alleles to gnomAD variants based on ClinGen Allele IDs (CAIDs).
    This job fetches the current authoritative alleles of a score set that carry CAIDs,
    retrieves corresponding gnomAD variant data, and establishes valid-time links between them.

    Job Parameters:
        - score_set_id (int): The ID of the ScoreSet whose alleles to process.
        - correlation_id (str): Correlation ID for tracing requests across services.
        - force (bool, optional): Bypass the version-keyed skip and re-fetch every CAID-bearing
          allele. The linker still supersedes only on change, so a forced re-run of unchanged data
          writes no new links. Use for re-ingestion or to heal suspected link corruption.

    Args:
        ctx (dict): The job context dictionary.
        job_id (int): The ID of the job being executed.
        job_manager (JobManager): The job manager instance for database and logging operations.

    Side Effects:
        - Creates GnomadAlleleLink rows linking alleles to gnomAD variants.

    Returns:
        JobExecutionOutcome: outcome with per-allele created/preexisting/skipped counts.
    """
    # Get the job definition we are working on
    job = job_manager.get_job()

    _job_required_params = ["score_set_id", "correlation_id"]
    validate_job_params(_job_required_params, job)

    # Fetch required resources based on param inputs. Safely ignore mypy warnings here, as they were checked above.
    score_set = job_manager.db.scalars(select(ScoreSet).where(ScoreSet.id == job.job_params["score_set_id"])).one()  # type: ignore
    correlation_id = job.job_params["correlation_id"]  # type: ignore
    force = bool(job.job_params.get("force", False))  # type: ignore[union-attr]

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

    # One work-unit per allele (payload = CAID; alleles without one are skipped). Covers the score
    # set's alleles — authoritative and RT-derived — since the genomic allele gnomAD knows
    # is often the RT-derived one. Events are allele-keyed, so every linked allele is recorded.
    allele_rows = get_alleles_for_score_set(job_manager.db, score_set.id)
    allele_data = group_alleles_for_annotation(allele_rows, payload=lambda row: row.clingen_allele_id)
    allele_levels = {row.allele_id: row.level for row in allele_rows}

    annotation_counts: Counter[str] = Counter(
        {
            "created_allele_count": 0,
            "preexisting_allele_count": 0,
            "skipped_allele_count": 0,
        }
    )

    num_alleles_with_caids = len(allele_data)
    job_manager.save_to_context({"num_alleles_to_link_gnomad": num_alleles_with_caids})

    if not allele_data:
        logger.warning(
            msg="No current alleles with CAIDs were found for this score set. Skipping gnomAD linkage (nothing to do).",
            extra=job_manager.logging_context(),
        )
        job_manager.db.flush()
        return JobExecutionOutcome.succeeded(data=dict(annotation_counts))

    job_manager.update_progress(
        10, 100, f"Found {num_alleles_with_caids} alleles with CAIDs to link to gnomAD variants."
    )

    def alleles_linked_at_current_version(allele_ids: set[int]) -> set[int]:
        """Allele ids (within the given set) holding a live gnomAD link at the current gnomAD version."""
        if not allele_ids:
            return set()
        return set(
            job_manager.db.scalars(
                select(GnomadAlleleLink.allele_id)
                .join(GnomADVariant, GnomADVariant.id == GnomadAlleleLink.gnomad_variant_id)
                .where(GnomadAlleleLink.allele_id.in_(allele_ids))
                .where(GnomadAlleleLink.current)
                .where(GnomADVariant.db_version == GNOMAD_DATA_VERSION)
            ).all()
        )

    # Skip alleles already linked at the current version (they can't change). force re-fetches
    # all; the linker still supersedes only on change, so a forced no-op writes nothing.
    already_current = set() if force else alleles_linked_at_current_version(set(allele_data.keys()))
    # Never query Athena for protein alleles — gnomAD is nucleotide-level, so they can't match. They
    # are still recorded as not-applicable in the loop below; this only spares them the round-trip.
    variant_caids = sorted(
        {
            allele_data[aid]
            for aid in allele_data
            if aid not in already_current and allele_levels.get(aid) != SequenceLevel.protein.value
        }
    )

    job_manager.save_to_context(
        {
            "num_alleles_already_current": len(already_current),
            "num_caids_to_query": len(variant_caids),
            "force": force,
        }
    )

    verdicts: dict[int, GnomadLinkVerdict] = {}
    if variant_caids:
        with athena.engine.connect() as athena_session:
            logger.debug("Fetching gnomAD variants from Athena.")
            gnomad_variant_data = gnomad_variant_data_for_caids(athena_session, variant_caids)

        num_gnomad_variants_with_caid_match = len(gnomad_variant_data)
        job_manager.save_to_context({"num_gnomad_variants_with_caid_match": num_gnomad_variants_with_caid_match})
        job_manager.update_progress(
            75, 100, f"Found {num_gnomad_variants_with_caid_match} gnomAD variants matching CAIDs."
        )

        logger.info(msg="Attempting to link alleles to gnomAD variants.", extra=job_manager.logging_context())
        verdicts = link_gnomad_variants_to_alleles(job_manager.db, gnomad_variant_data)
        job_manager.db.flush()
    else:
        logger.info(
            msg="All CAID-bearing alleles are already linked at the current gnomAD version; skipping Athena query.",
            extra=job_manager.logging_context(),
        )
        job_manager.update_progress(75, 100, "All alleles already current at this gnomAD version.")

    annotation_manager = AnnotationStatusManager(
        job_manager.db, job_run_id=job_manager.job_id, score_set_id=score_set.id
    )
    for allele_id, caid in allele_data.items():
        # gnomAD is a nucleotide-level, genomic-coordinate resource. Skip any protein alleles.
        if allele_levels.get(allele_id) == SequenceLevel.protein.value:
            annotation_counts["skipped_allele_count"] += 1
            _annotate_gnomad(
                annotation_manager,
                allele_id,
                Disposition.NOT_APPLICABLE,
                EventReason.PROTEIN_LEVEL_ALLELE,
                error_message="Protein-level alleles are not linked to gnomAD; see nucleotide siblings.",
                metadata={"clingen_allele_id": caid},
            )
            continue

        verdict = verdicts.get(allele_id)
        if verdict is GnomadLinkVerdict.CREATED:
            annotation_counts["created_allele_count"] += 1
            _annotate_gnomad(
                annotation_manager,
                allele_id,
                Disposition.PRESENT,
                EventReason.CREATED,
                metadata={"clingen_allele_id": caid},
            )
        elif allele_id in already_current or verdict is GnomadLinkVerdict.UNCHANGED:
            annotation_counts["preexisting_allele_count"] += 1
            _annotate_gnomad(
                annotation_manager,
                allele_id,
                Disposition.PRESENT,
                EventReason.PREEXISTING,
                metadata={"clingen_allele_id": caid},
            )
        else:
            annotation_counts["skipped_allele_count"] += 1
            _annotate_gnomad(
                annotation_manager,
                allele_id,
                Disposition.ABSENT,
                EventReason.NO_RECORD,
                error_message="No gnomAD variant could be linked for this allele.",
                metadata={"clingen_allele_id": caid},
            )

    annotation_manager.flush()

    outcome_data = dict(annotation_counts)
    job_manager.save_to_context(outcome_data)
    logger.info(msg="Done linking gnomAD variants to alleles.", extra=job_manager.logging_context())
    job_manager.db.flush()
    return JobExecutionOutcome.succeeded(data=outcome_data)
