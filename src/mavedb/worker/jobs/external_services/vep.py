"""VEP molecular-consequence job for variant effect prediction.

Links deduplicated alleles to their Ensembl VEP molecular consequence. The job owns the lifecycle —
selecting which alleles to annotate, the current-release skip, linking, and status events — while the
resolution itself (transport, batching, Recoder fallback, the transcript-matching rule) lives in
``mavedb.lib.vep`` and the shared ``variant_annotation.lib.vep`` kernel, so the pipeline and the lab
CLI produce identical consequences for the same input (#772).
"""

import contextlib
import logging
from collections import Counter
from datetime import date
from typing import Iterator, Optional

from sqlalchemy import select
from variant_annotation.lib.vep import (
    RESOLVER_VERSION,
    ConsequenceOutcome,
    ConsequenceResolution,
    ReferenceSequence,
    VepInput,
)

from mavedb.lib.annotation_status_manager import AnnotationStatusManager
from mavedb.lib.clingen.alleles import ScoreSetAlleleRow, get_alleles_for_score_set, group_alleles_for_annotation
from mavedb.lib.types.workflow import JobExecutionOutcome
from mavedb.lib.utils import batched
from mavedb.lib.vep import (
    VepLinkVerdict,
    get_ensembl_release,
    link_vep_consequences_to_alleles,
    resolve_consequences,
)
from mavedb.models.enums.annotation_type import AnnotationType
from mavedb.models.enums.disposition import Disposition
from mavedb.models.enums.event_reason import EventReason
from mavedb.models.enums.sequence_level import SequenceLevel
from mavedb.models.score_set import ScoreSet
from mavedb.models.vep_allele_consequence import VepAlleleConsequence
from mavedb.worker.jobs.utils.setup import validate_job_params
from mavedb.worker.lib.decorators.pipeline_management import with_pipeline_management
from mavedb.worker.lib.managers.job_manager import JobManager
from mavedb.worker.lib.translation_ports import uta_transcript_source

logger = logging.getLogger(__name__)

# Inputs resolved per await so progress can be reported between chunks. Matches the kernel's own VEP
# batch ceiling (200 HGVS/request), so each chunk is ~one wire batch. Progress must be reported from
# this coroutine, never from inside resolve_consequences: that call runs on an executor thread and the
# progress commit touches this job's DB session, which is not thread-safe.
_RESOLUTION_CHUNK_SIZE = 200


class _BestEffortReference:
    """Wraps the UTA transcript-reference port so a mid-run failure disables reference-identical
    detection for the affected input (returns ``None``) instead of failing the VEP job.

    Reference-identical labelling is a refinement for wild-type controls; it must never take down
    resolution. A lookup that raises (a dropped UTA connection, a query error) is logged once and
    treated as "cannot decide", so the input falls back to ``ABSENT`` exactly as it would with no
    reference port. Runs on the resolution executor thread, so it logs without ``extra`` context.
    """

    def __init__(self, inner: ReferenceSequence) -> None:
        self._inner = inner
        self._warned = False

    def coding_interval_reference(self, transcript: str, start: int, stop: int) -> Optional[str]:
        try:
            return self._inner.coding_interval_reference(transcript, start, stop)
        except Exception as exc:  # noqa: BLE001 - any UTA failure degrades to no reference-identical label
            if not self._warned:
                logger.warning("UTA reference lookup failed mid-run; reference-identical detection degraded: %s", exc)
                self._warned = True
            return None


@contextlib.contextmanager
def _reference_source_for_vep(job_manager: JobManager) -> Iterator[Optional[ReferenceSequence]]:
    """Yield a transcript-reference port for reference-identical detection, or ``None`` when UTA is
    unavailable.

    Best-effort by design: VEP resolution must not fail because UTA is unset or unreachable. The port is
    consulted only for no-change controls and unparseables — everything VEP resolves normally is
    untouched — so without it those inputs simply fall back to ``ABSENT`` rather than being labelled
    ``REFERENCE_IDENTICAL``. An open-time failure is caught here; a mid-run failure is caught by
    :class:`_BestEffortReference`.
    """
    with contextlib.ExitStack() as stack:
        try:
            source = stack.enter_context(uta_transcript_source())
        except Exception as exc:  # noqa: BLE001 - opening UTA failed; run without reference-identical
            logger.warning(
                msg=f"UTA transcript source unavailable; reference-identical detection disabled for this run "
                f"(wild-type controls resolve to ABSENT). Cause: {exc}",
                extra=job_manager.logging_context(),
            )
            yield None
            return
        yield _BestEffortReference(source)


def _annotate_vep(
    annotation_manager: AnnotationStatusManager,
    allele_id: int,
    disposition: Disposition,
    reason: EventReason,
    *,
    source_version: str,
    error_message: str | None = None,
    metadata: dict | None = None,
) -> None:
    """Record one VEP_FUNCTIONAL_CONSEQUENCE event for an allele (the consequence is an allele-level fact).

    The single choke point for VEP's status writes. One event per allele, stamped with the Ensembl
    release queried; provenance (which variants drove the linkage) is derived from the live links
    as-of the event, not fanned out here. The consequence value itself is not embedded — it lives in
    the ``VepAlleleConsequence`` value table, joinable by ``allele_id`` + ``source_version``.
    """
    meta = dict(metadata or {})
    if error_message is not None:
        meta["error_message"] = error_message

    annotation_manager.record_event(
        AnnotationType.VEP_FUNCTIONAL_CONSEQUENCE,
        allele_id=allele_id,
        disposition=disposition,
        reason=reason,
        source_version=source_version,
        metadata=meta or None,
    )


def _vep_input_for_allele(row: ScoreSetAlleleRow) -> VepInput | None:
    """Build a VEP input for an allele, or ``None`` to skip it.

    Only coding and genomic alleles are submitted to VEP: a protein allele's consequence is carried
    instead by the coding alleles reverse translation enumerates from it, so protein alleles are never
    sent. Each allele carries exactly one of ``hgvs_g``/``hgvs_c``/``hgvs_p``.

    No transcript is set on the input. A coding allele carries its transcript inside the ``c.`` HGVS
    accession, which the kernel infers, so it resolves against its own transcript (the #772 fix). A
    genomic allele carries only an ``NC_`` chromosome accession, so it resolves to VEP's
    cross-transcript ``most_severe`` headline — deliberately, not as a gap. Alleles are deduplicated
    and content-addressed, so one genomic allele is shared across every score set that maps to it;
    there is no single "assay transcript" to attribute to it, and borrowing the coding transcript from
    one arbitrary pairing would be less faithful than the transcript-agnostic ``most_severe`` call.
    ``most_severe`` is the honest reconstruction for a genomic allele, and its
    ``consequence_source='most_severe'`` records that provenance so consumers can tell it apart from a
    transcript-matched call.
    """
    if row.level == SequenceLevel.protein.value:
        return None

    hgvs = row.hgvs_c or row.hgvs_g
    if not hgvs:
        return None

    return VepInput(hgvs=hgvs)


@with_pipeline_management
async def populate_vep_for_score_set(ctx: dict, job_id: int, job_manager: JobManager) -> JobExecutionOutcome:
    """Link deduplicated alleles to their VEP molecular consequence.

    Runs over the score set's current coding and genomic alleles (authoritative and RT-derived),
    resolves each against its own transcript via the shared kernel (with a Variant Recoder fallback),
    and stores the consequence plus its resolution provenance in a valid-time
    :class:`VepAlleleConsequence`, superseding only on a changed headline term. Protein alleles are not
    submitted — their consequence is carried by the coding alleles reverse translation enumerates.

    Wild-type controls (an input describing no sequence change) are resolved to a ``reference_identical``
    consequence via a best-effort UTA transcript-reference port, so a control is stored distinctly rather
    than dropped as a no-result. If UTA is unavailable the run proceeds without that label (those alleles
    fall back to absent); see :func:`_reference_source_for_vep`.

    Job Parameters:
        - score_set_id (int): The ID of the ScoreSet whose alleles to process.
        - correlation_id (str): Correlation ID for tracing requests across services.
        - force (bool, optional): Bypass the current-release/resolver skip and re-query every eligible
          allele. The linker still supersedes only on a headline-term change, so a forced re-run of
          unchanged data writes no new rows. Use for re-ingestion or to heal suspected corruption. A
          resolution-rule change no longer needs force: it bumps RESOLVER_VERSION, which the skip keys
          on, so those alleles re-query on the next ordinary run.

    Args:
        ctx (dict): The job context dictionary.
        job_id (int): The ID of the job being executed.
        job_manager (JobManager): Manager for job lifecycle and DB operations.

    Returns:
        JobExecutionOutcome: outcome with per-allele created/preexisting/absent/errored counts, plus
        ``retained_on_absence_count`` — preexisting alleles whose consequence was kept because VEP
        returned nothing this run (a genuine-disappearance signal, surfaced in the job metadata).
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

    # One work-unit per eligible allele (payload = a transcript-aware VepInput; protein alleles and
    # alleles without HGVS are skipped). Events are allele-keyed, so each allele records its own event.
    allele_inputs = group_alleles_for_annotation(
        get_alleles_for_score_set(job_manager.db, score_set.id),
        payload=_vep_input_for_allele,
    )

    annotation_counts: Counter[str] = Counter(
        {
            "created_allele_count": 0,
            "preexisting_allele_count": 0,
            "absent_allele_count": 0,
            "errored_allele_count": 0,
            # A subset of preexisting: alleles that kept a prior consequence because VEP returned nothing
            # this run. Surfaced in the job outcome so a genuine-disappearance pattern is visible without
            # digging through logs (see VepLinkVerdict.RETAINED_ON_ABSENCE).
            "retained_on_absence_count": 0,
        }
    )

    num_alleles_with_hgvs = len(allele_inputs)
    job_manager.save_to_context({"num_alleles_to_link_vep": num_alleles_with_hgvs})

    if not allele_inputs:
        logger.warning(
            msg="No current coding/genomic alleles with HGVS were found for this score set. Skipping VEP linkage (nothing to do).",
            extra=job_manager.logging_context(),
        )
        job_manager.db.flush()
        return JobExecutionOutcome.succeeded(data=dict(annotation_counts))

    all_allele_ids = set(allele_inputs.keys())

    # The Ensembl release version-keys the run (coordinated software + transcript set + vocabulary). It
    # is load-bearing for the skip below, so a failure here aborts the job rather than mis-versioning
    # writes (the exception propagates to the job decorators).
    ensembl_release = await get_ensembl_release()
    job_manager.save_to_context({"ensembl_release": ensembl_release})

    def alleles_at_current_release(allele_ids: set[int]) -> set[int]:
        """Allele ids (within the given set) holding a live VEP consequence at the current Ensembl release
        *and* the current resolver version — both axes must match to count as up to date."""
        if not allele_ids:
            return set()
        return set(
            job_manager.db.scalars(
                select(VepAlleleConsequence.allele_id)
                .where(VepAlleleConsequence.allele_id.in_(allele_ids))
                .where(VepAlleleConsequence.current)
                .where(VepAlleleConsequence.functional_consequence.isnot(None))
                .where(VepAlleleConsequence.source_version == ensembl_release)
                # A resolution-rule change bumps RESOLVER_VERSION without an Ensembl release bump; gating
                # on both axes re-queries those alleles automatically instead of them looking current
                # forever. A NULL resolver_version (pre-column row) never matches and so is re-queried.
                .where(VepAlleleConsequence.resolver_version == RESOLVER_VERSION)
            ).all()
        )

    # Skip alleles already resolved at the current Ensembl release AND resolver version (they cannot
    # change without one of those bumping). force re-queries all regardless — for re-ingestion or to heal
    # suspected corruption. The linker still supersedes only on a value change, so a forced (or
    # resolver-bumped) no-op advances the version in place and writes no new history row.
    already_current = set() if force else alleles_at_current_release(all_allele_ids)
    inputs_by_allele = {aid: allele_inputs[aid] for aid in allele_inputs if aid not in already_current}
    # HGVS alone identifies a unique resolution question here: this job never sets VepInput.transcript
    # (a coding allele's transcript rides inside its c. HGVS; a genomic allele is transcript-agnostic —
    # see _vep_input_for_allele), and resolve_consequences re-keys its result by HGVS to match. If this
    # job ever passes explicit transcripts, both this dedup key and that return keying must become
    # (hgvs, transcript), or same-HGVS/different-transcript inputs collapse into one.
    unique_inputs = list({i.hgvs: i for i in inputs_by_allele.values()}.values())
    job_manager.save_to_context(
        {
            "num_alleles_already_current": len(already_current),
            "num_hgvs_to_query": len(unique_inputs),
            "force": force,
        }
    )

    verdicts: dict[int, VepLinkVerdict] = {}
    errored_allele_ids: set[int] = set()
    if unique_inputs:
        # The shared orchestration exposes no per-input progress hook, so resolve in chunks and report
        # between them. Progress is emitted here on the coroutine, not from inside resolve_consequences:
        # that runs on an executor thread and the progress commit writes this job's (non-thread-safe) DB
        # session. Chunk-level granularity is the resolution; finer would require a kernel progress hook.
        job_manager.update_progress(10, 100, f"Resolving VEP consequences for {len(unique_inputs)} HGVS strings.")
        resolutions_by_hgvs: dict[str, ConsequenceResolution] = {}
        chunks = list(batched(unique_inputs, _RESOLUTION_CHUNK_SIZE))
        # A no-change control (an explicit c.= form, or a delins whose bases equal the transcript's own)
        # is not a VEP-resolvable variant; the reference port lets it resolve to REFERENCE_IDENTICAL
        # instead of vanishing into ABSENT. Best-effort: a missing/down UTA just disables that label.
        with _reference_source_for_vep(job_manager) as reference:
            for chunk_idx, chunk in enumerate(chunks):
                resolutions_by_hgvs.update(await resolve_consequences(list(chunk), reference=reference))
                job_manager.update_progress(
                    10 + int((chunk_idx + 1) / len(chunks) * 85),
                    100,
                    f"Resolved VEP consequences for batch {chunk_idx + 1}/{len(chunks)}.",
                )

        # Every queried input gets a resolution. An allele missing a resolution is treated as genuinely empty
        # and never a failure. This ensures a dropped input can only under-report, never overwrite a held consequence.
        resolution_by_allele = {
            aid: resolutions_by_hgvs.get(inp.hgvs)
            or ConsequenceResolution(input=inp, outcome=ConsequenceOutcome.ABSENT)
            for aid, inp in inputs_by_allele.items()
        }

        # Alleles whose VEP/Recoder request failed: unknown, not a negative — kept distinct from a genuine
        # empty. Never linked, since that would overwrite a held consequence with a failure.
        errored_allele_ids = {
            aid for aid, res in resolution_by_allele.items() if res.outcome is ConsequenceOutcome.ERRORED
        }
        linkable = {
            aid: res for aid, res in resolution_by_allele.items() if res.outcome is not ConsequenceOutcome.ERRORED
        }
        verdicts = link_vep_consequences_to_alleles(
            job_manager.db, linkable, source_version=ensembl_release, access_date=date.today()
        )
        job_manager.db.flush()
    else:
        logger.info(
            msg="All eligible alleles are already resolved at the current Ensembl release; skipping VEP query.",
            extra=job_manager.logging_context(),
        )
        job_manager.update_progress(99, 100, "All alleles already current at this Ensembl release.")

    annotation_manager = AnnotationStatusManager(
        job_manager.db, job_run_id=job_manager.job_id, score_set_id=score_set.id
    )
    for allele_id, vep_input in allele_inputs.items():
        verdict = verdicts.get(allele_id)
        if verdict is VepLinkVerdict.CREATED:
            annotation_counts["created_allele_count"] += 1
            _annotate_vep(
                annotation_manager,
                allele_id,
                Disposition.PRESENT,
                EventReason.CREATED,
                source_version=ensembl_release,
                metadata={"hgvs": vep_input.hgvs},
            )

        elif allele_id in already_current or verdict in (
            VepLinkVerdict.UNCHANGED,
            VepLinkVerdict.RETAINED_ON_ABSENCE,
        ):
            # The allele still has a live consequence, so its status is preexisting either way. When it
            # was retained despite VEP finding nothing this run, also tally it separately for the outcome.
            annotation_counts["preexisting_allele_count"] += 1
            if verdict is VepLinkVerdict.RETAINED_ON_ABSENCE:
                annotation_counts["retained_on_absence_count"] += 1
            _annotate_vep(
                annotation_manager,
                allele_id,
                Disposition.PRESENT,
                EventReason.PREEXISTING,
                source_version=ensembl_release,
                metadata={"hgvs": vep_input.hgvs},
            )

        elif allele_id in errored_allele_ids:
            annotation_counts["errored_allele_count"] += 1
            _annotate_vep(
                annotation_manager,
                allele_id,
                Disposition.FAILED,
                EventReason.API_ERROR,
                source_version=ensembl_release,
                error_message="The VEP/Variant Recoder request for this allele failed; result unknown.",
                metadata={"hgvs": vep_input.hgvs},
            )

        else:
            annotation_counts["absent_allele_count"] += 1
            _annotate_vep(
                annotation_manager,
                allele_id,
                Disposition.ABSENT,
                EventReason.NO_RECORD,
                source_version=ensembl_release,
                error_message="VEP found no functional consequence for this allele, even after Variant Recoder fallback.",
                metadata={"hgvs": vep_input.hgvs},
            )

    annotation_manager.flush()

    outcome_data = dict(annotation_counts)
    job_manager.save_to_context(outcome_data)
    job_manager.update_progress(
        100,
        100,
        (
            f"Completed VEP linkage: {annotation_counts['created_allele_count'] + annotation_counts['preexisting_allele_count']} linked, "
            f"{annotation_counts['absent_allele_count']} absent (no result), "
            f"{annotation_counts['errored_allele_count']} errored, "
            f"{annotation_counts['retained_on_absence_count']} retained despite no result this run."
        ),
    )
    logger.info(msg="Done linking VEP consequences to alleles.", extra=job_manager.logging_context())
    job_manager.db.flush()
    return JobExecutionOutcome.succeeded(data=outcome_data)
