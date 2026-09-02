"""Reverse translation worker job — builds cross-level HGVS equivalence classes.

For each mapped variant in a score set, calls construct_equivalent_variants from
the variant-annotation library to produce the coding/genomic HGVS candidates
encoding the same protein consequence, plus that protein consequence itself. Each
candidate is written as a non-authoritative Allele row linked to the existing
MappingRecord via MappingRecordAllele.

The library returns the equivalence class as a list of ProjectionPair objects,
each one projection pair (a coding candidate and its deterministic genomic projection
— the same change at two levels). The job preserves that pairing by stamping both
links of a projection pair with a shared per-record ``projection_group`` id; the protein
apex is shared across all projection pairs and carries no group. Where a projection pair member
equals the record's authoritative (measured) allele, its group is folded onto the
existing authoritative link rather than duplicated, so the canonical c/g projection
is resolvable from the authoritative allele's group downstream.
"""

import asyncio
import dataclasses
import functools
import logging
from collections import Counter
from datetime import date
from typing import Any, Callable, NamedTuple, Sequence

from ga4gh.vrs.extras.translator import AlleleTranslator
from sqlalchemy import select
from sqlalchemy.orm import Session
from variant_annotation import __version__ as variant_annotation_version
from variant_annotation.lib.accessions import looks_like_refseq_protein_accession
from variant_annotation.lib.translation import construct_equivalent_variants
from variant_annotation.lib.translation.types import (
    TranslationConfig,
    TranslationErrorReason,
    VariantInput,
    WtCodonMode,
)

from mavedb.lib.annotation_status_manager import AnnotationStatusManager
from mavedb.lib.hgvs import extract_accession, strip_protein_prediction_parens
from mavedb.lib.types.workflow import JobExecutionOutcome
from mavedb.lib.variant_translations import get_or_create_allele
from mavedb.lib.vrs_utils import translate_hgvs_to_variation
from mavedb.models.allele import Allele as AlleleDbModel
from mavedb.models.enums.sequence_level import SequenceLevel
from mavedb.models.enums.annotation_type import AnnotationType
from mavedb.models.enums.disposition import Disposition
from mavedb.models.enums.event_reason import EventReason
from mavedb.models.enums.job_pipeline import FailureCategory
from mavedb.models.enums.target_category import TargetCategory
from mavedb.models.mapping_record import MappingRecord
from mavedb.models.mapping_record_allele import MappingRecordAllele
from mavedb.models.score_set import ScoreSet
from mavedb.models.target_gene import TargetGene
from mavedb.models.target_gene_mapping import TargetGeneMapping
from mavedb.models.variant import Variant
from mavedb.worker.jobs.utils.setup import validate_job_params
from mavedb.worker.lib.decorators.pipeline_management import with_pipeline_management
from mavedb.worker.lib.managers.job_manager import JobManager
from mavedb.worker.lib.translation_ports import WorkerCoordinateTranslator, uta_transcript_source

logger = logging.getLogger(__name__)

# Job defaults: full coding equivalence class — every synonymous codon, indels included.
# wt_codon_mode "all" requires include_indels=True.
_DEFAULT_TRANSLATION_CONFIG: dict[str, Any] = {
    "include_indels": True,
    "wt_codon_mode": WtCodonMode.ALL,
}


class _TranscriptResolution(NamedTuple):
    """Pairs a mapping record with its pre-UTA transcript info: gene_transcript if the mapper
    supplied a cdna row, protein_accession if NP_→NM_ resolution is still needed. Both None
    means the record will be skipped."""

    rec: MappingRecord
    variant: Variant
    gene_transcript: str | None
    protein_accession: str | None
    target_gene_id: int | None


def _classify_skip(
    resolution: _TranscriptResolution, category: TargetCategory | None
) -> tuple[EventReason, Disposition]:
    """Classify why a record was skipped, returning its reason and event disposition.

    ``no_assay_level_hgvs`` — no input to translate, we could not ask → ``not_applicable``.
    ``transcript_unresolved`` — protein-coding target with no transcript, a recoverable pipeline
    gap → ``failed``. ``no_coding_transcript`` — non-coding target has no protein consequence, a
    biological negative → ``absent``.
    """
    if not resolution.rec.hgvs_assay_level:
        return (EventReason.NO_ASSAY_LEVEL_HGVS, Disposition.NOT_APPLICABLE)
    if category == TargetCategory.protein_coding:
        return (EventReason.TRANSCRIPT_UNRESOLVED, Disposition.FAILED)
    return (EventReason.NO_CODING_TRANSCRIPT, Disposition.ABSENT)


def _coding_transcripts_for_proteins(protein_accessions: set[str]) -> dict[str, str]:
    """Resolve RefSeq protein accessions (NP_/XP_) to their preferred coding transcripts via UTA.

    Protein-level mappings carry no cdna transcript in TargetGeneMapping, so reverse
    translation falls back to the NP_→NM_ association in UTA. Connection is opened and
    closed per-call to avoid leaking connections across long-lived worker jobs.
    """
    if not protein_accessions:
        return {}

    with uta_transcript_source() as client:
        return {
            pro_ac: transcript
            for pro_ac in sorted(protein_accessions)
            if (transcript := client.transcript_for_protein(pro_ac)) is not None
        }


def _cdna_transcript_resolver(db: Session, score_set_id: int) -> Callable[[int | None, int, date | None], str | None]:
    """Return a resolver from a record's ``(job_run_id, target_gene_id, mapped_date)`` to its cdna transcript.

    A record binds to *its own run's* cdna row via ``job_run_id``. Records with no ``job_run_id`` (pre-column,
    or reshaped from legacy) fall back to the day-granular ``(target_gene_id, mapped_date)`` key, which cannot
    separate two runs on one calendar day. Within either key, highest ``TargetGeneMapping.id`` wins. The two
    lookup tables are built once per score set here and closed over, so per-record resolution is a dict hit.
    """
    by_job_run: dict[tuple[int, int], str | None] = {}
    by_run_date: dict[tuple[int, date | None], str | None] = {}
    for target_gene_id, mapped_date, job_run_id, reference_accession in (
        db.execute(
            select(
                TargetGeneMapping.target_gene_id,
                TargetGeneMapping.mapped_date,
                TargetGeneMapping.job_run_id,
                TargetGeneMapping.reference_accession,
            )
            .join(TargetGene, TargetGene.id == TargetGeneMapping.target_gene_id)
            .where(TargetGene.score_set_id == score_set_id)
            .where(TargetGeneMapping.alignment_level == SequenceLevel.cdna)
            .where(TargetGeneMapping.reference_accession.isnot(None))
            .order_by(TargetGeneMapping.id)
        )
        .tuples()
        .all()
    ):
        if job_run_id is not None:
            by_job_run[(job_run_id, target_gene_id)] = reference_accession
        by_run_date[(target_gene_id, mapped_date)] = reference_accession

    def resolve(job_run_id: int | None, target_gene_id: int, mapped_date: date | None) -> str | None:
        if job_run_id is not None:
            return by_job_run.get((job_run_id, target_gene_id))
        return by_run_date.get((target_gene_id, mapped_date))

    return resolve


def _build_translation_config(overrides: dict[str, Any] | None) -> TranslationConfig:
    """Build a TranslationConfig from optional job-param overrides merged with job defaults.

    Overrides win over _DEFAULT_TRANSLATION_CONFIG; wt_codon_mode accepts string values and
    is coerced to the enum. Raises ValueError for unknown fields, invalid modes, or invalid
    combinations (e.g. wt_codon_mode != "none" without include_indels).
    """
    config_kwargs: dict[str, Any] = {**_DEFAULT_TRANSLATION_CONFIG, **(overrides or {})}

    allowed_fields = {field.name for field in dataclasses.fields(TranslationConfig)}
    unknown_fields = set(config_kwargs) - allowed_fields
    if unknown_fields:
        raise ValueError(
            f"Unknown translation_config option(s): {', '.join(sorted(unknown_fields))}. "
            f"Valid options: {', '.join(sorted(allowed_fields))}."
        )

    raw_mode = config_kwargs.get("wt_codon_mode")
    if raw_mode is not None:
        try:
            config_kwargs["wt_codon_mode"] = WtCodonMode(raw_mode)
        except ValueError:
            valid_modes = ", ".join(repr(mode.value) for mode in WtCodonMode)
            raise ValueError(
                f"Invalid translation_config wt_codon_mode {raw_mode!r}. Valid values: {valid_modes}."
            ) from None

    try:
        return TranslationConfig(**config_kwargs)
    except ValueError as exc:
        raise ValueError(f"Invalid translation_config: {exc}") from exc


def _annotate_translation(
    annotation_manager: AnnotationStatusManager,
    variant_id: int,
    disposition: Disposition,
    reason: EventReason,
    *,
    error_message: str | None = None,
    metadata: dict | None = None,
) -> None:
    """Record one CROSS_LEVEL_TRANSLATION event for an allele (the translation is a variant-level fact).

    The single choke point for RT's status writes.
    """
    meta = dict(metadata or {})
    if error_message is not None:
        meta["error_message"] = error_message

    annotation_manager.record_event(
        AnnotationType.CROSS_LEVEL_TRANSLATION,
        variant_id=variant_id,
        disposition=disposition,
        reason=reason,
        source_version=variant_annotation_version,
        metadata=meta or None,
    )


@with_pipeline_management
async def reverse_translate_variants_for_score_set(
    ctx: dict, job_id: int, job_manager: JobManager
) -> JobExecutionOutcome:
    """Build the cross-level HGVS equivalence class for every mapped variant in the score set.

    For each current MappingRecord with an hgvs_assay_level, collapses to a ProteinConsequence
    and expands to all coding/genomic HGVS candidates via the variant-annotation library.
    Each candidate is written as a non-authoritative Allele linked to the MappingRecord.

    job_params: score_set_id (int), correlation_id (str),
                translation_config (dict, optional) — TranslationConfig overrides.
    """
    job = job_manager.get_job()
    validate_job_params(["score_set_id", "correlation_id"], job)

    score_set_id: int = job.job_params["score_set_id"]  # type: ignore[index]
    correlation_id: str = job.job_params["correlation_id"]  # type: ignore[index]
    translation_config = _build_translation_config(job.job_params.get("translation_config"))  # type: ignore[union-attr]
    score_set = job_manager.db.scalars(select(ScoreSet).where(ScoreSet.id == score_set_id)).one()

    job_manager.save_to_context(
        {
            "application": "mavedb-worker",
            "function": "reverse_translate_variants_for_score_set",
            "resource": score_set.urn,
            "correlation_id": correlation_id,
        }
    )
    job_manager.update_progress(0, 100, "Starting reverse translation job.")
    logger.info(msg="Started reverse translation job.", extra=job_manager.logging_context())

    resolve_cdna_transcript = _cdna_transcript_resolver(job_manager.db, score_set_id)

    # Load current authoritative MappingRecords with their Variant and TargetGeneMapping. The record's own
    # TargetGeneMapping (its measured alignment) carries the run's job_run_id and mapped_date, which anchor
    # the cdna transcript lookup below.
    rows: Sequence[tuple[MappingRecord, Variant, int, date | None, int | None]] = (
        job_manager.db.execute(
            select(
                MappingRecord,
                Variant,
                TargetGeneMapping.target_gene_id,
                TargetGeneMapping.mapped_date,
                TargetGeneMapping.job_run_id,
            )
            .join(MappingRecordAllele, MappingRecord.id == MappingRecordAllele.mapping_record_id)
            .join(Variant, MappingRecord.variant_id == Variant.id)
            .outerjoin(TargetGeneMapping, MappingRecord.target_gene_mapping_id == TargetGeneMapping.id)
            .where(Variant.score_set_id == score_set_id)
            .where(MappingRecord.current)
            .where(MappingRecordAllele.is_authoritative.is_(True))
            .where(MappingRecordAllele.current)
        )
        .tuples()
        .all()
    )

    annotation_counts: Counter[str] = Counter({"translated": 0, "failed": 0, "skipped": 0, "alleles_created": 0})

    if not rows:
        logger.warning(
            msg="No current and authoritative mapping records found for this score set.",
            extra=job_manager.logging_context(),
        )
        job_manager.db.flush()
        return JobExecutionOutcome.succeeded(data=dict(annotation_counts))

    # Genomic/cdna records resolve via the cdna TargetGeneMapping; protein records have no
    # cdna reference_accession, so collect their NP_ accessions for a batched NP_→NM_ UTA lookup.
    transcript_resolutions: list[_TranscriptResolution] = []
    protein_accessions: set[str] = set()
    for rec, variant, target_gene_id, mapped_date, record_job_run_id in rows:
        coding_accession = resolve_cdna_transcript(record_job_run_id, target_gene_id, mapped_date)
        protein_accession = None
        if not coding_accession and rec.hgvs_assay_level is not None:
            raw_accession = extract_accession(rec.hgvs_assay_level)
            if looks_like_refseq_protein_accession(raw_accession):
                protein_accession = raw_accession
                protein_accessions.add(raw_accession)

        transcript_resolutions.append(
            _TranscriptResolution(rec, variant, coding_accession, protein_accession, target_gene_id)
        )

    transcript_by_protein = _coding_transcripts_for_proteins(protein_accessions)

    # Target category per gene — used to classify skips as recoverable (coding target,
    # transcript unresolved) vs. correct (non-coding/regulatory, no protein consequence).
    target_category_by_gene: dict[int, TargetCategory] = dict(
        job_manager.db.execute(
            select(TargetGene.id, TargetGene.category).where(TargetGene.score_set_id == score_set_id)
        )
        .tuples()
        .all()
    )

    # Build VariantInputs with the resolved coding transcript (p./c./g. all collapse to a
    # ProteinConsequence on that transcript). Object identity is preserved so TranslationResult
    # can be correlated back to its MappingRecord. Records with no transcript are skipped.
    variant_inputs: list[Any] = []
    variant_input_map: dict[int, tuple[MappingRecord, Variant]] = {}
    skipped_variants: list[_TranscriptResolution] = []
    for p in transcript_resolutions:
        transcript = p.gene_transcript or (
            transcript_by_protein.get(p.protein_accession) if p.protein_accession else None
        )
        if not transcript or not p.rec.hgvs_assay_level:
            skipped_variants.append(p)
            continue

        inp = VariantInput(hgvs=p.rec.hgvs_assay_level, transcript=transcript)
        variant_inputs.append(inp)
        variant_input_map[id(inp)] = (p.rec, p.variant)

    total = len(variant_inputs)
    job_manager.save_to_context({"total_variants": total, "skipped_variants": len(skipped_variants)})
    job_manager.update_progress(
        10,
        100,
        f"Prepared {total} variants for reverse translation ({len(skipped_variants)} skipped, no coding transcript).",
    )
    logger.info(
        msg=f"Running reverse translation for {total} variants ({len(skipped_variants)} skipped).",
        extra=job_manager.logging_context(),
    )

    # construct_equivalent_variants is I/O-bound (blocks on a subprocess); run in the
    # default thread pool rather than the process pool to avoid pickling the port objects.
    coordinates = WorkerCoordinateTranslator(ctx["hdp"])
    loop = asyncio.get_running_loop()
    job_manager.update_progress(20, 100, "Running reverse translation subprocess.")
    # WtCodonMode.ALL reads the reference codon via TranscriptSource.codon_at, so pass a
    # live UTA-backed source. codon_at is only touched in the post-subprocess WT-codon step,
    # but the connection must outlive the executor call, so scope it around the whole call.
    # (transcript_for_protein is redundant here -- the job already resolved every transcript
    # and supplies it via VariantInput.transcript.)
    with uta_transcript_source() as transcripts:
        results, errors = await loop.run_in_executor(
            None,
            functools.partial(
                construct_equivalent_variants,
                variant_inputs,
                transcripts=transcripts,
                coordinates=coordinates,
                config=translation_config,
            ),
        )

    job_manager.update_progress(70, 100, "Writing translated alleles to database.")
    logger.info(
        msg=f"Translation complete: {len(results)} succeeded, {len(errors)} failed.",
        extra=job_manager.logging_context(),
    )

    annotation_manager = AnnotationStatusManager(
        job_manager.db, job_run_id=job_manager.job_id, score_set_id=score_set_id
    )
    allele_translator = AlleleTranslator(ctx["seqrepo"])

    current_record_ids = (
        select(MappingRecord.id)
        .join(Variant, MappingRecord.variant_id == Variant.id)
        .where(Variant.score_set_id == score_set_id)
        .where(MappingRecord.current.is_(True))
    )

    # Defer linkage until all candidates are processed so the prior derived links can be
    # superseded atomically — retire and insert share a timestamp with no gap.
    new_links: list[MappingRecordAllele] = []

    # The live authoritative links for the current records, keyed by (record_id, allele_id).
    # Held as ORM objects (not a bare pair set) because the authoritative fold-in updates them in
    # place: when an RT candidate's allele equals a record's authoritative (measured) allele, that
    # link already exists (is_authoritative=True, projection_group NULL, written by the mapping job)
    # and uq_mapping_record_alleles_live forbids a derived duplicate — so the candidate's group is
    # stamped onto this existing link instead of inserting a new one. Modifying the loaded object
    # emits an UPDATE on flush; supersede_live_where at the end only retires derived links, leaving
    # the authoritative link (and its freshly stamped group) intact.
    authoritative_links: dict[tuple[int, int], MappingRecordAllele] = {
        (link.mapping_record_id, link.allele_id): link
        for link in job_manager.db.scalars(
            select(MappingRecordAllele)
            .where(MappingRecordAllele.is_authoritative.is_(True))
            .where(MappingRecordAllele.current)
            .where(MappingRecordAllele.mapping_record_id.in_(current_record_ids))
        ).all()
    }

    for result in results:
        rec, variant = variant_input_map[id(result.input)]
        candidate_count = 0

        # Equivalence generation may surface the same VRS object more than once.
        # Dedup by vrs_digest per mapping record to avoid duplicate links.
        seen_digests: set[str] = set()
        failed_candidates: list[dict[str, str]] = []

        # Flatten the projection pairs into a work list of (hgvs, level, field, projection_group) members,
        # preserving the coding↔genomic pairing the library emits. Each ProjectionPair's coding and
        # genomic members share the pair's per-record group id — its index in the equivalence class
        # (0..N-1). hgvs_c is always present (the pair key); hgvs_g is None when the c→g projection
        # failed, yielding a well-formed one-member (coding only) group rather than a desync. The
        # group id is assigned once per pair here, so the two members are guaranteed to carry the
        # same id even though they translate and link independently below.
        members: list[tuple[str, SequenceLevel, str, int | None]] = []
        for group_id, pair in enumerate(result.projection_pairs):
            members.append((pair.hgvs_c, SequenceLevel.cdna, "hgvs_c", group_id))
            if pair.hgvs_g is not None:
                members.append((pair.hgvs_g, SequenceLevel.genomic, "hgvs_g", group_id))

        # The protein consequence is the apex of the equivalence set — shared across every
        # pair, a member of none — so it carries no group (None). Prediction parens
        # (p.(Ala222Val)) are stripped before translation and storage. None for protein-assay
        # inputs, where the protein is already the authoritative allele.
        if result.hgvs_p:
            members.append((strip_protein_prediction_parens(result.hgvs_p), SequenceLevel.protein, "hgvs_p", None))

        for hgvs, level, hgvs_field, projection_group in members:
            # A candidate may not be translatable (intronic projection, malformed expression).
            # Track per-candidate failures; a variant fails only if *all* candidates fail.
            try:
                variation = translate_hgvs_to_variation(hgvs, allele_translator)
            except Exception as e:
                logger.warning(
                    msg=f"Failed to translate candidate HGVS to VRS: {hgvs} ({e})",
                    extra=job_manager.logging_context(),
                )
                failed_candidates.append({"hgvs": hgvs, "level": level.value, "error": str(e)})
                continue

            if variation.id in seen_digests:
                continue

            seen_digests.add(variation.id)
            draft_allele = AlleleDbModel(
                vrs_digest=variation.id,
                # exclude_none mirrors the mapper's serialization.
                post_mapped=variation.model_dump(exclude_none=True),
                level=level,
                **{hgvs_field: hgvs},  # type: ignore[arg-type]
            )
            allele = get_or_create_allele(job_manager.db, draft_allele)
            job_manager.db.flush()

            authoritative_link = authoritative_links.get((rec.id, allele.id))
            if authoritative_link is not None:
                # Authoritative fold-in: this member is the record's measured allele, already
                # linked authoritatively. Stamp it's projection group to preserve the c↔g pairing,
                # and skip creating a new derived link.
                authoritative_link.projection_group = projection_group
                continue

            new_links.append(
                MappingRecordAllele(
                    mapping_record_id=rec.id,
                    allele_id=allele.id,
                    is_authoritative=False,
                    projection_group=projection_group,
                )
            )
            candidate_count += 1

        annotation_counts["alleles_created"] += candidate_count
        annotation_metadata = {
            "hgvs_input": result.input.hgvs,
            # Serializable projection of the projection pairs (the pairing the links now encode).
            "candidates": [
                {"hgvs_c": pair.hgvs_c, "hgvs_g": pair.hgvs_g, "variant_type": pair.variant_type}
                for pair in result.projection_pairs
            ],
            "hgvs_p": result.hgvs_p,
            "alleles_created": candidate_count,
            "failed_candidates": failed_candidates,
        }

        # No translatable candidates and failures mean the variant failed reverse translation. No
        # failures and no candidates is a success with no alleles created.
        if candidate_count == 0 and failed_candidates:
            annotation_counts["failed"] += 1
            _annotate_translation(
                annotation_manager,
                variant_id=variant.id,
                disposition=Disposition.FAILED,
                reason=EventReason.TRANSLATION_FAILED,
                error_message="All candidate HGVS failed VRS translation.",
                metadata=annotation_metadata,
            )
        else:
            annotation_counts["translated"] += 1
            _annotate_translation(
                annotation_manager,
                variant_id=variant.id,
                disposition=Disposition.PRESENT,
                reason=EventReason.TRANSLATED,
                metadata=annotation_metadata,
            )

    # Supersede prior live derived links atomically.
    # TODO#765: re-runs retire and recreate the whole derived set because re-mapping re-mints
    # records; idempotent records would allow unchanged links to stay live.
    MappingRecordAllele.supersede_live_where(
        job_manager.db,
        new_links,
        MappingRecordAllele.is_authoritative.is_(False),
        MappingRecordAllele.mapping_record_id.in_(current_record_ids),
    )

    # The library types each error's reason: NOT_TRANSLATABLE is a benign structural gap (the
    # protein consequence's edit type — del/ins/delins/fs/ext/stop-loss — has no DNA equivalence
    # class to construct), everything else is a genuine failure. Split on that typed reason rather
    # than pattern-matching the engine's error text.
    for error in errors:
        _rec, variant = variant_input_map[id(error.input)]
        if error.reason is TranslationErrorReason.NOT_TRANSLATABLE:
            # A structural gap ("we could not ask"), not a failure — count it as a skip and record
            # NOT_APPLICABLE so it never pollutes the failed tally (mirrors NO_ASSAY_LEVEL_HGVS).
            annotation_counts["skipped"] += 1
            _annotate_translation(
                annotation_manager,
                variant_id=variant.id,
                disposition=Disposition.NOT_APPLICABLE,
                reason=EventReason.NOT_TRANSLATABLE,
                metadata={"hgvs_input": error.input.hgvs, "error_message": error.error},
            )
            continue

        annotation_counts["failed"] += 1
        _annotate_translation(
            annotation_manager,
            variant_id=variant.id,
            disposition=Disposition.FAILED,
            reason=EventReason.TRANSLATION_ERROR,
            metadata={"hgvs_input": error.input.hgvs, "error_message": error.error},
        )

    # skipped already holds the NOT_TRANSLATABLE skips counted above; add the transcript-unresolved
    # skips (skipped_variants) rather than overwriting.
    annotation_counts["skipped"] += len(skipped_variants)
    for p in skipped_variants:
        category = target_category_by_gene.get(p.target_gene_id) if p.target_gene_id is not None else None
        reason, disposition = _classify_skip(p, category)
        _annotate_translation(
            annotation_manager,
            variant_id=p.variant.id,
            disposition=disposition,
            reason=reason,
            metadata={"hgvs_input": p.rec.hgvs_assay_level},
        )

    annotation_manager.flush()

    outcome_data = dict(annotation_counts)
    job_manager.save_to_context(outcome_data)
    logger.info(
        msg=(
            f"Reverse translation complete: {annotation_counts['translated']} translated, "
            f"{annotation_counts['failed']} failed, {annotation_counts['skipped']} skipped, "
            f"{annotation_counts['alleles_created']} alleles created."
        ),
        extra=job_manager.logging_context(),
    )
    job_manager.db.flush()

    if annotation_counts["translated"] == 0 and annotation_counts["failed"] > 0:
        logger.error(
            msg="All variant reverse translations failed.",
            extra=job_manager.logging_context(),
        )
        return JobExecutionOutcome.failed(
            reason="All variant reverse translations failed.",
            data=outcome_data,
            failure_category=FailureCategory.DATA_ERROR,
        )

    return JobExecutionOutcome.succeeded(data=outcome_data)
