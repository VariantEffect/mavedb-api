"""Reverse translation worker job — builds cross-level HGVS equivalence classes.

First refreshes every record's measured projection group (see :mod:`mavedb.worker.lib.projection`), the
measured variant described at each level. Then, for each mapped variant in the score set, calls
construct_equivalent_variants from the variant-annotation library to produce the coding/genomic HGVS
candidates encoding the same protein consequence, plus that protein consequence itself. Each candidate is
written as a non-authoritative Allele row linked to the existing MappingRecord via MappingRecordAllele.

The library returns the equivalence class as a list of ProjectionPair objects, each a coding candidate and
its deterministic genomic projection — the same change at two levels. Each pair becomes a candidate
ProjectionGroup row, and both of its links point at it. A pair holding the measured allele is the measured
variant itself, so its other member joins the measured group instead; so does the protein consequence.
"""

import asyncio
import dataclasses
import functools
import logging
from collections import Counter
from datetime import date
from typing import Any, Callable, NamedTuple, Sequence

from ga4gh.vrs.extras.translator import AlleleTranslator
from sqlalchemy import func, select
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
from mavedb.lib.hgvs import extract_accession, is_cis_phased_hgvs, strip_protein_prediction_parens
from mavedb.lib.types.workflow import JobExecutionOutcome
from mavedb.lib.variant_translations import get_or_create_allele
from mavedb.lib.vrs_utils import translate_hgvs_to_variation, verify_allele_refget
from mavedb.models.allele import Allele as AlleleDbModel
from mavedb.models.enums.sequence_level import NUCLEOTIDE_LEVELS, SequenceLevel
from mavedb.models.enums.annotation_type import AnnotationType
from mavedb.models.enums.disposition import Disposition
from mavedb.models.enums.event_reason import EventReason
from mavedb.models.enums.job_pipeline import FailureCategory
from mavedb.models.enums.target_category import TargetCategory
from mavedb.models.mapping_record import MappingRecord
from mavedb.models.mapping_record_allele import MappingRecordAllele
from mavedb.models.projection_group import ProjectionGroup
from mavedb.models.score_set import ScoreSet
from mavedb.models.target_accession import TargetAccession
from mavedb.models.target_gene import TargetGene
from mavedb.models.target_gene_mapping import TargetGeneMapping
from mavedb.models.variant import Variant
from mavedb.worker.jobs.utils.setup import validate_job_params
from mavedb.worker.lib.decorators.pipeline_management import with_pipeline_management
from mavedb.worker.lib.managers.job_manager import JobManager
from mavedb.worker.lib.projection import project_measured_variants
from mavedb.worker.lib.translation_ports import WorkerCoordinateTranslator, uta_transcript_source

logger = logging.getLogger(__name__)

TranscriptResolver = Callable[[int | None, int, date | None, str | None], str | None]

# Job defaults: full coding equivalence class — every synonymous codon, indels included.
# wt_codon_mode "all" requires include_indels=True.
_DEFAULT_TRANSLATION_CONFIG: dict[str, Any] = {
    "include_indels": True,
    "wt_codon_mode": WtCodonMode.ALL,
    # The public UTA server refuses connections for minutes at a time; the library default (about 6 s
    # in total) fails the whole batch on a blip. Waits 5, 10, 20, 40 and 80 s before giving up.
    "upstream_max_attempts": 6,
    "upstream_retry_backoff_seconds": 5.0,
}

# A protein-coding target with no MANE Select transcript at its first loci (an intergenic tile, say) is
# given up on after this many lookups rather than queried once per record.
_MANE_ATTEMPTS_PER_TARGET = 5


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


def cdna_transcript_resolver(
    db: Session, score_set_id: int, coordinates: WorkerCoordinateTranslator
) -> TranscriptResolver:
    """Return a resolver from a record's ``(job_run_id, target_gene_id, mapped_date, genomic_hgvs)`` to its
    cdna transcript.

    In order:

    1. The run's own cdna mapping row. A record binds to it via ``job_run_id``; records with no
       ``job_run_id`` (pre-column, or reshaped from legacy) use the day-granular
       ``(target_gene_id, mapped_date)`` key, which cannot separate two runs on one calendar day. Within
       either key, highest ``TargetGeneMapping.id`` wins.
    2. For records with no ``job_run_id``, the coding accession the legacy mapper recorded in the target's
       ``post_mapped_metadata``: legacy mappings wrote no cdna row, and their genomic HGVS came from that
       same run.
    3. For genomic records, the target's own accession when it is a transcript (NM_, ENST).
    4. For genomic records of a protein-coding target, the MANE Select transcript at the record's locus,
       found once per target gene. Genomic-only targets (most saturation genome editing) have no other
       transcript; this is the one the legacy ClinGen HGVS lookup reported for them.

    Protein records pass no ``genomic_hgvs``, so they stop after step 2 and keep their NP_→NM_ lookup. The
    lookup tables are built once per score set here and closed over. A failed MANE Select lookup raises
    rather than resolving to no transcript, which would erase a coding projection an earlier run wrote.
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

    legacy_by_target_gene: dict[int, str] = {}
    accession_by_target_gene: dict[int, str] = {}
    protein_coding: set[int] = set()
    for target_gene_id, metadata, category, target_accession in (
        db.execute(
            select(TargetGene.id, TargetGene.post_mapped_metadata, TargetGene.category, TargetAccession.accession)
            .outerjoin(TargetAccession, TargetAccession.id == TargetGene.accession_id)
            .where(TargetGene.score_set_id == score_set_id)
        )
        .tuples()
        .all()
    ):
        cdna = metadata.get("cdna") if isinstance(metadata, dict) else None
        accessions = cdna.get("sequence_accessions") if isinstance(cdna, dict) else None
        if accessions:
            legacy_by_target_gene[target_gene_id] = accessions[0]
        if target_accession and target_accession.startswith(("NM_", "ENST")):
            accession_by_target_gene[target_gene_id] = target_accession
        if category == TargetCategory.protein_coding:
            protein_coding.add(target_gene_id)

    mane_by_target_gene: dict[int, str] = {}
    mane_attempts: Counter[int] = Counter()

    def mane_select(target_gene_id: int, genomic_hgvs: str) -> str | None:
        if target_gene_id in mane_by_target_gene:
            return mane_by_target_gene[target_gene_id]
        if target_gene_id not in protein_coding or mane_attempts[target_gene_id] >= _MANE_ATTEMPTS_PER_TARGET:
            return None
        transcript = coordinates.mane_select_transcript(genomic_hgvs)
        mane_attempts[target_gene_id] += 1
        if transcript is not None:
            mane_by_target_gene[target_gene_id] = transcript
        return transcript

    def resolve(
        job_run_id: int | None, target_gene_id: int, mapped_date: date | None, genomic_hgvs: str | None
    ) -> str | None:
        if job_run_id is not None:
            transcript = by_job_run.get((job_run_id, target_gene_id))
        else:
            transcript = by_run_date.get((target_gene_id, mapped_date)) or legacy_by_target_gene.get(target_gene_id)
        if transcript is not None or genomic_hgvs is None:
            return transcript
        return accession_by_target_gene.get(target_gene_id) or mane_select(target_gene_id, genomic_hgvs)

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

    coordinates = WorkerCoordinateTranslator(ctx["hdp"])
    resolve_cdna_transcript = cdna_transcript_resolver(job_manager.db, score_set_id, coordinates)

    # The measured groups come first: candidates that match the measured variant join its group below.
    projection_counts = project_measured_variants(
        job_manager.db, score_set_id, coordinates=coordinates, resolve_transcript=resolve_cdna_transcript
    )
    logger.info(
        msg=f"Measured projection groups: {dict(projection_counts)}.",
        extra=job_manager.logging_context(),
    )

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

    annotation_counts: Counter[str] = Counter(
        {
            "translated": 0,
            "failed": 0,
            "skipped": 0,
            "alleles_created": 0,
            "fold_in_missed": 0,
        }
    )

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
        genomic_hgvs = rec.hgvs_assay_level if rec.assay_level == SequenceLevel.genomic else None
        coding_accession = resolve_cdna_transcript(record_job_run_id, target_gene_id, mapped_date, genomic_hgvs)
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
    cis_phased_variants: list[_TranscriptResolution] = []
    for p in transcript_resolutions:
        # A cis-phased multivariant HGVS (NC_…:g.[a;b;…]) cannot be forward-translated: the engine's
        # parser rejects the allele list at the opening "[". These are almost always true multi-mutant
        # haplotypes, not single-codon components, so they have no meaningful cross-level sibling triple.
        # Record a benign skip rather than send a guaranteed failure to the engine.
        if p.rec.hgvs_assay_level and is_cis_phased_hgvs(p.rec.hgvs_assay_level):
            cis_phased_variants.append(p)
            continue

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
    job_manager.save_to_context(
        {
            "total_variants": total,
            "skipped_variants": len(skipped_variants),
            "cis_phased_skipped": len(cis_phased_variants),
        }
    )
    job_manager.update_progress(
        10,
        100,
        f"Prepared {total} variants for reverse translation "
        f"({len(skipped_variants)} no coding transcript, {len(cis_phased_variants)} cis-phased).",
    )
    logger.info(
        msg=(
            f"Running reverse translation for {total} variants "
            f"({len(skipped_variants)} skipped no-transcript, {len(cis_phased_variants)} skipped cis-phased)."
        ),
        extra=job_manager.logging_context(),
    )

    # construct_equivalent_variants is I/O-bound (blocks on a subprocess); run in the
    # default thread pool rather than the process pool to avoid pickling the port objects.
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

    # Derived links and candidate groups are deferred until every record is processed, so the prior derived
    # set is superseded atomically: retire and insert share one timestamp.
    new_links: list[MappingRecordAllele] = []
    new_groups: list[tuple[ProjectionGroup, list[MappingRecordAllele]]] = []

    # The live authoritative link of each current record. It points at the record's measured projection
    # group (written by project_measured_variants above), which this job adds alleles to but never rewrites.
    authoritative_by_record: dict[int, MappingRecordAllele] = {
        link.mapping_record_id: link
        for link in job_manager.db.scalars(
            select(MappingRecordAllele)
            .where(MappingRecordAllele.is_authoritative.is_(True))
            .where(MappingRecordAllele.current)
            .where(MappingRecordAllele.mapping_record_id.in_(current_record_ids))
        ).all()
    }
    seqrepo = getattr(ctx["seqrepo"], "sr", None)

    for result in results:
        rec, variant = variant_input_map[id(result.input)]
        authoritative = authoritative_by_record.get(rec.id)
        measured_allele_id = authoritative.allele_id if authoritative is not None else None
        measured_group = authoritative.projection_group if authoritative is not None else None
        # Prediction parens (p.(Ala222Val)) are stripped before translation and storage. None for protein-assay
        # inputs, where the protein is already the authoritative allele.
        hgvs_p = strip_protein_prediction_parens(result.hgvs_p) if result.hgvs_p else None

        candidate_count = 0
        linked_nucleotide = False
        folded_in = False
        # Equivalence generation may surface the same VRS object more than once; link each allele once.
        seen_digests: set[str] = set()
        failed_candidates: list[dict[str, str]] = []
        candidates_metadata: list[dict[str, Any]] = []

        def translated(hgvs: str, level: SequenceLevel, hgvs_field: str) -> AlleleDbModel | None:
            """The allele for one candidate HGVS, or None if it cannot be translated (intronic, malformed)."""
            try:
                variation = translate_hgvs_to_variation(hgvs, allele_translator)
            except Exception as e:
                logger.warning(
                    msg=f"Failed to translate candidate HGVS to VRS: {hgvs} ({e})",
                    extra=job_manager.logging_context(),
                )
                failed_candidates.append({"hgvs": hgvs, "level": level.value, "error": str(e)})
                return None

            # Verify that the allele's sequence matches what SeqRepo holds for its accession.
            if seqrepo is not None:
                verify_allele_refget(
                    variation.model_dump(exclude_none=True), hgvs, seqrepo, subject=f"variant record {rec.id}"
                )

            # translate_hgvs_to_variation identifies every variation it returns.
            assert variation.id is not None
            allele = get_or_create_allele(
                job_manager.db,
                AlleleDbModel(
                    vrs_digest=variation.id,
                    # exclude_none mirrors the mapper's serialization.
                    post_mapped=variation.model_dump(exclude_none=True),
                    level=level,
                    **{hgvs_field: hgvs},  # type: ignore[arg-type]
                ),
            )
            job_manager.db.flush()
            return allele

        def link(allele: AlleleDbModel) -> MappingRecordAllele:
            seen_digests.add(allele.vrs_digest)
            return MappingRecordAllele(
                mapping_record_id=rec.id, score_set_id=rec.score_set_id, allele_id=allele.id, is_authoritative=False
            )

        # Each ProjectionPair is a coding candidate and its genomic projection: the same change at two levels.
        # hgvs_g is None when the c→g projection failed, leaving a one-member group.
        for pair in result.projection_pairs:
            members = [(pair.hgvs_c, SequenceLevel.cdna, "hgvs_c")]
            if pair.hgvs_g is not None:
                members.append((pair.hgvs_g, SequenceLevel.genomic, "hgvs_g"))
            alleles = [
                allele for hgvs, level, field in members if (allele := translated(hgvs, level, field)) is not None
            ]
            # Unseen alleles, once each: a pair's two members can identify to one VRS object.
            fresh = list({a.vrs_digest: a for a in alleles if a.vrs_digest not in seen_digests}.values())

            # A pair holding the measured allele is the measured variant itself: its other member joins the
            # measured group rather than forming a candidate group.
            is_measured = measured_allele_id is not None and any(a.id == measured_allele_id for a in alleles)
            candidates_metadata.append(
                {
                    "hgvs_c": pair.hgvs_c,
                    "hgvs_g": pair.hgvs_g,
                    "variant_type": pair.variant_type,
                    "measured": is_measured,
                }
            )
            if is_measured:
                folded_in = True
                for allele in fresh:
                    if allele.id != measured_allele_id:
                        partner = link(allele)
                        partner.projection_group = measured_group
                        new_links.append(partner)
                        candidate_count += 1
                continue

            if fresh:
                group = ProjectionGroup(
                    mapping_record_id=rec.id,
                    score_set_id=rec.score_set_id,
                    hgvs_c=pair.hgvs_c,
                    hgvs_g=pair.hgvs_g,
                    hgvs_p=hgvs_p,
                )
                new_groups.append((group, [link(allele) for allele in fresh]))
                candidate_count += len(fresh)
                linked_nucleotide = True

        # The protein consequence is the measured variant's own, so it joins the measured group.
        if hgvs_p is not None:
            protein = translated(hgvs_p, SequenceLevel.protein, "hgvs_p")
            if protein is not None and protein.id != measured_allele_id and protein.vrs_digest not in seen_digests:
                apex = link(protein)
                apex.projection_group = measured_group
                new_links.append(apex)
                candidate_count += 1

        # A nucleotide measured allele that RT linked candidates to but never matched: its candidates are distinct
        # encodings of the consequence. Expected for changes RT's codon-level candidates can't express; otherwise
        # the mapper and RT produced different VRS objects for one change.
        fold_in_missed = (
            linked_nucleotide and not folded_in and authoritative is not None and rec.assay_level in NUCLEOTIDE_LEVELS
        )
        annotation_counts["fold_in_missed"] += fold_in_missed

        annotation_counts["alleles_created"] += candidate_count
        annotation_metadata = {
            "hgvs_input": result.input.hgvs,
            "candidates": candidates_metadata,
            "hgvs_p": result.hgvs_p,
            "alleles_created": candidate_count,
            "failed_candidates": failed_candidates,
            "fold_in_missed": fold_in_missed,
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

    # A record the library couldn't ask about because UTA was unavailable keeps its derived links and
    # candidate groups: its inputs are unchanged since the run that produced them, so they are still that
    # run's answer. Every other outcome, including a skip or a failed translation, is this run's answer and
    # replaces the prior set.
    unavailable_record_ids = {
        variant_input_map[id(error.input)][0].id
        for error in errors
        if error.reason is TranslationErrorReason.UPSTREAM_UNAVAILABLE
    }
    measured_group_ids = (
        select(MappingRecordAllele.projection_group)
        .where(MappingRecordAllele.is_authoritative.is_(True))
        .where(MappingRecordAllele.current)
        .where(MappingRecordAllele.projection_group.isnot(None))
        .where(MappingRecordAllele.mapping_record_id.in_(current_record_ids))
    )

    # Supersede the prior candidate groups and derived links under one timestamp. Measured groups belong to
    # project_measured_variants and are never retired here; their derived links are, and are re-added above.
    # TODO#765: re-runs retire and recreate the whole derived set because re-mapping re-mints
    # records; idempotent records would allow unchanged links to stay live.
    superseded_at = job_manager.db.scalar(select(func.now()))
    ProjectionGroup.supersede_live_where(
        job_manager.db,
        [group for group, _ in new_groups],
        ProjectionGroup.id.not_in(measured_group_ids),
        ProjectionGroup.mapping_record_id.in_(current_record_ids),
        ProjectionGroup.mapping_record_id.not_in(unavailable_record_ids),
        at=superseded_at,
    )
    job_manager.db.flush()
    for group, links in new_groups:
        for group_link in links:
            group_link.projection_group = group.id
        new_links.extend(links)

    MappingRecordAllele.supersede_live_where(
        job_manager.db,
        new_links,
        MappingRecordAllele.is_authoritative.is_(False),
        MappingRecordAllele.mapping_record_id.in_(current_record_ids),
        MappingRecordAllele.mapping_record_id.not_in(unavailable_record_ids),
        at=superseded_at,
    )

    # The library types each error's reason: NOT_TRANSLATABLE is a benign structural gap (the
    # protein consequence's edit type — ins/delins/dup/fs/ext/stop-loss — has no DNA equivalence
    # class to construct), UPSTREAM_UNAVAILABLE is a UTA outage that outlasted the library's
    # retries, and FAILED is a genuine translation error. Split on that typed reason rather than
    # pattern-matching the engine's error text.
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
            # An upstream outage says nothing about the variant, so keep it out of translation_error.
            reason=(
                EventReason.API_ERROR
                if error.reason is TranslationErrorReason.UPSTREAM_UNAVAILABLE
                else EventReason.TRANSLATION_ERROR
            ),
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

    # add cis-phased multivariant inputs as NOT_APPLICABLE skips
    annotation_counts["skipped"] += len(cis_phased_variants)
    for p in cis_phased_variants:
        _annotate_translation(
            annotation_manager,
            variant_id=p.variant.id,
            disposition=Disposition.NOT_APPLICABLE,
            reason=EventReason.CIS_PHASED_UNSUPPORTED,
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

    if annotation_counts["fold_in_missed"]:
        logger.warning(
            msg=(
                f"{annotation_counts['fold_in_missed']} measured alleles matched none of their reverse translation "
                "candidates; their events carry fold_in_missed in metadata."
            ),
            extra=job_manager.logging_context(),
        )

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
