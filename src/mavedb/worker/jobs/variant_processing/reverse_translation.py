"""Reverse translation worker job — builds cross-level HGVS equivalence classes.

For each mapped variant in a score set, calls construct_equivalent_variants from
the variant-annotation library to produce all coding and genomic HGVS candidates
encoding the same protein consequence. The candidates are written as non-authoritative
Allele rows linked to the existing MappingRecord via MappingRecordAllele.
"""

import asyncio
import contextlib
import dataclasses
import functools
import logging
import os
from typing import Any, NamedTuple, Sequence

from ga4gh.vrs.extras.translator import AlleleTranslator
from sqlalchemy import select
from variant_annotation.lib.accessions import looks_like_refseq_protein_accession
from variant_annotation.lib.clients.uta import UtaClient, connect_uta
from variant_annotation.lib.translation import construct_equivalent_variants
from variant_annotation.lib.translation.types import TranslationConfig, VariantInput, WtCodonMode

from mavedb.lib.annotation_status_manager import AnnotationStatusManager
from mavedb.lib.hgvs import extract_accession
from mavedb.lib.types.workflow import JobExecutionOutcome
from mavedb.lib.variant_translations import get_or_create_allele
from mavedb.lib.vrs_utils import translate_hgvs_to_variation
from mavedb.models.allele import Allele as AlleleDbModel
from mavedb.models.enums.annotation_layer import AnnotationLayer
from mavedb.models.enums.annotation_type import AnnotationType
from mavedb.models.enums.job_pipeline import AnnotationFailureCategory, AnnotationStatus, FailureCategory
from mavedb.models.mapping_record import MappingRecord
from mavedb.models.mapping_record_allele import MappingRecordAllele
from mavedb.models.score_set import ScoreSet
from mavedb.models.target_gene import TargetGene
from mavedb.models.target_gene_mapping import TargetGeneMapping
from mavedb.models.variant import Variant
from mavedb.worker.jobs.utils.setup import validate_job_params
from mavedb.worker.lib.decorators.pipeline_management import with_pipeline_management
from mavedb.worker.lib.managers.job_manager import JobManager
from mavedb.worker.lib.translation_ports import NullTranscriptSource, WorkerCoordinateTranslator

logger = logging.getLogger(__name__)

# Job defaults when no `translation_config` override is supplied: enumerate the full coding
# equivalence class — every synonymous codon, indels included. wt_codon_mode "all" requires
# include_indels=True (TranslationConfig enforces this).
_DEFAULT_TRANSLATION_CONFIG: dict[str, Any] = {
    "include_indels": True,
    "wt_codon_mode": WtCodonMode.ALL,
}


class _TranscriptResolution(NamedTuple):
    """A mapping record paired with what we know about its coding transcript before the
    batched UTA lookup: the gene-level cdna transcript if the mapper supplied one, and the
    protein accession still awaiting NP_→NM_ resolution otherwise. Exactly one of the two is
    set for a resolvable record; both are None when neither path applies (record is skipped)."""

    rec: MappingRecord
    variant: Variant
    gene_transcript: str | None
    protein_accession: str | None


def _coding_transcripts_for_proteins(protein_accessions: set[str]) -> dict[str, str]:
    """Resolve each RefSeq protein accession (NP_/XP_…) to its preferred coding transcript
    via UTA's associated_accessions table.

    Protein-level mappings carry no coding transcript — the mapper emits reference_accession
    only for cdna alignments — so reverse-translating them relies on the NP_→NM_ association
    UTA records. Resolution and the multi-transcript preference order both live in the
    variant-annotation library's UtaClient; we own only the connection lifecycle here so the
    long-lived worker process does not leak UTA connections across jobs.
    """
    if not protein_accessions:
        return {}

    uta_db_url = (os.environ.get("UTA_DB_URL") or "").strip()
    if not uta_db_url:
        raise RuntimeError("UTA_DB_URL must be set to resolve protein→transcript associations.")

    with contextlib.closing(connect_uta(uta_db_url)) as conn:
        client = UtaClient(conn)
        return {
            pro_ac: transcript
            for pro_ac in sorted(protein_accessions)
            if (transcript := client.transcript_for_protein(pro_ac)) is not None
        }


def _build_translation_config(overrides: dict[str, Any] | None) -> TranslationConfig:
    """Build the variant-annotation TranslationConfig from optional job-param overrides.

    Each key in `overrides` is a TranslationConfig field (that dataclass is the source of truth
    for the available knobs) and wins over the job defaults in _DEFAULT_TRANSLATION_CONFIG;
    absent or None means use the defaults. wt_codon_mode is coerced to the enum so a JSON string
    value ("all"/"unambiguous"/"none") works.

    Raises ValueError with an actionable message — listing the offending value and the valid
    options — for an unknown field, an invalid wt_codon_mode, or an invalid combination (e.g. a
    wt_codon_mode other than "none" without include_indels). The allowed-field set is derived
    from TranslationConfig itself, so it never drifts from the library.
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


@with_pipeline_management
async def reverse_translate_variants_for_score_set(
    ctx: dict, job_id: int, job_manager: JobManager
) -> JobExecutionOutcome:
    """Build the cross-level HGVS equivalence class for every mapped variant in the score set.

    Reads current MappingRecords that carry an hgvs_assay_level string, collapses each
    to its ProteinConsequence, and expands to all coding/genomic HGVS candidates via a
    single batched subprocess call to the variant-annotation library. Each candidate is
    written as a non-authoritative Allele linked to the MappingRecord.

    Required job_params:
        - score_set_id (int): ID of the ScoreSet to process
        - correlation_id (str): Correlation ID for tracking

    Optional job_params:
        - translation_config (dict): Overrides for any variant-annotation TranslationConfig
          field (e.g. include_indels, wt_codon_mode, max_indel_size). Omitted keys fall back to
          the job defaults in _DEFAULT_TRANSLATION_CONFIG (full codon equivalence class, indels
          included).
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

    # Build {target_gene_id -> NM_ transcript} from the cdna TargetGeneMappings the mapper
    # emits. Reverse translation must run against the cdna (NM_) transcript.
    cdna_transcript_by_gene: dict[int, str | None] = dict(
        job_manager.db.execute(
            select(TargetGeneMapping.target_gene_id, TargetGeneMapping.reference_accession)
            .join(TargetGene, TargetGene.id == TargetGeneMapping.target_gene_id)
            .where(TargetGene.score_set_id == score_set_id)
            .where(TargetGeneMapping.alignment_level == AnnotationLayer.cdna)
            .where(TargetGeneMapping.reference_accession.isnot(None))
        )
        .tuples()
        .all()
    )

    # Load current, authoritative, and successfully-mapped MappingRecords along with their
    # target gene (for the coding-transcript lookup) and parent Variant.
    rows: Sequence[tuple[MappingRecord, Variant, int]] = (
        job_manager.db.execute(
            select(MappingRecord, Variant, TargetGeneMapping.target_gene_id)
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

    if not rows:
        logger.warning(
            msg="No current and authoritative mapping records found for this score set.",
            extra=job_manager.logging_context(),
        )
        job_manager.db.flush()
        return JobExecutionOutcome.succeeded(data={"translated": 0, "failed": 0, "skipped": 0, "alleles_created": 0})

    # Resolve each record's coding transcript. Genomic/cdna mappings share their target
    # gene's cdna alignment reference; protein mappings have none (the mapper emits
    # reference_accession only for cdna alignments), so gather their protein accessions and
    # resolve NP_→NM_ from UTA in a single batch query below.
    transcript_resolutions: list[_TranscriptResolution] = []
    protein_accessions: set[str] = set()
    for rec, variant, target_gene_id in rows:
        coding_accession = cdna_transcript_by_gene.get(target_gene_id)
        protein_accession = None
        if not coding_accession and rec.hgvs_assay_level is not None:
            raw_accession = extract_accession(rec.hgvs_assay_level)
            if looks_like_refseq_protein_accession(raw_accession):
                protein_accession = raw_accession
                protein_accessions.add(raw_accession)

        transcript_resolutions.append(_TranscriptResolution(rec, variant, coding_accession, protein_accession))

    transcript_by_protein = _coding_transcripts_for_proteins(protein_accessions)

    # Build VariantInputs, supplying the resolved coding transcript for every input
    # regardless of its own assay level (p./c./g. all collapse to a ProteinConsequence
    # anchored on that transcript). Object identity on each VariantInput is preserved
    # through the call so we can correlate TranslationResult.input back to its MappingRecord.
    #
    # A record with no coding transcript (e.g. a regulatory element aligned only at the
    # genomic level, or a protein with no UTA association) has no protein consequence to
    # reverse-translate; it is skipped and recorded as SKIPPED rather than counted as a failure.
    variant_inputs: list[Any] = []
    variant_input_map: dict[int, tuple[MappingRecord, Variant]] = {}
    skipped_variants: list[tuple[MappingRecord, Variant]] = []
    for p in transcript_resolutions:
        transcript = p.gene_transcript or (
            transcript_by_protein.get(p.protein_accession) if p.protein_accession else None
        )
        if not transcript or not p.rec.hgvs_assay_level:
            skipped_variants.append((p.rec, p.variant))
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
    transcripts = NullTranscriptSource()
    loop = asyncio.get_running_loop()
    job_manager.update_progress(20, 100, "Running reverse translation subprocess.")
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

    translated = 0
    failed = 0
    alleles_created = 0
    annotation_manager = AnnotationStatusManager(job_manager.db, job_run_id=job_manager.job_id)
    allele_translator = AlleleTranslator(ctx["seqrepo"])

    current_record_ids = (
        select(MappingRecord.id)
        .join(Variant, MappingRecord.variant_id == Variant.id)
        .where(Variant.score_set_id == score_set_id)
        .where(MappingRecord.current.is_(True))
    )

    # Collect new MappingRecord -> Allele links and defer linkage until all candidates are processed.
    # This allows us to supersede the prior live derived links with the new set in one atomic operation at the end,
    # so the retire and insert share a timestamp and there is no gap between the old and new sets of live links.
    new_links: list[MappingRecordAllele] = []

    # The live authoritative (record, allele) pairs. A derived candidate that equals a record's
    # authoritative allele is not linked again.
    authoritative_pairs: set[tuple[int, int]] = {
        (record_id, allele_id)
        for record_id, allele_id in job_manager.db.execute(
            select(MappingRecordAllele.mapping_record_id, MappingRecordAllele.allele_id)
            .where(MappingRecordAllele.is_authoritative.is_(True))
            .where(MappingRecordAllele.current)
            .where(MappingRecordAllele.mapping_record_id.in_(current_record_ids))
        )
        .tuples()
        .all()
        if record_id is not None and allele_id is not None
    }

    for result in results:
        rec, variant = variant_input_map[id(result.input)]
        candidate_count = 0

        # Equivalence generation may surface the same VRS object more than once.
        # Dedup by vrs_digest per mapping record to avoid duplicate links.
        seen_digests: set[str] = set()
        failed_candidates: list[dict[str, str]] = []
        candidates: list[tuple[str, AnnotationLayer, str]] = [
            (hgvs_g, AnnotationLayer.genomic, "hgvs_g") for hgvs_g in result.hgvs_g_candidates
        ] + [(hgvs_c, AnnotationLayer.cdna, "hgvs_c") for hgvs_c in result.hgvs_c_candidates]

        for hgvs, level, hgvs_field in candidates:
            # A candidate the equivalence class produced may be a form ga4gh cannot
            # translate (an intronic projection, an unsupported edge case, a malformed
            # bracketed expression). A variant with at least one translatable candidate still
            # advances the pipeline; only a variant where every candidate fails is
            # recorded as failed (with the per-candidate errors retained as metadata).
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
                post_mapped=variation.model_dump(),
                level=level,
                **{hgvs_field: hgvs},  # type: ignore[arg-type]
            )
            allele = get_or_create_allele(job_manager.db, draft_allele)
            job_manager.db.flush()

            if (rec.id, allele.id) in authoritative_pairs:
                continue  # already linked

            new_links.append(
                MappingRecordAllele(
                    mapping_record_id=rec.id,
                    allele_id=allele.id,
                    is_authoritative=False,
                )
            )
            candidate_count += 1

        alleles_created += candidate_count
        annotation_metadata = {
            "hgvs_input": result.input.hgvs,
            "hgvs_c_candidates": result.hgvs_c_candidates,
            "hgvs_g_candidates": result.hgvs_g_candidates,
            "alleles_created": candidate_count,
            "failed_candidates": failed_candidates,
        }

        # No translatable candidates and failures mean the variant failed reverse translation. No
        # failures and no candidates is a success with no alleles created.
        if candidate_count == 0 and failed_candidates:
            failed += 1
            annotation_manager.add_annotation(
                variant_id=variant.id,
                annotation_type=AnnotationType.CROSS_LEVEL_TRANSLATION,
                status=AnnotationStatus.FAILED,
                failure_category=AnnotationFailureCategory.UNKNOWN,
                annotation_data={
                    "error_message": "All candidate HGVS failed VRS translation.",
                    "annotation_metadata": annotation_metadata,
                },
            )
        else:
            translated += 1
            annotation_manager.add_annotation(
                variant_id=variant.id,
                annotation_type=AnnotationType.CROSS_LEVEL_TRANSLATION,
                status=AnnotationStatus.SUCCESS,
                annotation_data={"annotation_metadata": annotation_metadata},
            )

    # Supersede the prior live derived links with the new set in one gap-free operation.
    # TODO#765: a re-run supersedes the whole derived set wholesale because re-mapping re-mints the
    # records, so we cannot tell which derivations are unchanged; idempotent mapping records would let
    # unchanged derived links stay live instead of being retired and recreated.
    MappingRecordAllele.supersede_live_where(
        job_manager.db,
        new_links,
        MappingRecordAllele.is_authoritative.is_(False),
        MappingRecordAllele.mapping_record_id.in_(current_record_ids),
    )

    for error in errors:
        _rec, variant = variant_input_map[id(error.input)]
        failed += 1
        annotation_manager.add_annotation(
            variant_id=variant.id,
            annotation_type=AnnotationType.CROSS_LEVEL_TRANSLATION,
            status=AnnotationStatus.FAILED,
            failure_category=AnnotationFailureCategory.UNKNOWN,
            annotation_data={
                "error_message": error.error,
                "annotation_metadata": {"hgvs_input": error.input.hgvs},
            },
        )

    skipped = len(skipped_variants)
    for rec, variant in skipped_variants:
        annotation_manager.add_annotation(
            variant_id=variant.id,
            annotation_type=AnnotationType.CROSS_LEVEL_TRANSLATION,
            status=AnnotationStatus.SKIPPED,
            annotation_data={
                "annotation_metadata": {
                    "hgvs_input": rec.hgvs_assay_level,
                    "reason": "No coding transcript for target gene; no protein consequence to reverse-translate.",
                }
            },
        )

    annotation_manager.flush()

    job_manager.save_to_context(
        {
            "translated": translated,
            "failed": failed,
            "skipped": skipped,
            "alleles_created": alleles_created,
        }
    )
    logger.info(
        msg=(
            f"Reverse translation complete: {translated} translated, {failed} failed, "
            f"{skipped} skipped, {alleles_created} alleles created."
        ),
        extra=job_manager.logging_context(),
    )
    job_manager.db.flush()

    if translated == 0 and failed > 0:
        logger.error(
            msg="All variant reverse translations failed.",
            extra=job_manager.logging_context(),
        )
        return JobExecutionOutcome.failed(
            reason="All variant reverse translations failed.",
            data={"translated": 0, "failed": failed, "skipped": skipped, "alleles_created": 0},
            failure_category=FailureCategory.DATA_ERROR,
        )

    return JobExecutionOutcome.succeeded(
        data={"translated": translated, "failed": failed, "skipped": skipped, "alleles_created": alleles_created}
    )
