"""Measured projection groups: each mapping record's measured variant described at every level it has.

A record's measured group holds its measured variant's HGVS at the measured level and its projections to
the other levels: the coding HGVS of a genomic measurement (``g_to_c`` on the record's transcript) or the
genomic HGVS of a coding one (``c_to_g``), plus the protein consequence (``c_to_p``). They are computed from
the mapping alone, so every nucleotide record gets them, including variants reverse translation cannot
reach: intronic and UTR changes, multi-codon indels, start and stop changes.

The group carries strings only. Alleles join it through their links: the authoritative link here, the rest
from reverse translation when it constructs the same change (see the reverse translation job).
"""

import logging
import re
from collections import Counter
from dataclasses import dataclass
from datetime import date
from typing import Callable, Optional, cast

from sqlalchemy import CursorResult, select, true
from sqlalchemy.orm import Session

from mavedb.lib.hgvs import extract_accession, is_cis_phased_hgvs, strip_protein_prediction_parens
from mavedb.models.enums.sequence_level import SequenceLevel
from mavedb.models.mapping_record import MappingRecord
from mavedb.models.mapping_record_allele import MappingRecordAllele
from mavedb.models.projection_group import ProjectionGroup
from mavedb.models.target_gene_mapping import TargetGeneMapping
from mavedb.models.variant import Variant
from mavedb.worker.lib.translation_ports import WorkerCoordinateTranslator

logger = logging.getLogger(__name__)

TranscriptResolver = Callable[[Optional[int], int, Optional[date], Optional[str]], Optional[str]]

_SYNONYMOUS_STOP = re.compile(r"p\.Ter(\d+)Ter$")
_SPANLESS_IDENTITY = re.compile(r"^(?P<accession>[^:]+):g\.(?P<start>\d+)(?P<ref>[ACGTN]{2,})=$")


@dataclass(frozen=True)
class MeasuredHgvs:
    """A measured variant's HGVS at each level; ``None`` where a level has none or could not be projected."""

    hgvs_c: Optional[str] = None
    hgvs_g: Optional[str] = None
    hgvs_p: Optional[str] = None


def _expand_reference_identity(hgvs: str) -> str:
    """Give a multi-base reference identity the span its reference implies.

    Legacy mappings wrote a codon-level identity as ``g.100CCT=``: one position with a three-base
    reference, which hgvs rejects. ``g.100_102=`` is the same variant in valid HGVS.
    """
    match = _SPANLESS_IDENTITY.match(hgvs)
    if match is None:
        return hgvs
    start = int(match["start"])
    return f"{match['accession']}:g.{start}_{start + len(match['ref']) - 1}="


def _is_inversion(hgvs: str) -> bool:
    """Whether hgvs wrote a variant as ``inv``, which VRS cannot represent; projections keep the delins."""
    return hgvs.endswith("inv")


def _protein_consequence(coordinates: WorkerCoordinateTranslator, hgvs_c: str) -> Optional[str]:
    """The protein consequence of a coding variant, or ``None`` when it is unknown (``p.?``: intronic, UTR)."""
    hgvs_p = strip_protein_prediction_parens(coordinates.c_to_p(hgvs_c))
    if hgvs_p.endswith(":p.?"):
        return None

    # hgvs writes a synonymous stop as ``p.Ter778Ter``; HGVS writes every synonymous change with ``=``.
    return _SYNONYMOUS_STOP.sub(r"p.Ter\1=", hgvs_p)


def project_measured_hgvs(
    assay_level: SequenceLevel,
    hgvs: str,
    *,
    transcript: Optional[str],
    coordinates: WorkerCoordinateTranslator,
) -> tuple[MeasuredHgvs, list[str]]:
    """Describe a measured variant at every level, returning the description and any projection errors.

    Each projection is attempted independently, so a failed protein consequence still keeps the coding
    projection. A genomic measurement is normalized before it is projected, and the group carries the
    normalized form; one with no coding transcript has only its genomic HGVS.
    """
    errors: list[str] = []
    if assay_level == SequenceLevel.protein:
        return MeasuredHgvs(hgvs_p=hgvs), errors

    hgvs_c: Optional[str] = None
    hgvs_g: Optional[str] = None
    if assay_level == SequenceLevel.genomic:
        hgvs_g = _expand_reference_identity(hgvs)
        # A reference identity has nothing to trim, and hgvs would collapse its span to one position.
        if not hgvs_g.endswith("="):
            try:
                hgvs_g = coordinates.normalize(hgvs)
            except Exception as e:
                # A variant hgvs cannot normalize (a reference mismatch, say) still projects as written.
                logger.debug("Could not normalize %s: %s", hgvs, e)

        if transcript is not None:
            try:
                hgvs_c = coordinates.g_to_c(hgvs_g, transcript)
                if _is_inversion(hgvs_c):
                    hgvs_c = coordinates.g_to_c_literal(hgvs_g, transcript)
            except Exception as e:
                errors.append(f"g_to_c: {e}")

    else:
        hgvs_c = hgvs
        try:
            hgvs_g = coordinates.c_to_g(hgvs)
            if _is_inversion(hgvs_g):
                hgvs_g = coordinates.c_to_g_literal(hgvs)
        except Exception as e:
            errors.append(f"c_to_g: {e}")

    hgvs_p: Optional[str] = None
    if hgvs_c is not None:
        try:
            hgvs_p = _protein_consequence(coordinates, hgvs_c)
        except Exception as e:
            errors.append(f"c_to_p: {e}")

    return MeasuredHgvs(hgvs_c=hgvs_c, hgvs_g=hgvs_g, hgvs_p=hgvs_p), errors


def project_measured_variants(
    db: Session,
    score_set_id: int,
    *,
    coordinates: WorkerCoordinateTranslator,
    resolve_transcript: TranscriptResolver,
    missing_only: bool = False,
) -> Counter[str]:
    """Write or refresh the measured projection group of every current mapping record in a score set.

    ``missing_only`` restricts the run to records with no measured group yet, skipping the projection of
    records that have one.

    A record whose live measured group already holds the same description is left alone, so a re-run
    writes nothing. Otherwise its group is superseded and its authoritative link is superseded by a copy
    pointing at the new group. A record whose projection raises keeps the group it has, so an unreachable
    data provider cannot erase earlier projections.

    Superseding a group retires the other links in it, because they described the old group: the measured
    variant's partner at the other nucleotide level and its protein consequence, both added by reverse
    translation. Until reverse translation runs again on the score set, the record serves neither allele
    nor the ClinVar, gnomAD and VEP annotations reached through them. Candidate groups are not touched.
    The reverse translation job runs this first, so it re-attaches them in the same run; a standalone run
    reports the retired links as ``detached_rt_links`` and must be followed by reverse translation for
    each score set with a non-zero count. ``missing_only`` runs never supersede a group.

    Does not commit.
    """
    counts: Counter[str] = Counter()
    # Scoped through variants: mapping_records has no score-set index, so filtering on its own score_set_id
    # would scan the table once per score set.
    rows = (
        db.execute(
            select(
                MappingRecord,
                MappingRecordAllele,
                TargetGeneMapping.target_gene_id,
                TargetGeneMapping.mapped_date,
                TargetGeneMapping.job_run_id,
            )
            .join(Variant, Variant.id == MappingRecord.variant_id)
            .join(MappingRecordAllele, MappingRecordAllele.mapping_record_id == MappingRecord.id)
            .outerjoin(TargetGeneMapping, TargetGeneMapping.id == MappingRecord.target_gene_mapping_id)
            .where(Variant.score_set_id == score_set_id)
            .where(MappingRecord.current)
            .where(MappingRecordAllele.is_authoritative.is_(True))
            .where(MappingRecordAllele.current)
            .where(MappingRecordAllele.projection_group.is_(None) if missing_only else true())
        )
        .tuples()
        .all()
    )

    group_ids = [link.projection_group for _, link, *_ in rows if link.projection_group is not None]
    live_groups = {
        group.id: group
        for group in db.scalars(
            select(ProjectionGroup).where(ProjectionGroup.id.in_(group_ids)).where(ProjectionGroup.current)
        )
    }

    pending: list[tuple[MappingRecord, MappingRecordAllele, MeasuredHgvs]] = []
    for record, link, target_gene_id, mapped_date, job_run_id in rows:
        hgvs = record.hgvs_assay_level
        if not hgvs or is_cis_phased_hgvs(hgvs):
            counts["skipped_no_hgvs" if not hgvs else "skipped_cis_phased"] += 1
            continue

        level = SequenceLevel(record.assay_level)
        transcript = None
        resolve_errors: list[str] = []
        if level == SequenceLevel.genomic and target_gene_id is not None:
            try:
                transcript = resolve_transcript(job_run_id, target_gene_id, mapped_date, hgvs)
            except Exception as e:
                resolve_errors.append(f"transcript: {e}")
        elif level == SequenceLevel.cdna:
            transcript = extract_accession(hgvs)

        measured, errors = project_measured_hgvs(level, hgvs, transcript=transcript, coordinates=coordinates)
        errors = resolve_errors + errors
        current = live_groups.get(link.projection_group) if link.projection_group is not None else None
        if errors:
            counts["projection_errors"] += 1
            logger.debug("Projection errors for mapping record %s (%s): %s", record.id, hgvs, errors)
            # An error may be transient (the data provider unreachable), so it never replaces a group a
            # previous run wrote; a record with no group yet still gets the levels that did project.
            if current is not None:
                counts["kept_on_error"] += 1
                continue

        if current is not None and (current.hgvs_c, current.hgvs_g, current.hgvs_p) == (
            measured.hgvs_c,
            measured.hgvs_g,
            measured.hgvs_p,
        ):
            counts["unchanged"] += 1
            continue

        pending.append((record, link, measured))

    if not pending:
        return counts

    # Retire before inserting: the live-unique indexes on links (one per record and allele, one authoritative
    # per record) must see the old rows closed. Both statements and the inserts below take the transaction's
    # now(), so the handoff is gap-free.
    prior_group_ids = [link.projection_group for _, link, _ in pending if link.projection_group is not None]
    if prior_group_ids:
        detached = cast(
            CursorResult,
            db.execute(
                MappingRecordAllele.retire_live_where(
                    MappingRecordAllele.projection_group.in_(prior_group_ids),
                    MappingRecordAllele.is_authoritative.is_(False),
                )
            ),
        )
        if detached.rowcount:
            counts["detached_rt_links"] += detached.rowcount
        db.execute(ProjectionGroup.retire_live_where(ProjectionGroup.id.in_(prior_group_ids)))
    db.execute(MappingRecordAllele.retire_live_where(MappingRecordAllele.id.in_([link.id for _, link, _ in pending])))

    groups = [
        ProjectionGroup(
            mapping_record_id=record.id,
            score_set_id=record.score_set_id,
            hgvs_c=measured.hgvs_c,
            hgvs_g=measured.hgvs_g,
            hgvs_p=measured.hgvs_p,
        )
        for record, _, measured in pending
    ]
    db.add_all(groups)
    db.flush()

    db.add_all(
        MappingRecordAllele(
            mapping_record_id=link.mapping_record_id,
            score_set_id=link.score_set_id,
            allele_id=link.allele_id,
            is_authoritative=True,
            projection_group=group.id,
        )
        for (_, link, _), group in zip(pending, groups)
    )
    db.flush()

    counts["projected"] += len(pending)
    return counts
