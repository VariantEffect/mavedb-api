"""Backfill ``target_gene_mappings`` rows and per-variant ScoreAnnotation columns
for existing ``mapped_variants``.

Context
-------
Schema migration ``8c4a2f1d9e6b`` introduced the ``target_gene_mappings`` table
(per-(target gene, alignment level) provenance/QC) and added
``target_gene_mapping_id`` / ``alignment_level`` / ``at_mismatched_locus`` /
``near_gap`` columns on ``mapped_variants``. These columns are nullable so the
schema migration can run without data; this standalone script populates them.

Strategy
--------
We can't recover the original QC numbers (they were never produced), so most
TargetGeneMapping fields stay null. What we *can* derive deterministically:

* ``alignment_level`` of each MappedVariant -- via the cascade described below.
* ``target_gene`` of each MappedVariant -- the only target for single-target
  score sets; for multi-target score sets we parse the ``Variant.hgvs_nt`` /
  ``hgvs_pro`` / ``hgvs_splice`` prefix (``TARGET_NAME:c.1A>G``).
* ``tool_version`` -- copied from ``MappedVariant.mapping_api_version`` (one
  TargetGeneMapping row is created per distinct (target, level, tool_version)).

Layer attribution cascade
-------------------------
Attribution is attempted in order, stopping at the first success:

1. ``target.post_mapped_metadata`` keys -- deterministic when populated.
2. ``target.pre_mapped_metadata`` keys -- same layer structure; covers targets
   where a computed reference sequence was found but all variant-level mappings
   failed before producing ``post_mapped`` output.
3. ``post_mapped`` VRS HGVS / sequence alphabet -- inspects a sample of mapped
   variants; reliable because dcd-mapping uses ``preferred_layer_only=True``.
4. ``target_sequence.sequence_type`` + ``category`` -- last resort for targets
   where every variant failed before any VRS data was produced:
   - ``protein`` sequence_type → protein layer (always)
   - ``dna`` + ``protein_coding`` category → genomic (dcd-mapping uses MANE
     select for protein-coding genes regardless of input sequence)
   - ``dna`` + non-coding categories → ambiguous; left unattributed

Known unattributable case
--------------------------
Score sets whose targets have a ``dna`` sequence type with a non-coding category
(``regulatory`` or ``other_noncoding``) AND where every mapped variant failed
before producing any ``post_mapped`` or reference-sequence metadata cannot be
attributed deterministically. For these targets, cDNA and genomic are both valid
dcd-mapping outputs and there is no surviving signal to distinguish them.

In practice this means we can fully attribute all legacy mappings to a layer as
we don't have any examples of this unnatributable case in production data. 

We follow the multi-hop/standalone-script convention used by
``migrate_jsonb_ranges_to_table_rows.py`` so this can be invoked outside of
``alembic upgrade`` (it can take a long time on production data).

Usage
-----
::

    python -m alembic.manual_migrations.migrate_target_gene_mapping_qc          # run
    python -m alembic.manual_migrations.migrate_target_gene_mapping_qc verify   # status
    python -m alembic.manual_migrations.migrate_target_gene_mapping_qc rollback # destructive
"""

from __future__ import annotations

import re
import sys
from collections import defaultdict
from typing import Optional

import sqlalchemy as sa
from sqlalchemy.orm import Session, configure_mappers, selectinload

# SQLAlchemy needs access to all models to properly map relationships.
from mavedb.models import *  # noqa: F401,F403  pylint: disable=wildcard-import
from mavedb.db.session import SessionLocal
from mavedb.lib.variants import HGVS_G_REGEX, HGVS_P_REGEX, get_hgvs_from_post_mapped
from mavedb.models.enums.annotation_layer import AnnotationLayer
from mavedb.models.mapped_variant import MappedVariant
from mavedb.models.score_set import ScoreSet
from mavedb.models.target_gene import TargetGene
from mavedb.models.target_gene_mapping import TargetGeneMapping
from mavedb.models.variant import Variant

configure_mappers()

# Pattern for the ``TARGET_NAME:`` prefix used on variant.hgvs_* strings in
# multi-target score sets. Per mavehgvs grammar a target name is an unquoted
# identifier ending with ``:``; we only need to capture the leading token.
_MULTI_TARGET_PREFIX_RE = re.compile(r"^([^:\s]+):")

# cDNA / non-coding HGVS prefix (``c.`` or ``n.``). ``HGVS_G_REGEX`` and
# ``HGVS_P_REGEX`` already cover ``g.`` and ``p.`` -- this fills the gap.
_HGVS_C_REGEX = re.compile(r"(^|:)[cn]\.")

# Nucleotide alphabet for distinguishing protein from nucleotide sequences in VRS
# state/ref-allele payloads. IUPAC ambiguity codes (``V``, ``S``, ``H`` etc.) are
# intentionally excluded -- they overlap with amino-acid one-letter codes and are
# not used in MAVE variant alleles. Any letter outside this set (e.g. ``V``, ``S``,
# ``T`` as amino acids) pins the layer to protein.
_NUCLEOTIDE_ALPHABET = frozenset("ACGTUNRYSWKMBDHV-")


def _layer_from_hgvs(hgvs: str) -> Optional[AnnotationLayer]:
    """Classify an HGVS string into an :class:`AnnotationLayer`."""
    if HGVS_G_REGEX.search(hgvs):
        return AnnotationLayer.genomic
    if HGVS_P_REGEX.search(hgvs):
        return AnnotationLayer.protein
    if _HGVS_C_REGEX.search(hgvs):
        return AnnotationLayer.cdna
    return None


def _layer_from_vrs_sequence(post_mapped: dict) -> Optional[AnnotationLayer]:
    """
    Infer layer from VRS sequence alphabet (protein vs nucleotide).
    Returns protein if any allele/member sequence contains a non-nucleotide letter.
    Returns genomic otherwise (since we never go to cDNA directly).
    Handles both VRS 2.X and VRS 1.X (VariationDescriptor) structures.
    """
    if not post_mapped:
        return None

    def extract_sequences(obj):
        # VRS 2.X Allele
        if obj.get("type") == "Allele":
            seq = obj.get("state", {}).get("sequence")
            if seq:
                yield seq

        # VRS 1.X VariationDescriptor
        elif obj.get("type") == "VariationDescriptor":
            seq = obj.get("vrs_ref_allele_seq")
            if seq:
                yield seq

            # Also check nested variation
            var = obj.get("variation")
            if isinstance(var, dict):
                yield from extract_sequences(var)

        # Haplotype or CisPhasedBlock
        elif obj.get("type") in ("Haplotype", "CisPhasedBlock"):
            for member in obj.get("members", []):
                yield from extract_sequences(member)

    found_seq = False
    for seq in extract_sequences(post_mapped):
        found_seq = True
        if any(letter.upper() not in _NUCLEOTIDE_ALPHABET for letter in seq if letter.isalpha()):
            return AnnotationLayer.protein
        
    if found_seq:
        return AnnotationLayer.genomic
    
    return None


def _alignment_level_for(target: TargetGene) -> Optional[AnnotationLayer]:
    """Recover the alignment layer for legacy mapped variants of ``target``.

    Tries ``post_mapped_metadata`` first, then ``pre_mapped_metadata`` (same
    key structure). The worker writes one entry per layer that produced a
    reference sequence; genomic alignments also emit cDNA metadata "for free"
    via the transcript, so ``{genomic, cdna}`` is a normal genomic-layer output.
    ``{genomic, protein}`` is not a valid ``preferred_layer_only=True`` output;
    we treat it as unattributable so diagnostics surface it for inspection.
    """
    for metadata in (target.post_mapped_metadata or {}, target.pre_mapped_metadata or {}):
        has_genomic = "genomic" in metadata
        has_protein = "protein" in metadata
        has_cdna = "cdna" in metadata

        if has_genomic and has_protein:
            return None

        if has_genomic:
            return AnnotationLayer.genomic
        if has_protein:
            return AnnotationLayer.protein
        if has_cdna:
            return AnnotationLayer.cdna

    return None


def _layer_from_sequence_type(target: TargetGene) -> Optional[AnnotationLayer]:
    """Infer alignment layer from ``target_sequence.sequence_type`` and ``category``.

    Last-resort fallback for targets where all variant-level mappings failed
    before any VRS data was produced. Reflects dcd-mapping's selection logic:
    - Protein sequences are always aligned at the protein layer.
    - DNA sequences for protein-coding genes are aligned at the genomic layer
      via MANE select, regardless of the input sequence representation.
    - DNA sequences for non-coding categories are ambiguous without VRS data
      (cDNA or genomic) and are left unattributed.
    """
    if target.target_sequence is None:
        return None
    seq_type = target.target_sequence.sequence_type
    if seq_type == "protein":
        return AnnotationLayer.protein
    if seq_type == "dna" and target.category is not None and target.category.value == "protein_coding":
        return AnnotationLayer.genomic
    return None


def _populate_layer_by_target_id(
    db: Session,
    targets_by_score_set_id: dict[int, list[TargetGene]],
    layer_by_target_id: dict[int, Optional[AnnotationLayer]],
    layer_via_hgvs_target_ids: set[int],
    layer_via_sequence_type_target_ids: set[int],
) -> None:
    """Determine the alignment layer for every target up front.

    Applies the attribution cascade documented in the module docstring:
    1. post/pre_mapped_metadata keys on the target gene
    2. post_mapped VRS inspection on a sample of mapped variants
    3. target_sequence.sequence_type + category as last resort
    """
    mv_t = MappedVariant.__table__
    v_t = Variant.__table__

    for score_set_id, targets in targets_by_score_set_id.items():
        ambiguous: list[TargetGene] = []
        for target in targets:
            level = _alignment_level_for(target)
            if level is not None:
                layer_by_target_id[target.id] = level
            else:
                ambiguous.append(target)

        if not ambiguous:
            continue

        # Sample mapped_variants for this score set with parseable post_mapped data.
        # Pull HGVS prefixes alongside so we can route each row to the right target on
        # multi-target score sets. Cap the sample size: in practice dcd-mapping emits
        # uniform layers per target, so a handful of rows is enough.
        sample_limit = 50 * max(len(ambiguous), 1)
        rows = db.execute(
            sa.select(
                mv_t.c.post_mapped,
                v_t.c.hgvs_nt,
                v_t.c.hgvs_pro,
                v_t.c.hgvs_splice,
            )
            .select_from(mv_t.join(v_t, mv_t.c.variant_id == v_t.c.id))
            .where(v_t.c.scoreset_id == score_set_id)
            .where(mv_t.c.post_mapped.is_not(None))
            .limit(sample_limit)
        ).all()

        if rows:
            ambiguous_ids = {t.id for t in ambiguous}
            for row in rows:
                target = _resolve_target_from_row(row, targets)
                if target is None or target.id not in ambiguous_ids:
                    continue
                if target.id in layer_by_target_id:
                    continue

                hgvs = get_hgvs_from_post_mapped(row.post_mapped)
                level = None
                if hgvs:
                    level = _layer_from_hgvs(hgvs)
                if level is None:
                    level = _layer_from_vrs_sequence(row.post_mapped)
                if level is None:
                    continue

                layer_by_target_id[target.id] = level
                layer_via_hgvs_target_ids.add(target.id)
                ambiguous_ids.discard(target.id)
                if not ambiguous_ids:
                    break

        # Record cache misses so the chunk loop doesn't re-evaluate them per row.
        for target in ambiguous:
            layer_by_target_id.setdefault(target.id, None)

        # Last-resort: for targets still unattributed after VRS inspection (including
        # score sets where every mapped variant had null post_mapped), infer from
        # target_sequence.sequence_type + category. Accessing target.target_sequence
        # triggers a lazy load; acceptable here since this path is rare.
        for target in ambiguous:
            if layer_by_target_id.get(target.id) is None:
                level = _layer_from_sequence_type(target)
                if level is not None:
                    layer_by_target_id[target.id] = level
                    layer_via_sequence_type_target_ids.add(target.id)


def _resolve_target_from_row(row, targets: list[TargetGene]) -> Optional[TargetGene]:
    if len(targets) == 1:
        return targets[0]

    if not targets:
        return None

    # Multi-target score sets prefix each HGVS string with the per-target identifier:
    # ``target_sequence.label`` for sequence-based score sets, ``target_accession.accession``
    # for accession-based ones. Build a single lookup covering both.
    target_by_name: dict[str, TargetGene] = {}
    for t in targets:
        if t.target_sequence is not None and t.target_sequence.label:
            target_by_name[t.target_sequence.label] = t
        if t.target_accession is not None and t.target_accession.accession:
            target_by_name[t.target_accession.accession] = t

    for raw in (row.hgvs_nt, row.hgvs_pro, row.hgvs_splice):
        if not raw:
            continue

        match = _MULTI_TARGET_PREFIX_RE.match(raw)
        if not match:
            continue

        candidate = target_by_name.get(match.group(1))
        if candidate is not None:
            return candidate

    return None


def do_migration(db: Session) -> None:
    print("Starting backfill of target_gene_mappings and mapped_variant ScoreAnnotation columns...")

    # Index target genes by score_set_id so we can resolve target ownership without
    # an extra query per mapped variant on multi-target score sets.
    targets_by_score_set_id: dict[int, list[TargetGene]] = defaultdict(list)
    for target in db.scalars(sa.select(TargetGene).options(selectinload(TargetGene.target_sequence))).all():
        targets_by_score_set_id[target.score_set_id].append(target)

    # Index score set urns by id for diagnostic logging on unattributed rows --
    # cheap, and lets the post-run report group by URN without joins.
    score_set_urn_by_id: dict[int, str] = dict(
        db.execute(sa.select(ScoreSet.id, ScoreSet.urn)).all()  # type: ignore[arg-type]
    )

    # Cache (target_gene_id, alignment_level, tool_version) -> persisted TargetGeneMapping
    # so we create one row per distinct combination and reuse it across every
    # mapped variant that maps into it.
    cache: dict[tuple[int, AnnotationLayer, str], TargetGeneMapping] = {}

    total = db.scalar(sa.select(sa.func.count(MappedVariant.id))) or 0
    print(f"  {total} mapped variants to consider.")

    # Walk in id order so re-runs are reproducible. Stream in chunks to keep
    # the session footprint bounded; each chunk is committed independently so a
    # mid-run failure leaves a partial-but-consistent state we can resume from.
    chunk_size = 50000
    processed = 0
    backfilled = 0
    skipped_no_target = 0
    skipped_no_target_current = 0
    skipped_no_layer = 0
    skipped_no_layer_current = 0

    # Diagnostics so we can decide what to do with unattributed legacy rows
    # before flipping ``target_gene_mapping_id`` to NOT NULL in a future migration.
    # Counts by (reason, current) and per-score-set; sample ids capped to keep output bounded.
    skipped_by_score_set: dict[tuple[str, str, bool], int] = defaultdict(int)
    skipped_samples: dict[str, list[int]] = defaultdict(list)
    sample_cap = 10

    # Project only the columns we need and join Variant once pulling full ORM objects forces 
    # an N+1 lazy-load on ``mapped_variant.variant`` for every row.
    mv_t = MappedVariant.__table__
    v_t = Variant.__table__
    projection = (
        sa.select(
            mv_t.c.id,
            mv_t.c.current,
            mv_t.c.mapping_api_version,
            mv_t.c.vrs_version,
            mv_t.c.mapped_date,
            v_t.c.scoreset_id.label("score_set_id"),
            v_t.c.hgvs_nt,
            v_t.c.hgvs_pro,
            v_t.c.hgvs_splice,
        )
        .select_from(mv_t.join(v_t, mv_t.c.variant_id == v_t.c.id))
        .where(mv_t.c.target_gene_mapping_id.is_(None))
        .order_by(mv_t.c.id)
    )

    # Per-target alignment-level cache. Under dcd-mapping ``preferred_layer_only=True``,
    # every successfully mapped variant for a given target shares the same layer, so we
    # only need to determine it once per target -- and any single attributable mapped
    # variant for that target lets us attribute every sibling. The via_* sets track which
    # fallback was used so the diagnostic report shows attribution breadth.
    layer_by_target_id: dict[int, Optional[AnnotationLayer]] = {}
    layer_via_hgvs_target_ids: set[int] = set()
    layer_via_sequence_type_target_ids: set[int] = set()
    _populate_layer_by_target_id(
        db,
        targets_by_score_set_id,
        layer_by_target_id,
        layer_via_hgvs_target_ids,
        layer_via_sequence_type_target_ids,
    )
    print(
        f"  Layer attributed for {sum(1 for v in layer_by_target_id.values() if v is not None)} of "
        f"{len(layer_by_target_id)} targets "
        f"({len(layer_via_hgvs_target_ids)} via post_mapped HGVS, "
        f"{len(layer_via_sequence_type_target_ids)} via sequence_type fallback)."
    )

    last_id = 0
    while True:
        chunk = db.execute(
            projection.where(mv_t.c.id > last_id).limit(chunk_size)
        ).all()
        if not chunk:
            break

        # Group ids to update by (tgm_id, level) so we can issue one bulk UPDATE
        # per group instead of a per-row ORM flush.
        updates_by_group: dict[tuple[int, AnnotationLayer], list[int]] = defaultdict(list)

        for row in chunk:
            last_id = row.id
            processed += 1

            targets = targets_by_score_set_id.get(row.score_set_id, [])
            target = _resolve_target_from_row(row, targets)
            if target is None:
                skipped_no_target += 1
                if row.current:
                    skipped_no_target_current += 1

                score_set_urn = score_set_urn_by_id.get(row.score_set_id, "<unknown>")
                skipped_by_score_set[(score_set_urn, "no_target", bool(row.current))] += 1
                samples = skipped_samples["no_target"]

                if len(samples) < sample_cap:
                    samples.append(row.id)

                continue

            level = layer_by_target_id.get(target.id)
            if level is None:
                skipped_no_layer += 1
                if row.current:
                    skipped_no_layer_current += 1

                score_set_urn = score_set_urn_by_id.get(row.score_set_id, "<unknown>")
                skipped_by_score_set[(score_set_urn, "no_layer", bool(row.current))] += 1
                samples = skipped_samples["no_layer"]

                if len(samples) < sample_cap:
                    samples.append(row.id)

                continue

            tool_version = row.mapping_api_version
            cache_key = (target.id, level, tool_version)
            tgm = cache.get(cache_key)
            if tgm is None:
                # Look up an already-persisted row first (idempotent re-runs).
                tgm = db.scalars(
                    sa.select(TargetGeneMapping).where(
                        TargetGeneMapping.target_gene_id == target.id,
                        TargetGeneMapping.alignment_level == level,
                        TargetGeneMapping.tool_version == tool_version,
                    )
                ).first()

                if tgm is None:
                    tgm = TargetGeneMapping(
                        target_gene_id=target.id,
                        alignment_level=level,
                        # The legacy mapping was the one selected for this variant,
                        # so it's effectively the preferred record for (target, level).
                        preferred=True,
                        tool_name="dcd-mapping",
                        tool_version=tool_version,
                        vrs_version=row.vrs_version,
                        mapped_date=row.mapped_date,
                    )
                    db.add(tgm)
                    db.flush()

                cache[cache_key] = tgm

            updates_by_group[(tgm.id, level)].append(row.id)
            backfilled += 1

        # One bulk UPDATE per (tgm, level) group -- typically only a handful per
        # chunk even on multi-target score sets.
        for (tgm_id, level), ids in updates_by_group.items():
            db.execute(
                sa.update(mv_t)
                .where(mv_t.c.id.in_(ids))
                .values(target_gene_mapping_id=tgm_id, alignment_level=level.value)
            )

        db.commit()
        print(
            f"  Committed chunk -- processed={processed} backfilled={backfilled} "
            f"skipped_no_target={skipped_no_target} skipped_no_layer={skipped_no_layer}"
        )

    print("Backfill complete:")
    print(f"  processed:                     {processed}")
    print(f"  backfilled:                    {backfilled}")
    print(f"  skipped_no_target:             {skipped_no_target} (current=True: {skipped_no_target_current})")
    print(f"  skipped_no_layer:              {skipped_no_layer} (current=True: {skipped_no_layer_current})")
    print(f"  TargetGeneMapping rows created: {len(cache)}")

    # Diagnostic report. ``current=True`` rows are the ones we actually need to
    # attribute before the future NOT NULL migration; ``current=False`` rows are
    # historical tombstones and are likely fine to leave or delete.
    if skipped_by_score_set:
        print("\nUnattributed mapped_variants by score set / reason / current:")
        # Sort by current=True rows desc so the rows that block a future NOT NULL
        # constraint surface first.
        rows = sorted(
            skipped_by_score_set.items(),
            key=lambda kv: (not kv[0][2], -kv[1]),
        )
        print(f"  {'urn':<32}  {'reason':<10}  {'current':<7}  count")
        for (urn, reason, current), count in rows:
            print(f"  {urn:<32}  {reason:<10}  {str(current):<7}  {count}")

        for reason, samples in skipped_samples.items():
            if samples:
                print(f"  Sample mapped_variant.id (reason={reason}): {samples}")
    else:
        print("\nNo unattributed mapped_variants -- safe to flip target_gene_mapping_id NOT NULL after this run.")


def verify_migration(db: Session) -> None:
    print("\nVerifying backfill...")
    total = db.scalar(sa.select(sa.func.count(MappedVariant.id))) or 0
    backfilled = db.scalar(
        sa.select(sa.func.count(MappedVariant.id)).where(MappedVariant.target_gene_mapping_id.is_not(None))
    ) or 0
    tgm_count = db.scalar(sa.select(sa.func.count(TargetGeneMapping.id))) or 0
    print(f"  mapped_variants total:              {total}")
    print(f"  mapped_variants backfilled:         {backfilled}")
    print(f"  mapped_variants still null:         {total - backfilled}")
    print(f"  target_gene_mappings rows present:  {tgm_count}")

    # Break the still-null bucket down by ``current``. ``current=True`` is the
    # blocker for tightening the FK to NOT NULL in a future migration; the rest
    # are inert historical rows.
    null_current = db.scalar(
        sa.select(sa.func.count(MappedVariant.id))
        .where(MappedVariant.target_gene_mapping_id.is_(None))
        .where(MappedVariant.current.is_(True))
    ) or 0
    null_historical = (total - backfilled) - null_current
    print(f"    of which current=True:            {null_current}")
    print(f"    of which current=False:           {null_historical}")


def rollback_migration(db: Session) -> None:
    """Destructive: clears every column populated by :func:`do_migration`."""
    print("Rolling back backfill...")
    db.execute(
        sa.update(MappedVariant).values(target_gene_mapping_id=None, alignment_level=None)
    )
    deleted = db.execute(sa.delete(TargetGeneMapping)).rowcount  # type: ignore[attr-defined]
    db.commit()
    print(f"  Cleared mapped_variant FKs and dropped {deleted} target_gene_mappings rows.")


def show_usage() -> None:
    print(
        """
Usage: python -m alembic.manual_migrations.migrate_target_gene_mapping_qc [command]

Commands:
  migrate  (default) - Backfill target_gene_mappings and ScoreAnnotation columns
  verify             - Report current backfill status (read-only)
  rollback           - Destructive: clear all backfilled data
"""
    )


if __name__ == "__main__":
    command = sys.argv[1] if len(sys.argv) > 1 else "migrate"

    if command in {"help", "--help", "-h"}:
        show_usage()
    elif command == "rollback":
        print("WARNING: This will delete every target_gene_mappings row and clear the FKs on mapped_variants.")
        if input("Are you sure you want to continue? (y/N): ").lower() == "y":
            with SessionLocal() as db:
                rollback_migration(db)
        else:
            print("Rollback cancelled.")

    elif command == "verify":
        with SessionLocal() as db:
            verify_migration(db)

    elif command == "migrate":
        with SessionLocal() as db:
            do_migration(db)
            verify_migration(db)

    else:
        print(f"Unknown command: {command}")
        show_usage()
