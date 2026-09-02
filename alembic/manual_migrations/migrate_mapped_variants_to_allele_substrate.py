"""Backfill the Allele substrate (``mapping_records`` / ``alleles`` /
``mapping_record_alleles`` + the annotation link tables) from existing
``mapped_variants`` rows.

Context
-------
The annotation/serving layer is migrating off ``MappedVariant`` onto a
parallel-tables model::

    Variant ─< MappingRecord (1 live/variant, ValidTime) ─< MappingRecordAllele (ValidTime) >─ Allele (dedup by vrs_digest)

This script populates the new tables from the *existing* ``mapped_variants`` data so
the read-cutover (the ``v_variant_annotations`` view / ``published_variants`` MV
rewrites and the serving readers) resolves for historical score sets, which are
otherwise empty in the new substrate.

This is the **reshape half of the hybrid migration**: it reconstructs the
authoritative spine deterministically from old data with no external-service
calls. It deliberately does **not** reproduce the reverse-translation
equivalence fan-out (the per-level coding/genomic/protein alleles and their
``projection_group`` pairing) — that data was never computed for old mappings
(``MappedVariant`` holds a single ``post_mapped`` VRS plus cross-level HGVS
*strings*, and an Allele needs a real ``vrs_digest`` that a string cannot
supply). Restoring that richness is the decoupled *enrichment* half: queue the
reverse-translation + annotation-refresh jobs per score set on the worker after
this backfill lands. Serving is correct at the measured level as soon as this
runs; enrichment only adds cross-level breadth and blocks nothing.

What is reconstructed
---------------------
* **MappingRecord history** — every ``MappedVariant`` version of a variant with a resolvable sequence
  level becomes a ``MappingRecord`` with a gap-free ``[valid_from, valid_to)`` window derived from the
  versions' ``mapped_date`` ordering. The ``current=True`` version is the live tail (``valid_to IS
  NULL``). History dates are inherently approximate: ``MappedVariant`` records versioning with a
  ``current`` boolean, not transaction time, so a window boundary is the successor's ``mapped_date``,
  not the exact instant the swap happened. A version whose sequence level can't be attributed is
  skipped **individually** (its siblings still get their records — see Idempotency below); if that
  version happens to be the live one, the variant has no live record and stays eligible for retry.
  Live dates that precede a historical version's are a logged anomaly (a likely re-map candidate), not
  a fatal condition — see :func:`_reconstruct_windows`.
* **Authoritative Allele** — from each version's ``post_mapped`` VRS, deduplicated by ``vrs_digest``
  against an in-memory, per-chunk cache (:func:`_authoritative_allele` — not the live job's
  ``get_or_create_allele``, which queries per call; see :func:`do_migration`'s docstring for why).
  Carries the CAID and the assay-level HGVS
  across so the allele-keyed serving endpoints resolve. The assay-level HGVS itself is read through a
  three-tier fallback (:func:`_hgvs_assay_level_with_source`): ``mv.hgvs_assay_level`` directly, else
  the per-level column matching the resolved level (``hgvs_g``/``hgvs_c``/``hgvs_p`` — backfilled later
  and never caught up on a large share of old data), else the ``post_mapped`` VRS blob's own
  ``expressions`` (the mapper sometimes stamps or reconstructs an HGVS expression directly on the VRS
  Allele even when neither dedicated column got populated). Even with all three tiers, some legacy rows
  carry no recoverable HGVS at all — that gap is real and needs the mapper re-run to close, not another
  migration-time trick. Reading only ``hgvs_assay_level`` (as this script originally did) left most
  reconstructed Alleles (and ``MappingRecord.hgvs_assay_level``, and therefore its derived
  ``transcript``) NULL even when a real string was recoverable — silently starving any downstream pass
  that resolves a transcript from it.
* **MappingRecordAllele** — one ``is_authoritative=True`` link per record on the
  record's validity window. ``projection_group`` stays NULL (no fan-out).
* **Annotation timelines** — reconstructed across *every* mapping version, not just the live one:
  ``gnomad_variants`` → ``gnomad_allele_links``, ``clinical_controls`` → ``clinvar_allele_links``,
  VEP columns → ``vep_allele_consequences``. Each mapping version's bundled annotations are captured
  against that version's window and, per allele, merged per source key (gnomAD variant / ClinVar
  control / consequence value) into maximal intervals — so a continuously-held annotation is one link
  dated from its **earliest** mapping version, open-ended only if it reaches the live record. A
  release change that coincided with a re-map surfaces as a superseding closed+live pair. This is
  faithful (the old jobs wrote annotations on the current mapping row, so their changes aligned to
  re-map boundaries), just **coarse** — sub-mapping-version churn collapses to the latest value in
  that window, and boundary timestamps are day-grained. The valid-time axis is uniformly the mapping
  date across sources (VEP's real ``access_date`` is kept in its own audit column, not as
  ``valid_from``, so ``as_of`` stays coherent). VEP has no Ensembl release string in old data, but
  ``access_date`` is enough to derive one: ``source_version`` is looked up from a snapshotted table of
  Ensembl release dates (:data:`_ENSEMBL_VEP_RELEASE_DATES`), not stamped as an opaque sentinel — see
  :func:`_resolve_vep_source_version`. Only a missing ``access_date``, or one older than the table's
  earliest known release, falls back to the ``"legacy"`` sentinel.

  **Exception:** a gnomAD or ClinVar window that would otherwise land live on a **protein-level** allele
  is force-closed at the migration's own run time instead (VEP is unaffected — see below). The old
  model could proxy both onto protein-level rows because there was nowhere else to attach them; the new
  model resolves them correctly through the CA/PA link graph onto nucleotide siblings instead
  (``lib/allele_measurements.py``), so carrying the proxy forward live would just create a second,
  competing provenance story for the same value. Applied uniformly even though gnomAD is not known to
  have actually been proxied this way in practice (ClinVar was) — it is the same underlying anti-pattern
  (a nucleotide-level concept attached to a protein-level allele), so the guard costs nothing to keep
  general. The historical fact is kept (closed, not deleted) for point-in-time audit; live annotations
  for these alleles return once cross-level annotation fan-out re-derives them properly.

Idempotency / no double-write
-----------------------------
A variant is either old-substrate or new (the parallel-tables invariant); native new-model variants
have no ``mapped_variants``. The migration therefore processes only variants that have
``mapped_variants`` and **no live** ``MappingRecord`` yet — live, not "any", because a variant whose
live mapped_variant version has no resolvable sequence level is left with only historical records (see
above) and must stay selectable so a later re-map or QC fix can complete it. Within a selected variant,
each window is additionally guarded by a ``(variant_id, mapped_date)`` existence check, so a re-run
that revisits a partially-migrated variant only fills in the windows still missing rather than
duplicating the ones a prior pass already wrote. Annotation links are existence-checked before
insertion, so a partial run resumes cleanly; run ``rebuild-annotations`` after resuming a
partially-migrated variant to pick up any timelines the skipped windows didn't get to build.

Standalone convention
---------------------
Follows the ``migrate_jsonb_ranges_to_table_rows.py`` pattern: invoked outside
``alembic upgrade`` (it can take a long time on production data), commits in
variant batches, and offers ``verify`` / ``rollback`` commands.

Usage
-----
::

    python -m alembic.manual_migrations.migrate_mapped_variants_to_allele_substrate           # run
    python -m alembic.manual_migrations.migrate_mapped_variants_to_allele_substrate verify    # inspect without writing
    python -m alembic.manual_migrations.migrate_mapped_variants_to_allele_substrate rollback  # remove backfilled records (destructive)
    python -m alembic.manual_migrations.migrate_mapped_variants_to_allele_substrate rebuild-annotations  # (re)build annotation links for already-migrated variants
    python -m alembic.manual_migrations.migrate_mapped_variants_to_allele_substrate migrate --score-set urn:mavedb:00000001-a-1
    python -m alembic.manual_migrations.migrate_mapped_variants_to_allele_substrate migrate --batch-size 1000 --dry-run
"""

from __future__ import annotations

import argparse
import logging
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import date, datetime, time, timezone
from time import perf_counter  # `time` above is datetime.time; import the timer directly
from typing import Optional

import sqlalchemy as sa
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session, configure_mappers

from mavedb.models import *  # noqa: F401,F403  pylint: disable=wildcard-import  — register all mappers for configure_mappers()
from mavedb.db.session import SessionLocal
from mavedb.lib.annotation_status_manager import AnnotationStatusManager
from pydantic import ValidationError

from mavedb.lib.variants import get_hgvs_from_post_mapped
from mavedb.lib.vrs_utils import canonical_variation_document
from mavedb.models.allele import Allele
from mavedb.models.annotation_event import AnnotationEvent
from mavedb.models.clinvar_allele_link import ClinvarAlleleLink
from mavedb.models.enums.annotation_type import AnnotationType
from mavedb.models.enums.disposition import Disposition
from mavedb.models.enums.event_reason import EventReason
from mavedb.models.enums.sequence_level import SequenceLevel
from mavedb.models.gnomad_allele_link import GnomadAlleleLink
from mavedb.models.mapped_variant import MappedVariant
from mavedb.models.mapping_record import MappingRecord
from mavedb.models.mapping_record_allele import MappingRecordAllele
from mavedb.models.score_set import ScoreSet
from mavedb.models.variant import Variant
from mavedb.models.vep_allele_consequence import VepAlleleConsequence

configure_mappers()

logger = logging.getLogger(__name__)

# Old VEP data carries no Ensembl release string, only an access_date — but source_version is derivable:
# Ensembl releases (and VEP's version with them) are coordinated, dated, and public
# (https://github.com/Ensembl/ensembl-vep/releases), so the release active on a given access_date can
# be looked up rather than stamped as an opaque sentinel. Each entry is a release's *earliest* GitHub
# tag date (patch tags like 92.1-92.6 are hotfixes within release 92, not new majors); an access_date
# is attributed to the newest release whose date is <= it. Snapshotted from the GitHub API on
# 2026-07-20 (covers releases 92 through 116); this table does not self-update, so re-run the same
# query and extend it before backfilling data annotated under a release newer than the last entry here.
_ENSEMBL_VEP_RELEASE_DATES: list[tuple[date, str]] = [
    (date(2018, 4, 12), "92"),
    (date(2018, 7, 17), "93"),
    (date(2018, 10, 2), "94"),
    (date(2019, 1, 10), "95"),
    (date(2019, 4, 8), "96"),
    (date(2019, 7, 3), "97"),
    (date(2019, 9, 27), "98"),
    (date(2020, 1, 16), "99"),
    (date(2020, 4, 29), "100"),
    (date(2020, 8, 20), "101"),
    (date(2020, 11, 30), "102"),
    (date(2021, 2, 16), "103"),
    (date(2021, 5, 5), "104"),
    (date(2021, 12, 9), "105"),
    (date(2022, 4, 14), "106"),
    (date(2022, 7, 13), "107"),
    (date(2022, 10, 20), "108"),
    (date(2023, 2, 8), "109"),
    (date(2023, 7, 18), "110"),
    (date(2024, 1, 11), "111"),
    (date(2024, 5, 16), "112"),
    (date(2024, 10, 18), "113"),
    (date(2025, 5, 8), "114"),
    (date(2025, 9, 3), "115"),
    (date(2026, 6, 10), "116"),
]

# Fallback sentinel for the cases the release-date table can't resolve: no access_date at all, or an
# access_date older than the table's earliest known release (GitHub has no tags before release/92, so
# anything before 2018-04-12 is out of range). VepAlleleConsequence.source_version is NOT NULL, and a
# refresh run should treat this value as unversioned and re-confirm it against the current release
# rather than trusting it as current — the same behavior the sentinel always had.
LEGACY_VEP_SOURCE_VERSION = "legacy"


def _resolve_vep_source_version(access_date: Optional[date]) -> str:
    """Map a legacy VEP ``access_date`` to the Ensembl release active on that day.

    Returns :data:`LEGACY_VEP_SOURCE_VERSION` when ``access_date`` is missing or predates the earliest
    release the table knows about — both cases where the release genuinely can't be determined, as
    opposed to just not being in the table's covered range for lack of maintenance.
    """
    if access_date is None or access_date < _ENSEMBL_VEP_RELEASE_DATES[0][0]:
        return LEGACY_VEP_SOURCE_VERSION
    resolved = _ENSEMBL_VEP_RELEASE_DATES[0][1]
    for release_date, version in _ENSEMBL_VEP_RELEASE_DATES:
        if release_date > access_date:
            break
        resolved = version
    return resolved


def _as_dt(d: date) -> datetime:
    """Lift a ``date`` to a timezone-aware ``datetime`` for a ValidTime boundary.

    ValidTime windows are ``DateTime(timezone=True)``; ``MappedVariant`` versioning
    is day-grained (``mapped_date``), so midnight UTC is the reconstructed boundary.
    """
    return datetime.combine(d, time.min, tzinfo=timezone.utc)


def _resolve_level(mv: MappedVariant) -> Optional[SequenceLevel]:
    """The sequence level a ``MappedVariant`` was assayed/aligned at.

    ``alignment_level`` is populated on every row by the target-gene-mapping QC
    backfill and is the direct source (the mapping job sets a record's
    ``assay_level`` and ``alignment_level`` to the same value). For the rare
    legacy row the QC backfill could not attribute, fall back to whichever mapped
    HGVS column is populated. Returns ``None`` when neither signal is present —
    the record cannot be created (``assay_level`` is NOT NULL) and the caller
    skips the variant with a logged anomaly.
    """
    if mv.alignment_level is not None:
        return mv.alignment_level
    if mv.hgvs_g:
        return SequenceLevel.genomic
    if mv.hgvs_c:
        return SequenceLevel.cdna
    if mv.hgvs_p:
        return SequenceLevel.protein
    return None


_LEVEL_HGVS_COLUMN = {
    SequenceLevel.genomic: "hgvs_g",
    SequenceLevel.cdna: "hgvs_c",
    SequenceLevel.protein: "hgvs_p",
}


def _hgvs_assay_level_with_source(mv: MappedVariant, level: SequenceLevel) -> tuple[Optional[str], str]:
    """Resolve the assay-level HGVS string, reporting which of three tiers supplied it.

    ``mv.hgvs_assay_level`` is frequently ``NULL`` on legacy rows — it was backfilled later and never
    caught up on a large share of old data — and reading only it (as this script originally did) meant
    most reconstructed Alleles got a NULL ``hgvs_g``/``hgvs_c``/``hgvs_p`` and
    ``MappingRecord.hgvs_assay_level`` even when the real string was recoverable. Since
    ``MappingRecord.transcript`` derives from ``hgvs_assay_level``, that silently starved any downstream
    pass that resolves a transcript from it (e.g. cross-level annotation fan-out).

    Three tiers, in order:

    1. ``"direct"`` — ``mv.hgvs_assay_level`` itself.
    2. ``"level_column"`` — the per-level column matching ``level`` (``hgvs_g``/``hgvs_c``/``hgvs_p``),
       mirroring :func:`_resolve_level`'s own fallback (same column), just for the *value* rather than
       the level.
    3. ``"vrs_expression"`` — the ``post_mapped`` VRS blob's own ``expressions``. Some legacy rows have
       *neither* dedicated column populated, but the mapper stamped an HGVS expression directly onto
       the VRS Allele it produced (``dcd_mapping`` sets this unconditionally for coding-layer alleles,
       and reconstructs it for genomic/protein when accession resolution succeeds — so it is not
       universal either, just an independent, additional chance). Reuses
       :func:`mavedb.lib.variants.get_hgvs_from_post_mapped`, the same extraction serving already relies
       on, rather than re-parsing VRS JSON here. A malformed or VRS-1.x blob raises there; caught here
       and treated as "no expression" so one bad blob doesn't abort the whole variant.

    Returns ``(None, "none")`` when no tier resolves anything — some legacy data genuinely carries no
    recoverable HGVS at all, and that gap cannot be closed without re-running the mapper.
    """
    if mv.hgvs_assay_level:
        return mv.hgvs_assay_level, "direct"

    level_value = getattr(mv, _LEVEL_HGVS_COLUMN[level])
    if level_value:
        return level_value, "level_column"

    try:
        vrs_value = get_hgvs_from_post_mapped(mv.post_mapped)
    except (KeyError, ValueError):
        vrs_value = None
    if vrs_value:
        return vrs_value, "vrs_expression"

    return None, "none"


def _resolve_hgvs_assay_level(mv: MappedVariant, level: SequenceLevel) -> Optional[str]:
    """The assay-level HGVS string for a version — see :func:`_hgvs_assay_level_with_source`."""
    return _hgvs_assay_level_with_source(mv, level)[0]


@dataclass
class _WindowResult:
    """``_reconstruct_windows``'s output: the ordered windows plus an anomaly flag.

    Kept as one return value (rather than ``None``-means-skip on windows, anomaly bundled in) so a
    caller can't forget to check the flag the way a bare tuple invites.
    """

    windows: list[tuple[MappedVariant, datetime, Optional[datetime]]]
    live_precedes_history: bool


def _reconstruct_windows(mvs: list[MappedVariant]) -> Optional[_WindowResult]:
    """Order a variant's ``MappedVariant`` versions and assign ValidTime windows.

    Returns ``(mv, valid_from, valid_to)`` per version, gap-free: each version's
    ``valid_to`` is its successor's ``valid_from``, and the live tail's is
    ``None``. The ``current=True`` version is the live tail regardless of its
    ``mapped_date``. Returns ``None`` (skip) when no version is current — a
    variant with mappings but no live one is a source anomaly we do not guess a
    live state for.

    ``live_precedes_history=True`` flags the case where the live version's ``mapped_date`` is earlier
    than some historical version's — a data-quality anomaly (``current`` should always be the newest).
    When it happens, the pairwise clamp below still prevents inverted windows, but at the cost of
    collapsing the affected historical window to zero-width (invisible to ``as_of``) rather than
    losing data outright: the caller surfaces the flag as a loud, variant-attributed warning instead
    of silently reordering, since it likely means the source variant wants a re-map.
    """
    current = [m for m in mvs if m.current]
    history = [m for m in mvs if not m.current]

    if not current:
        return None
    if len(current) == 1:
        live = current[0]
    else:
        # Multiple live rows is a source anomaly; keep the newest live and demote
        # the rest into history so no data is dropped.
        current.sort(key=lambda m: (m.mapped_date, m.id))
        live = current[-1]
        history.extend(current[:-1])
        logger.warning("Variant %s has %d current mapped_variants; keeping newest live.", live.variant_id, len(current))

    history.sort(key=lambda m: (m.mapped_date, m.id))
    ordered = history + [live]
    live_precedes_history = bool(history) and live.mapped_date < history[-1].mapped_date

    windows: list[tuple[MappedVariant, datetime, Optional[datetime]]] = []
    for i, m in enumerate(ordered):
        valid_from = _as_dt(m.mapped_date)
        valid_to: Optional[datetime] = None
        if i + 1 < len(ordered):
            successor = _as_dt(ordered[i + 1].mapped_date)
            # Same-day or out-of-order successor: clamp so the window is never
            # inverted (a zero-width [t, t) window is simply invisible to as_of).
            valid_to = max(successor, valid_from)
        windows.append((m, valid_from, valid_to))
    return _WindowResult(windows=windows, live_precedes_history=live_precedes_history)


def _fill_missing_allele_fields(allele: Allele, mv: MappedVariant, level: SequenceLevel) -> None:
    """Backfill identity fields onto a (possibly pre-existing, deduped) allele.

    Dedup means the first variant to mint an allele sets its fields; a later
    variant sharing the ``vrs_digest`` may carry the CAID or level HGVS the first
    lacked. Those are content-derived (deterministic from the allele), so
    fill-if-missing is safe and never overwrites a populated value.
    """
    if allele.clingen_allele_id is None and mv.clingen_allele_id:
        allele.clingen_allele_id = mv.clingen_allele_id

    hgvs_col = _LEVEL_HGVS_COLUMN[level]
    hgvs_value = _resolve_hgvs_assay_level(mv, level)
    if getattr(allele, hgvs_col) is None and hgvs_value:
        setattr(allele, hgvs_col, hgvs_value)


def _authoritative_allele(
    db: Session, mv: MappedVariant, level: SequenceLevel, allele_cache: dict[str, Allele]
) -> Optional[Allele]:
    """Get-or-create the authoritative allele from ``mv.post_mapped``, via an in-memory dedup cache.

    Returns ``None`` for versions with no post-mapped representation (failed or benign-absent variants
    get a record but no linked allele, matching the mapping job).

    Unlike :func:`mavedb.lib.variant_translations.get_or_create_allele` (used by the live mapping job),
    this does **not** query the database or flush per call. ``allele_cache`` is pre-seeded by the
    caller with one bulk ``vrs_digest IN (...)`` query per chunk (see :func:`do_migration`), and this
    function keeps it updated in-memory as new alleles are drafted — so a digest shared by two windows
    in the *same* chunk (a common dedup case) resolves from the dict, not a second round trip. This is
    safe only because the session runs with ``autoflush=False`` (a query wouldn't see an unflushed
    insert anyway) and because the caller flushes once per variant, not per window: an actual
    ``uq_alleles_vrs_digest`` collision (e.g. a concurrent live mapping run) still surfaces at that
    flush, inside the same per-variant SAVEPOINT it always has — this only removes the redundant reads,
    not the write-time integrity check.
    """
    stored = mv.post_mapped or {}
    if not stored.get("id"):
        return None

    # Recompute identity rather than adopt the stored id. `vrs_digest` is the UNIQUE dedup key the
    # whole allele graph hangs off, `alleles` rows are immutable once written, and `alleles` is the
    # only table here with no ValidTime - so a wrong digest cannot be corrected in place or
    # reconstructed by `as_of`, and it silently collapses two different alleles into one row.
    # Historical `post_mapped` blobs cannot be trusted on this point: an id minted before VRS
    # normalization moved a del/dup span survives unchanged, because `ga4gh_identify`'s default
    # `in_place` returns a non-empty id untouched. The live mapping job canonicalizes for the same
    # reason (`worker/jobs/variant_processing/mapping.py`); a backfill that trusted the blob would
    # write drift into the substrate permanently. See `scripts/audit_allele_identifiers.py`.
    try:
        post_mapped, digest = canonical_variation_document(stored, subject=f"mapped_variant {mv.id}")
    except (ValidationError, ValueError):
        logger.exception(f"Could not canonicalize post_mapped for mapped_variant {mv.id}; skipping its allele.")
        return None

    existing = allele_cache.get(digest)
    if existing is not None:
        _fill_missing_allele_fields(existing, mv, level)
        return existing

    hgvs_value = _resolve_hgvs_assay_level(mv, level)
    allele = Allele(
        vrs_digest=digest,
        level=level,
        hgvs_g=hgvs_value if level == SequenceLevel.genomic else None,
        hgvs_c=hgvs_value if level == SequenceLevel.cdna else None,
        hgvs_p=hgvs_value if level == SequenceLevel.protein else None,
        clingen_allele_id=mv.clingen_allele_id,
        post_mapped=post_mapped,
    )
    db.add(allele)
    allele_cache[digest] = allele
    return allele


@dataclass
class _AnnotationObservation:
    """One mapping version's annotation payload for an authoritative allele, tagged with that
    version's validity window.

    Built fresh from the DB per allele-batch (:func:`_observations_for_alleles`) rather than collected
    inline during record creation — so a deduped allele shared by several variants, or spanning a
    variant's re-map history, still yields one merged timeline rather than per-version fragments (see
    :func:`_write_annotation_timelines`), without needing every observation for the whole run held in
    memory at once.
    """

    valid_from: datetime
    valid_to: Optional[datetime]
    gnomad_variant_ids: tuple[int, ...]
    clinvar_control_ids: tuple[int, ...]
    vep_consequence: Optional[str]
    vep_access_date: Optional[date]


def _observation_for(
    mv: MappedVariant, valid_from: datetime, valid_to: Optional[datetime]
) -> Optional[_AnnotationObservation]:
    """Capture a mapping version's annotations for its window, or ``None`` if it carries none."""
    gnomad_ids = tuple(g.id for g in mv.gnomad_variants)
    clinvar_ids = tuple(c.id for c in mv.clinical_controls)
    if not gnomad_ids and not clinvar_ids and not mv.vep_functional_consequence:
        return None
    return _AnnotationObservation(
        valid_from=valid_from,
        valid_to=valid_to,
        gnomad_variant_ids=gnomad_ids,
        clinvar_control_ids=clinvar_ids,
        vep_consequence=mv.vep_functional_consequence,
        vep_access_date=mv.vep_access_date,
    )


def _merge_intervals(
    windows: list[tuple[datetime, Optional[datetime]]],
) -> list[tuple[datetime, Optional[datetime]]]:
    """Merge overlapping/adjacent ``[valid_from, valid_to)`` windows; ``valid_to=None`` is open-ended.

    An annotation held across a run of mapping versions produces contiguous windows (and overlapping
    ones when a deduped allele is shared by several variants). Merging collapses each continuous run
    into one link — ``valid_from`` = its earliest mapping date, ``valid_to`` open only if the run
    reaches the live record — and closes a link only where the annotation actually lapses, rather than
    fabricating a boundary at every re-map.
    """
    ordered = sorted(windows, key=lambda w: w[0])
    merged: list[tuple[datetime, Optional[datetime]]] = []
    cur_from, cur_to = ordered[0]
    for vf, vt in ordered[1:]:
        if cur_to is None or vf <= cur_to:  # overlap/adjacency; an open-ended window absorbs the rest
            cur_to = None if (cur_to is None or vt is None) else max(cur_to, vt)
        else:
            merged.append((cur_from, cur_to))
            cur_from, cur_to = vf, vt
    merged.append((cur_from, cur_to))
    return merged


def _allele_has_live_link(db: Session, model: type, allele_id: int) -> bool:
    """Whether ``allele_id`` already has a live (``valid_to IS NULL``) link in ``model``.

    Sees **committed** rows only — the session runs ``autoflush=False``, so this does not observe
    not-yet-flushed adds from the current pass. It therefore catches prior-run and concurrently-
    committed live links (e.g. from the mapping pipeline running alongside this backfill); single-live
    within one pass is enforced separately, in memory, by :func:`_write_allele_timeline`.
    """
    return db.scalar(sa.select(model.id).where(model.allele_id == allele_id, model.valid_to.is_(None))) is not None


def _emit_gnomad_link(
    db: Session,
    allele_id: int,
    gnomad_variant_id: int,
    valid_from: datetime,
    valid_to: Optional[datetime],
    stats: Counter,
) -> None:
    already = db.scalar(
        sa.select(GnomadAlleleLink.id).where(
            GnomadAlleleLink.allele_id == allele_id,
            GnomadAlleleLink.gnomad_variant_id == gnomad_variant_id,
            GnomadAlleleLink.valid_from == valid_from,
        )
    )
    if already is not None:  # idempotent re-run
        return
    db.add(
        GnomadAlleleLink(
            allele_id=allele_id, gnomad_variant_id=gnomad_variant_id, valid_from=valid_from, valid_to=valid_to
        )
    )
    stats["gnomad_links"] += 1


def _emit_clinvar_link(
    db: Session,
    allele_id: int,
    clinvar_control_id: int,
    valid_from: datetime,
    valid_to: Optional[datetime],
    stats: Counter,
) -> None:
    already = db.scalar(
        sa.select(ClinvarAlleleLink.id).where(
            ClinvarAlleleLink.allele_id == allele_id,
            ClinvarAlleleLink.clinvar_control_id == clinvar_control_id,
            ClinvarAlleleLink.valid_from == valid_from,
        )
    )
    if already is not None:  # idempotent re-run; ClinVar is multi-live per (allele, control)
        return
    db.add(
        ClinvarAlleleLink(
            allele_id=allele_id, clinvar_control_id=clinvar_control_id, valid_from=valid_from, valid_to=valid_to
        )
    )
    stats["clinvar_links"] += 1


def _emit_vep_consequence(
    db: Session,
    allele_id: int,
    consequence: str,
    access_date: date,
    valid_from: datetime,
    valid_to: Optional[datetime],
    stats: Counter,
) -> None:
    already = db.scalar(
        sa.select(VepAlleleConsequence.id).where(
            VepAlleleConsequence.allele_id == allele_id,
            VepAlleleConsequence.functional_consequence == consequence,
            VepAlleleConsequence.valid_from == valid_from,
        )
    )
    if already is not None:  # idempotent re-run
        return
    source_version = _resolve_vep_source_version(access_date)
    stats[
        "vep_source_version_legacy_fallback"
        if source_version == LEGACY_VEP_SOURCE_VERSION
        else "vep_source_version_resolved"
    ] += 1
    db.add(
        VepAlleleConsequence(
            allele_id=allele_id,
            functional_consequence=consequence,
            source_version=source_version,
            access_date=access_date,
            valid_from=valid_from,
            valid_to=valid_to,
        )
    )
    stats["vep_consequences"] += 1


def _write_allele_timeline(
    db: Session, allele_id: int, obs_list: list[_AnnotationObservation], stats: Counter, run_at: datetime
) -> None:
    """Build and emit one allele's merged annotation link timelines (no flush/commit here).

    Keyed per source — gnomAD by ``gnomad_variant_id``, ClinVar by ``clinvar_control_id``, VEP by
    consequence value — each key's windows are merged so a continuously-held annotation is a single
    link dated from its earliest mapping version, open-ended only if it reaches the live record.

    **Single-live-per-allele** for gnomAD/VEP is enforced with an in-memory flag seeded from committed
    state (:func:`_allele_has_live_link`). This is the fix for the ``autoflush=False`` session: a
    deduped allele linked to two *different* gnomAD variants (from two variants sharing the allele)
    would otherwise emit two live links and violate ``uq_gnomad_allele_links_live`` at flush, since the
    per-add DB check can't see the earlier unflushed add. The flag also folds in a link already live
    from a prior run or the concurrent mapping pipeline. ClinVar is multi-live per ``(allele, control)``.

    **Protein-level gnomAD/ClinVar are a legacy proxy the new model doesn't carry forward live.** The
    old model could attach ClinVar significance and gnomAD frequency to protein-level ``MappedVariant``
    rows as a proxy — there was nowhere else to put them. Both are intrinsically nucleotide-level
    concepts (a ClinVar submission and a gnomAD allele frequency are both keyed to a genomic variant,
    not a protein change), and the new model resolves them correctly for a protein allele via the CA/PA
    link graph onto its nucleotide siblings (see ``lib/allele_measurements.py``), not by direct
    attachment. A live direct link here would just be a second, competing provenance story for the same
    value. So a protein allele's gnomAD/ClinVar window is force-closed at ``run_at`` (the migration's
    own run time) instead of staying open-ended: the historical fact that this had been proxy-annotated
    is kept for point-in-time audit, but it stops being *live* data at the moment of reshape, to be
    replaced once cross-level annotation fan-out re-derives it correctly through the link graph. VEP is
    exempt — a functional consequence is computed relative to the assayed representation itself, so it
    is not a proxied nucleotide-level value the way gnomAD/ClinVar are.
    """
    gnomad_windows: dict[int, list[tuple[datetime, Optional[datetime]]]] = defaultdict(list)
    clinvar_windows: dict[int, list[tuple[datetime, Optional[datetime]]]] = defaultdict(list)
    vep_windows: dict[str, list[tuple[datetime, Optional[datetime]]]] = defaultdict(list)
    vep_access: dict[str, list[date]] = defaultdict(list)

    for obs in obs_list:
        for gnomad_variant_id in obs.gnomad_variant_ids:
            gnomad_windows[gnomad_variant_id].append((obs.valid_from, obs.valid_to))
        for clinvar_control_id in obs.clinvar_control_ids:
            clinvar_windows[clinvar_control_id].append((obs.valid_from, obs.valid_to))
        if obs.vep_consequence:
            vep_windows[obs.vep_consequence].append((obs.valid_from, obs.valid_to))
            if obs.vep_access_date is not None:
                vep_access[obs.vep_consequence].append(obs.vep_access_date)

    is_protein = False
    if gnomad_windows or clinvar_windows:
        allele_level = db.scalar(sa.select(Allele.level).where(Allele.id == allele_id))
        is_protein = allele_level == SequenceLevel.protein.value

    gnomad_live_taken = _allele_has_live_link(db, GnomadAlleleLink, allele_id)
    for gnomad_variant_id, windows in gnomad_windows.items():
        for valid_from, valid_to in _merge_intervals(windows):
            if is_protein and valid_to is None:
                # Force-closed, not left live: not subject to the single-live guard below, since it
                # never becomes a live row that could collide with uq_gnomad_allele_links_live.
                valid_to = run_at
                stats["protein_gnomad_links_closed"] += 1
            elif valid_to is None:
                if gnomad_live_taken:
                    logger.warning(
                        "Allele %s already has a live gnomAD link; skipping gnomad_variant %s.",
                        allele_id,
                        gnomad_variant_id,
                    )
                    continue
                gnomad_live_taken = True
            _emit_gnomad_link(db, allele_id, gnomad_variant_id, valid_from, valid_to, stats)

    for clinvar_control_id, windows in clinvar_windows.items():
        for valid_from, valid_to in _merge_intervals(windows):
            if is_protein and valid_to is None:
                valid_to = run_at
                stats["protein_clinvar_links_closed"] += 1
            _emit_clinvar_link(db, allele_id, clinvar_control_id, valid_from, valid_to, stats)

    vep_live_taken = _allele_has_live_link(db, VepAlleleConsequence, allele_id)
    for consequence, windows in vep_windows.items():
        observed_dates = vep_access[consequence]
        for valid_from, valid_to in _merge_intervals(windows):
            if valid_to is None:
                if vep_live_taken:
                    logger.warning("Allele %s already has a live VEP consequence; skipping %r.", allele_id, consequence)
                    continue
                vep_live_taken = True
            access_date = min(observed_dates) if observed_dates else valid_from.date()
            _emit_vep_consequence(db, allele_id, consequence, access_date, valid_from, valid_to, stats)


def _write_annotation_timelines(
    db: Session, observations: dict[int, list[_AnnotationObservation]], stats: Counter, run_at: datetime
) -> None:
    """Write each allele's annotation timelines, isolating conflicts per allele.

    Each allele is built + flushed inside its own SAVEPOINT: if a live-uniqueness collision still
    slips through the in-memory guard — e.g. the concurrently-running mapping pipeline commits a live
    link for a shared allele between the guard's committed-state read and this flush — only that
    allele's links roll back (counted as ``annotation_conflicts``), and the rest of the pass proceeds
    instead of the whole final commit aborting.

    ``run_at`` is a single timestamp for the whole pass (not re-read per allele), so every protein-level
    ClinVar link force-closed by :func:`_write_allele_timeline` in this run shares the same cutover
    instant rather than drifting with wall-clock time across a long batch.
    """
    for allele_id, obs_list in observations.items():
        try:
            with db.begin_nested():
                _write_allele_timeline(db, allele_id, obs_list, stats, run_at)
                db.flush()  # surface any live-uniqueness violation inside this allele's savepoint
        except IntegrityError:
            stats["annotation_conflicts"] += 1
            logger.warning("Annotation links for allele %s conflicted (live elsewhere); skipped.", allele_id)


def _observations_for_alleles(db: Session, allele_ids: list[int]) -> dict[int, list[_AnnotationObservation]]:
    """Rebuild annotation observations for one batch of alleles, from fresh DB state.

    Scoped by allele id, not by variant/chunk: the merge :func:`_write_allele_timeline` performs is only
    ever correct once *every* mapping version that ever authoritatively linked to a given allele has been
    observed, and alleles are shared arbitrarily across variants — a variant-id chunk boundary says
    nothing about which alleles are "done" (an allele's contributing variants can land in different
    chunks). Batching by allele id instead means every batch's observations are complete for the alleles
    in it, however many separate variant-chunks originally fed those alleles.

    Pairs each authoritative ``MappingRecord`` to its source ``MappedVariant`` version by
    ``(variant_id, mapped_date)`` — the record was created with that version's ``mapped_date`` — mirroring
    the pairing :func:`_migrate_variant` used when the record was first built.
    """
    links = db.execute(
        sa.select(
            MappingRecordAllele.allele_id,
            MappingRecord.variant_id,
            MappingRecord.mapped_date,
            MappingRecord.valid_from,
            MappingRecord.valid_to,
        )
        .join(MappingRecord, MappingRecordAllele.mapping_record_id == MappingRecord.id)
        .where(MappingRecordAllele.allele_id.in_(allele_ids), MappingRecordAllele.is_authoritative)
    ).all()
    if not links:
        return {}

    variant_ids = {variant_id for _, variant_id, _, _, _ in links}
    mv_by_key: dict[tuple[int, date], MappedVariant] = {
        (mv.variant_id, mv.mapped_date): mv
        for mv in db.scalars(sa.select(MappedVariant).where(MappedVariant.variant_id.in_(variant_ids))).all()
    }

    observations: dict[int, list[_AnnotationObservation]] = defaultdict(list)
    for allele_id, variant_id, mapped_date, valid_from, valid_to in links:
        mv = mv_by_key.get((variant_id, mapped_date))
        if mv is None:
            continue  # authoritative link survives independently of its source mapped_variant row
        observation = _observation_for(mv, valid_from, valid_to)
        if observation is not None:
            observations[allele_id].append(observation)
    return observations


def _backfill_annotation_timelines(
    db: Session, allele_ids: list[int], *, batch_size: int, run_at: datetime, stats: Counter
) -> None:
    """Build and commit annotation link timelines for ``allele_ids``, batch by batch.

    Each batch re-reads its own observations fresh from the DB (:func:`_observations_for_alleles`) and
    commits before moving to the next, so memory is bounded by ``batch_size`` alleles at a time
    regardless of how many millions of variants fed into ``allele_ids``. This replaces the previous
    design, which accumulated every ``_AnnotationObservation`` for the *whole run* in memory before
    writing anything at the very end — correct, but the accumulator's unbounded growth over a
    multi-million-variant run put steadily increasing pressure on the GC, degrading throughput well
    before the run completed (not just a flat per-call cost, but one that compounded with how much had
    already been processed).
    """
    total = len(allele_ids)
    pass_start = perf_counter()
    for offset in range(0, total, batch_size):
        chunk = allele_ids[offset : offset + batch_size]
        observations = _observations_for_alleles(db, chunk)
        if observations:
            _write_annotation_timelines(db, observations, stats, run_at)
            db.commit()
        processed = min(offset + batch_size, total)
        elapsed = perf_counter() - pass_start
        rate = processed / elapsed if elapsed else 0.0
        print(f"  Built timelines for {processed}/{total} allele(s)... ({rate:.0f}/s, {elapsed:.0f}s elapsed)")


def _migrate_variant(
    db: Session,
    variant_id: int,
    mvs: list[MappedVariant],
    stats: Counter,
    skipped: dict[str, list[int]],
    annotation_manager: AnnotationStatusManager,
    existing_records: set[tuple[int, date]],
    allele_cache: dict[str, Allele],
) -> set[int]:
    """Reconstruct as much of a variant's record history as its data supports, returning the ids of
    every allele this variant authoritatively touched.

    Also records one ``VRS_MAPPING`` event per version created (variant-subject) and one
    ``CLINGEN_ALLELE_ID`` / ``MAPPED_HGVS`` event per allele-touch (allele-subject), all reason
    :data:`EventReason.MIGRATED` — see :func:`do_migration`'s docstring for why this is scoped to the
    mapping/identity side only, not gnomAD/ClinVar/VEP.

    The caller only accumulates these bare ids (not the annotation payload itself) into a global,
    whole-run set — a dedup key is all a deferred, allele-batched annotation pass
    (:func:`_backfill_annotation_timelines`) needs to know which alleles to revisit; the actual
    gnomAD/ClinVar/VEP observations are re-read fresh from the DB per allele-batch there, not carried
    in memory from here. Carrying full ``_AnnotationObservation`` payloads for the whole run was the
    previous design and is what overloaded the heap at scale — see that function's docstring. Returns
    an empty set for a variant with no current version at all — that one is a full skip, since there is
    no live state to reconstruct.

    A version with no resolvable sequence level is skipped **individually**, not the whole variant:
    ``ValidTime`` history is exactly the mechanism for retaining partial provenance, so one bad version
    should not cost its siblings their records. A window's ``valid_from``/``valid_to`` only depend on
    its neighbors' ``mapped_date`` (computed up front in :func:`_reconstruct_windows`), never on
    whether a record actually got created for those neighbors — so skipping a window here doesn't
    distort the boundaries around it. Already-migrated windows (from a prior partial run that
    stopped here before this fix, or a live-level failure that keeps this variant eligible for retry —
    see :func:`_variant_ids_to_migrate`) are detected via ``existing_records`` (bulk pre-fetched once per
    chunk by the caller — see :func:`do_migration`) and skipped as a no-op, so a re-run is safe to
    resume mid-variant. Their annotations are not re-derived here; run ``rebuild-annotations`` after a
    resumed run to backfill any timelines missed by the skip.

    Every ``MappingRecord``/``Allele``/``MappingRecordAllele`` this variant needs is only ``db.add``'d
    here, not flushed, until one ``db.flush()`` at the end covering the whole variant — cut from one
    flush per *window* (this was the dominant cost at scale: round trips, not query time, were the
    bottleneck). ``MappingRecordAllele`` is built from the ``record``/``allele`` *objects*, not their
    ``.id``, specifically so it doesn't need them flushed first — SQLAlchemy resolves the FKs at flush
    time. Observations and ``MIGRATED`` events (which need real ``allele.id``/``variant_id`` values) are
    built in a second pass over ``pending`` *after* that single flush, once ids exist.

    Skips/anomalies are recorded into ``skipped`` (reason → variant ids), not logged per variant: a
    single unmappable score set (e.g. a large pairwise-haplotype library the mapper never converted)
    can contribute hundreds of thousands of skips, so :func:`do_migration` rolls them up into one line
    per score set at the end rather than emitting a warning each.
    """
    result = _reconstruct_windows(mvs)
    if result is None:
        stats["variants_skipped_no_current"] += 1
        skipped["no_current"].append(variant_id)
        logger.debug("Variant %s has mapped_variants but none current; skipped.", variant_id)
        return set()

    if result.live_precedes_history:
        # Not fatal — the clamp in _reconstruct_windows keeps windows non-inverted — but it silently
        # collapses a historical window to zero-width, so surface it loudly: this shape means the
        # live mapped_variant's mapped_date is older than a superseded one, which shouldn't happen
        # absent a source data-quality issue, and likely wants a re-map to fix at the source.
        stats["variants_live_precedes_history"] += 1
        skipped["live_precedes_history"].append(variant_id)

    # (mv, record, allele-or-None, hgvs_assay_level, valid_from, valid_to) per window actually created
    # this pass — resolved to real ids by one flush below, then walked again to emit events and collect
    # touched allele ids.
    pending: list[
        tuple[MappedVariant, MappingRecord, Optional[Allele], Optional[str], datetime, Optional[datetime]]
    ] = []

    for mv, valid_from, valid_to in result.windows:
        if (variant_id, mv.mapped_date) in existing_records:
            continue  # already migrated in a prior pass over this variant; resuming, not redoing

        level = _resolve_level(mv)
        if level is None:
            is_live = valid_to is None
            reason = "no_level_live" if is_live else "no_level_historical"
            stats[f"windows_skipped_{reason}"] += 1
            skipped[reason].append(variant_id)
            logger.debug(
                "Variant %s has an unattributable mapped_variant (id=%s, live=%s); window skipped.",
                variant_id,
                mv.id,
                is_live,
            )
            continue

        hgvs_assay_level, hgvs_source = _hgvs_assay_level_with_source(mv, level)
        if hgvs_source == "level_column":
            stats["hgvs_recovered_from_level_column"] += 1
        elif hgvs_source == "vrs_expression":
            stats["hgvs_recovered_from_vrs_expression"] += 1

        # Same treatment for the pre-mapped representation. MappingRecord.vrs_digest is not a dedup
        # key, so a wrong value here is less destructive than on Allele - but it is still an identity
        # claim about content this migration is republishing, and the drift has the same cause.
        pre_mapped_document, pre_mapped_digest = None, None
        if (mv.pre_mapped or {}).get("id"):
            try:
                pre_mapped_document, pre_mapped_digest = canonical_variation_document(
                    mv.pre_mapped, subject=f"mapped_variant {mv.id} pre_mapped"
                )
            except (ValidationError, ValueError):
                logger.exception(f"Could not canonicalize pre_mapped for mapped_variant {mv.id}; storing it unkeyed.")
                pre_mapped_document = mv.pre_mapped

        record = MappingRecord(
            variant_id=variant_id,
            vrs_digest=pre_mapped_digest,
            pre_mapped=pre_mapped_document,
            assay_level=level,
            hgvs_assay_level=hgvs_assay_level,
            mapped_date=mv.mapped_date,
            vrs_version=mv.vrs_version,
            mapping_api_version=mv.mapping_api_version,
            target_gene_mapping_id=mv.target_gene_mapping_id,
            alignment_level=mv.alignment_level,
            at_mismatched_locus=mv.at_mismatched_locus,
            near_gap=mv.near_gap,
            valid_from=valid_from,
            valid_to=valid_to,
        )
        db.add(record)
        stats["records"] += 1

        allele = _authoritative_allele(db, mv, level, allele_cache)
        if allele is not None:
            db.add(
                MappingRecordAllele(
                    mapping_record=record,
                    allele=allele,
                    is_authoritative=True,
                    valid_from=valid_from,
                    valid_to=valid_to,
                )
            )
            stats["record_alleles"] += 1

        pending.append((mv, record, allele, hgvs_assay_level, valid_from, valid_to))

    if not pending:
        return set()

    db.flush()  # one round trip resolves every record/allele/link id added for this whole variant
    stats["variants"] += 1

    touched_allele_ids: set[int] = set()
    for mv, record, allele, hgvs_assay_level, valid_from, valid_to in pending:
        # VRS_MAPPING: variant-subject, written for every version regardless of allele — mirrors the
        # live job (mapping.py), whose FAILED/ABSENT outcomes get a record but no allele too.
        if mv.error_message:
            mapping_disposition = Disposition.FAILED
        elif allele is not None:
            mapping_disposition = Disposition.PRESENT
        else:
            mapping_disposition = Disposition.ABSENT
        annotation_manager.record_event(
            AnnotationType.VRS_MAPPING,
            variant_id=variant_id,
            disposition=mapping_disposition,
            reason=EventReason.MIGRATED.value,
            source_version=mv.mapping_api_version,
        )
        stats["annotation_events_written"] += 1

        if allele is not None:
            # CLINGEN_ALLELE_ID / MAPPED_HGVS: allele-subject, only meaningful once an allele exists.
            # allele.id is real now — this loop runs after the single flush above.
            annotation_manager.record_event(
                AnnotationType.CLINGEN_ALLELE_ID,
                allele_id=allele.id,
                disposition=Disposition.PRESENT if mv.clingen_allele_id else Disposition.ABSENT,
                reason=EventReason.MIGRATED.value,
                source_version=mv.mapping_api_version,
            )
            annotation_manager.record_event(
                AnnotationType.MAPPED_HGVS,
                allele_id=allele.id,
                disposition=Disposition.PRESENT if hgvs_assay_level else Disposition.ABSENT,
                reason=EventReason.MIGRATED.value,
                source_version=mv.mapping_api_version,
            )
            stats["annotation_events_written"] += 2

            touched_allele_ids.add(allele.id)

    # Flushed once for the whole variant (not per event) — still inside this variant's own SAVEPOINT
    # (the caller's `with db.begin_nested():` wraps this entire call), so a buffered event never
    # outlives the rollback of the record/allele it references. See do_migration's except-block for the
    # backstop: if something *between* here and there still raised, pending events for this variant are
    # explicitly discarded rather than leaking into a later, unrelated flush.
    annotation_manager.flush()
    return touched_allele_ids


def _variant_ids_to_migrate(db: Session, score_set_urn: Optional[str]) -> list[int]:
    """Variant ids that have ``mapped_variants`` but no *live* ``MappingRecord`` yet.

    Keyed on a live record specifically, not "any record at all": a variant can have historical
    records from a prior pass but still be missing its live one (the live version's mapped_variant had
    no resolvable sequence level — see :func:`_migrate_variant`), and that variant must stay eligible
    for retry until a re-map (or a QC fix) lets its live window resolve. The parallel-tables invariant
    still makes "has mapped_variants and no live mapping record" the exact set of variants with
    incomplete substrate; native new-model variants have no ``mapped_variants``.
    """
    has_live_record = sa.select(MappingRecord.id).where(
        MappingRecord.variant_id == Variant.id, MappingRecord.valid_to.is_(None)
    )
    query = (
        sa.select(Variant.id)
        .join(MappedVariant, MappedVariant.variant_id == Variant.id)
        .where(~has_live_record.exists())
    )
    if score_set_urn is not None:
        query = query.join(ScoreSet, ScoreSet.id == Variant.score_set_id).where(ScoreSet.urn == score_set_urn)
    return list(db.scalars(query.distinct().order_by(Variant.id)).all())


_SKIP_REASONS = {
    "no_current": "mapped_variants present but none current",
    "no_level_historical": "a superseded mapped_variant with no resolvable sequence level (window skipped, siblings rescued)",
    "no_level_live": "the LIVE mapped_variant has no resolvable sequence level — no live record could be created; remains eligible for retry",
    "live_precedes_history": "the live mapped_variant's mapped_date precedes a historical one (data anomaly — a re-map candidate)",
}


def _log_skip_summary(db: Session, skipped: dict[str, list[int]], *, chunk: int = 5000) -> None:
    """Roll skipped variants up into one log line per (reason, score set).

    Skips cluster hard by score set — a whole library the mapper couldn't convert skips en masse — so
    a per-variant warning drowns the log while a per-score-set count pinpoints exactly which set to
    chase. Variant ids are resolved to score set URNs in chunks to keep the ``IN`` clauses bounded.
    """
    for reason, variant_ids in skipped.items():
        if not variant_ids:
            continue
        counts: Counter = Counter()
        for offset in range(0, len(variant_ids), chunk):
            part = variant_ids[offset : offset + chunk]
            for urn, n in db.execute(
                sa.select(ScoreSet.urn, sa.func.count())
                .join(Variant, Variant.score_set_id == ScoreSet.id)
                .where(Variant.id.in_(part))
                .group_by(ScoreSet.urn)
            ).all():
                counts[urn] += n
        detail = _SKIP_REASONS.get(reason, reason)
        logger.warning("Skipped %d variant(s) across %d score set(s) — %s:", len(variant_ids), len(counts), detail)
        for urn, n in counts.most_common():
            logger.warning("  %s: %d variant(s) skipped (%s).", urn, n, reason)


def do_migration(
    db: Session,
    *,
    score_set_urn: Optional[str] = None,
    batch_size: int = 1000,
    dry_run: bool = False,
    run_at: Optional[datetime] = None,
) -> None:
    """Backfill the allele substrate from ``mapped_variants``, committing per batch.

    ``run_at`` (default: now) is the single cutover instant used to force-close any protein-level
    ClinVar link that would otherwise land live — see :func:`_write_allele_timeline`. Exposed as a
    parameter mainly for deterministic tests; a real run just wants "now".

    Also backfills the ``annotation_event`` audit log — but only for the mapping/identity side
    (``VRS_MAPPING``, ``CLINGEN_ALLELE_ID``, ``MAPPED_HGVS``), not gnomAD/ClinVar/VEP. That scope split
    is deliberate: the mapping side is a genuine, non-lossy reconstruction (one real historical event
    per ``mapped_variants`` version, the same data already trusted enough to build ``MappingRecord``/
    ``Allele`` from), whereas gnomAD/ClinVar/VEP would need to guess at a created/reconfirmed/skipped
    cadence legacy data never recorded — and those sources get real, fresh events again once each score
    set is re-annotated, so the audit gap there is temporary, not permanent. See
    :data:`EventReason.MIGRATED`.
    """
    print("Backfilling the Allele substrate from mapped_variants...")
    if dry_run:
        print("  DRY RUN — no changes will be committed.")
    run_at = run_at or datetime.now(timezone.utc)
    annotation_manager = AnnotationStatusManager(db)

    variant_ids = _variant_ids_to_migrate(db, score_set_urn)
    print(f"Found {len(variant_ids)} variants to migrate.")

    stats: Counter = Counter()
    # Wall-clock spent in each phase, so a run reports where the time goes (batch build vs. the
    # final annotation pass) and whether the per-batch rate degrades as the tables grow.
    phase_seconds: Counter = Counter()
    # Ids only — a deduped allele can be shared by variants in different chunks, so its annotation
    # timeline can only be assembled once every contributing variant has been migrated, but the actual
    # gnomAD/ClinVar/VEP payload doesn't need to ride along in memory to get there: it's re-read fresh
    # from the DB per allele-batch in the deferred pass below (_backfill_annotation_timelines). Holding
    # just ids here (not full _AnnotationObservation objects) keeps this set's footprint tiny even at
    # millions of variants — see that function's docstring for why the old accumulate-everything design
    # degraded badly at scale.
    touched_allele_ids: set[int] = set()
    # Skipped variant ids by reason, rolled up into a per-score-set summary at the end instead of a
    # per-variant warning (one unmappable library can skip hundreds of thousands of variants).
    skipped: dict[str, list[int]] = defaultdict(list)

    run_start = perf_counter()
    for offset in range(0, len(variant_ids), batch_size):
        chunk = variant_ids[offset : offset + batch_size]

        mark = perf_counter()
        mvs_by_variant: dict[int, list[MappedVariant]] = {vid: [] for vid in chunk}
        for mv in db.scalars(sa.select(MappedVariant).where(MappedVariant.variant_id.in_(chunk))).all():
            mvs_by_variant[mv.variant_id].append(mv)
        phase_seconds["fetch_mapped_variants"] += perf_counter() - mark

        # Bulk pre-fetch, once per chunk instead of once per window — this (not query execution time)
        # was the actual bottleneck: a per-window round trip for "does this record already exist" plus
        # another for "does this allele already exist" dominates wall-clock at millions-of-rows scale,
        # far more than the cost of the queries themselves.
        mark = perf_counter()
        existing_records: set[tuple[int, date]] = set(
            db.execute(
                sa.select(MappingRecord.variant_id, MappingRecord.mapped_date).where(
                    MappingRecord.variant_id.in_(chunk)
                )
            ).all()
        )
        candidate_digests = {(mv.post_mapped or {}).get("id") for mvs in mvs_by_variant.values() for mv in mvs}
        candidate_digests.discard(None)
        allele_cache: dict[str, Allele] = {}
        if candidate_digests:
            for allele in db.scalars(sa.select(Allele).where(Allele.vrs_digest.in_(candidate_digests))).all():
                allele_cache[allele.vrs_digest] = allele
        phase_seconds["prefetch"] += perf_counter() - mark

        mark = perf_counter()
        for variant_id in chunk:
            # A per-variant SAVEPOINT so one bad variant rolls back only its own inserts (not the
            # whole batch), and its touched allele ids are simply never added rather than referencing
            # rolled-back rows.
            pending_events_before = len(annotation_manager._pending)
            try:
                with db.begin_nested():
                    variant_touched_allele_ids = _migrate_variant(
                        db,
                        variant_id,
                        mvs_by_variant[variant_id],
                        stats,
                        skipped,
                        annotation_manager,
                        existing_records,
                        allele_cache,
                    )
            except Exception as exc:  # noqa: BLE001 — one bad variant must not abort the batch
                # Discard any events this variant buffered but never got to flush before raising — they
                # would otherwise linger in the manager's queue and get inserted by a later, unrelated
                # flush, still referencing this variant/allele's now-rolled-back id (an FK violation far
                # from its actual cause). _migrate_variant flushes its own events before returning
                # normally, so this is a no-op on the success path.
                del annotation_manager._pending[pending_events_before:]
                stats["variants_errored"] += 1
                logger.exception("Failed to migrate variant %s: %s", variant_id, exc)
                continue
            touched_allele_ids.update(variant_touched_allele_ids)
        phase_seconds["build_records"] += perf_counter() - mark

        mark = perf_counter()
        if dry_run:
            db.rollback()
            touched_allele_ids.clear()  # nothing persisted this batch; these ids are now invalid
        else:
            db.commit()
        phase_seconds["commit"] += perf_counter() - mark

        processed = min(offset + batch_size, len(variant_ids))
        elapsed = perf_counter() - run_start
        rate = processed / elapsed if elapsed else 0.0
        print(f"  Processed {processed}/{len(variant_ids)} variants... ({rate:.0f}/s, {elapsed:.0f}s elapsed)")

    if not dry_run and touched_allele_ids:
        print(f"Building annotation timelines for {len(touched_allele_ids)} allele(s)...")
        mark = perf_counter()
        _backfill_annotation_timelines(
            db, sorted(touched_allele_ids), batch_size=batch_size, run_at=run_at, stats=stats
        )
        phase_seconds["annotation_timelines"] += perf_counter() - mark

    print("\nMigration completed:")
    for key in (
        "variants",
        "records",
        "record_alleles",
        "hgvs_recovered_from_level_column",
        "hgvs_recovered_from_vrs_expression",
        "annotation_events_written",
        "gnomad_links",
        "protein_gnomad_links_closed",
        "clinvar_links",
        "protein_clinvar_links_closed",
        "vep_consequences",
        "vep_source_version_resolved",
        "vep_source_version_legacy_fallback",
        "annotation_conflicts",
        "variants_skipped_no_current",
        "windows_skipped_no_level_historical",
        "windows_skipped_no_level_live",
        "variants_live_precedes_history",
        "variants_errored",
    ):
        print(f"  {key}: {stats[key]}")

    _log_skip_summary(db, skipped)

    total = perf_counter() - run_start
    print("\nTiming by phase:")
    for phase in ("fetch_mapped_variants", "build_records", "commit", "annotation_timelines"):
        seconds = phase_seconds[phase]
        share = f"{100 * seconds / total:.0f}%" if total else "n/a"
        print(f"  {phase}: {seconds:.1f}s ({share})")
    variants_done = stats["variants"]
    overall_rate = f"{variants_done / total:.0f}/s" if total else "n/a"
    print(f"  total: {total:.1f}s ({overall_rate})")


def _allele_ids_to_rebuild(db: Session, score_set_urn: Optional[str]) -> list[int]:
    """Every allele id with an authoritative link, optionally scoped to one score set's variants."""
    query = sa.select(sa.distinct(MappingRecordAllele.allele_id)).where(MappingRecordAllele.is_authoritative)
    if score_set_urn is not None:
        query = (
            query.join(MappingRecord, MappingRecordAllele.mapping_record_id == MappingRecord.id)
            .join(Variant, Variant.id == MappingRecord.variant_id)
            .join(ScoreSet, ScoreSet.id == Variant.score_set_id)
            .where(ScoreSet.urn == score_set_urn)
        )
    return list(db.scalars(query.order_by(MappingRecordAllele.allele_id)).all())


def rebuild_annotation_timelines(
    db: Session, *, score_set_urn: Optional[str] = None, batch_size: int = 1000, run_at: Optional[datetime] = None
) -> None:
    """(Re)build annotation link timelines for already-migrated variants, independently of record
    creation. Idempotent (existence-checked, savepoint-isolated). Use to recover after a crash in the
    ``migrate`` annotation pass, or to refresh timelines after fixing annotation logic.

    Rebuilds strictly from the legacy proxy data (``mapped_variants`` associations), same as
    ``migrate`` — it is not the cross-level fan-out job — so it applies the same protein-level ClinVar
    closure rule (see :func:`_write_allele_timeline`) using its own ``run_at`` (default: now). Batches by
    allele id via :func:`_backfill_annotation_timelines`, the same bounded-memory pass ``migrate`` itself
    uses — this used to accumulate every observation for the whole rebuild in memory before writing
    anything, which had the identical unbounded-growth problem the ``migrate`` pass was fixed for.
    """
    print("Rebuilding annotation timelines from existing records...")
    run_at = run_at or datetime.now(timezone.utc)
    allele_ids = _allele_ids_to_rebuild(db, score_set_urn)
    print(f"Found {len(allele_ids)} allele(s) with authoritative links.")

    stats: Counter = Counter()
    _backfill_annotation_timelines(db, allele_ids, batch_size=batch_size, run_at=run_at, stats=stats)

    print("\nRebuild completed:")
    for key in (
        "gnomad_links",
        "protein_gnomad_links_closed",
        "clinvar_links",
        "protein_clinvar_links_closed",
        "vep_consequences",
        "vep_source_version_resolved",
        "vep_source_version_legacy_fallback",
        "annotation_conflicts",
    ):
        print(f"  {key}: {stats[key]}")


def verify_migration(db: Session, *, score_set_urn: Optional[str] = None) -> None:
    """Report substrate coverage without writing."""
    print("\nVerifying backfill...")

    remaining = len(_variant_ids_to_migrate(db, score_set_urn))
    total_mapped_variant_variants = db.scalar(sa.select(sa.func.count(sa.distinct(MappedVariant.variant_id))))
    total_records = db.scalar(sa.select(sa.func.count(MappingRecord.id)))
    live_records = db.scalar(sa.select(sa.func.count(MappingRecord.id)).where(MappingRecord.valid_to.is_(None)))
    total_alleles = db.scalar(sa.select(sa.func.count(Allele.id)))
    live_auth_links = db.scalar(
        sa.select(sa.func.count(MappingRecordAllele.id)).where(
            MappingRecordAllele.is_authoritative, MappingRecordAllele.valid_to.is_(None)
        )
    )
    gnomad_links = db.scalar(sa.select(sa.func.count(GnomadAlleleLink.id)))
    clinvar_links = db.scalar(sa.select(sa.func.count(ClinvarAlleleLink.id)))
    vep_consequences = db.scalar(sa.select(sa.func.count(VepAlleleConsequence.id)))
    migrated_events = db.scalar(
        sa.select(sa.func.count(AnnotationEvent.id)).where(AnnotationEvent.reason == EventReason.MIGRATED.value)
    )

    print(f"  Variants with mapped_variants:      {total_mapped_variant_variants}")
    print(f"  Variants still needing backfill:    {remaining}")
    print(f"  MappingRecords (total / live):      {total_records} / {live_records}")
    print(f"  Alleles (deduped):                  {total_alleles}")
    print(f"  Live authoritative links:           {live_auth_links}")
    print(f"  gnomAD / ClinVar / VEP annotations: {gnomad_links} / {clinvar_links} / {vep_consequences}")
    print(f"  MIGRATED annotation_event rows:      {migrated_events}")
    if remaining == 0:
        print("  ✓ Every legacy variant has a MappingRecord.")
    else:
        print(f"  ⚠ {remaining} variants remain (see logged anomalies, or re-run migrate).")


def rollback_migration(db: Session) -> None:
    """Delete backfilled ``MappingRecord``s (cascading their allele links).

    Scoped to variants that also have ``mapped_variants`` — the exact discriminator
    for backfilled records, since native new-model variants have no legacy rows.
    Deduped ``Allele``s and annotation links are left in place: alleles are content
    -addressed and harmless if orphaned (a re-run reuses them), and annotation
    links re-existence-check. Delete those manually if a full teardown is needed.

    ``AnnotationEvent`` rows (reason ``MIGRATED``) are left in place too, on purpose: it is an
    append-only audit log, and deleting them on rollback would erase the very audit trail it exists to
    keep — a re-run after rollback just appends a fresh set (matching how ``MappingRecord`` itself gets
    deleted and recreated), not a duplicate-avoidance concern the way the ValidTime domain tables are.
    """
    print("Rolling back backfilled MappingRecords (scoped to variants with mapped_variants)...")
    legacy_variant_ids = sa.select(sa.distinct(MappedVariant.variant_id))
    count = db.scalar(
        sa.select(sa.func.count(MappingRecord.id)).where(MappingRecord.variant_id.in_(legacy_variant_ids))
    )
    # ON DELETE CASCADE on mapping_record_alleles.mapping_record_id removes the links.
    db.execute(sa.delete(MappingRecord).where(MappingRecord.variant_id.in_(legacy_variant_ids)))
    db.commit()
    print(f"Deleted {count} MappingRecords (and their allele links via cascade).")
    print("Note: deduped Alleles, annotation links, and MIGRATED annotation_event rows were left in place")
    print("(re-runnable / harmless orphans; the event log is append-only by design).")


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Backfill the Allele substrate from mapped_variants.")
    parser.add_argument(
        "command", nargs="?", default="migrate", choices=["migrate", "verify", "rollback", "rebuild-annotations"]
    )
    parser.add_argument("--score-set", dest="score_set_urn", default=None, help="Limit to one score set URN.")
    parser.add_argument("--batch-size", type=int, default=1000, help="Variants per commit (default 1000).")
    parser.add_argument("--dry-run", action="store_true", help="Run without committing (rolls back each batch).")
    return parser.parse_args()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    args = _parse_args()

    if args.command == "rollback":
        print("WARNING: this deletes all backfilled MappingRecords and their allele links.")
        if input("Are you sure you want to continue? (y/N): ").lower() == "y":
            with SessionLocal() as session:
                rollback_migration(session)
        else:
            print("Rollback cancelled.")
    elif args.command == "verify":
        with SessionLocal() as session:
            verify_migration(session, score_set_urn=args.score_set_urn)
    elif args.command == "rebuild-annotations":
        with SessionLocal() as session:
            rebuild_annotation_timelines(session, score_set_urn=args.score_set_urn, batch_size=args.batch_size)
            verify_migration(session, score_set_urn=args.score_set_urn)
    else:
        with SessionLocal() as session:
            do_migration(
                session,
                score_set_urn=args.score_set_urn,
                batch_size=args.batch_size,
                dry_run=args.dry_run,
            )
            verify_migration(session, score_set_urn=args.score_set_urn)
