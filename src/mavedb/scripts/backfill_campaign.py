"""Read-only campaign map and safe-to-drop gate for the MappedVariant → allele-substrate backfill.

The backfill has two halves (see ``alembic/manual_migrations/migrate_mapped_variants_to_allele_substrate.py``
for the reshape and ``run_score_set_pipelines.py`` for the enrichment): a deterministic *reshape* that
reconstructs the measured-level spine and source-annotation links from the frozen ``mapped_variants`` rows,
and a decoupled *enrichment* that runs reverse translation (the cross-level fan-out) and re-runs the source
annotations against the substrate. This script does not move data; it reports where every published score set
sits between those two states, and whether the frozen ``mapped_variants`` tables are safe to drop.

Commands
--------
coverage
    The campaign's map of what's left. For each score set: reshaped? RT fan-out present? gnomAD / ClinVar /
    ClinGen / VEP enrichment present (and, when versions are supplied, at the current version)? Roll-up
    counts show how many score sets are in each state.

reconcile
    The gate that should block dropping the ``mapped_variants`` tables. Reports measured-level coverage
    parity between the frozen tables and the substrate, the expected divergence the substrate adds (RT
    fan-out, advanced annotation versions), and a static scan confirming no code outside the known
    migration/export allowlist still queries the legacy tables. Exits non-zero unless the drop is safe, so it
    can gate a release step in CI or a runbook.

Usage:
    poetry run python -m mavedb.scripts.backfill_campaign coverage --published-only
    poetry run python -m mavedb.scripts.backfill_campaign coverage --published-only --incomplete-only --json
    poetry run python -m mavedb.scripts.backfill_campaign reconcile --published-only
    poetry run python -m mavedb.scripts.backfill_campaign reconcile --published-only \\
        --vep-source-version 116 --vep-resolver-version 3
"""

import dataclasses
import json
import logging
import pathlib
import re
from typing import Optional, Sequence

import asyncclick as click
from sqlalchemy import and_, distinct, func, select
from sqlalchemy.orm import Session

from mavedb.db.session import SessionLocal
from mavedb.models.allele import Allele
from mavedb.models.clinvar_allele_link import ClinvarAlleleLink
from mavedb.models.gnomad_allele_link import GnomadAlleleLink
from mavedb.models.mapped_variant import MappedVariant
from mavedb.models.mapping_record import MappingRecord
from mavedb.models.mapping_record_allele import MappingRecordAllele
from mavedb.models.score_set import ScoreSet
from mavedb.models.variant import Variant
from mavedb.models.vep_allele_consequence import VepAlleleConsequence
from mavedb.scripts.environment import script_environment

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Coverage model
# ---------------------------------------------------------------------------


# The lifecycle states a score set moves through during the campaign, in order. classify_state returns the
# furthest state a score set has reached; the roll-up counts how many sit at each.
STATE_NONE = "none"  # no live mapping_record: reshape has not run
STATE_PARTIAL_RESHAPE = "partial_reshape"  # some but not all legacy-current variants have a live record
STATE_RESHAPED = "reshaped"  # measured-level spine complete, no RT fan-out yet
STATE_RT = "rt"  # reverse-translation fan-out present, but enrichment incomplete
STATE_ENRICHED = "enriched"  # RT + gnomAD + ClinVar + ClinGen + VEP all present (at current version if pinned)

STATE_ORDER = [STATE_NONE, STATE_PARTIAL_RESHAPE, STATE_RESHAPED, STATE_RT, STATE_ENRICHED]


@dataclasses.dataclass
class ScoreSetCoverage:
    """Substrate state of one score set, as counts over its variants and their live alleles."""

    score_set_id: int
    urn: Optional[str]
    n_variants: int = 0
    n_legacy_variants: int = 0  # variants with a live (current) legacy MappedVariant — the reshape denominator
    n_reshaped_variants: int = 0  # variants with a live MappingRecord
    n_live_alleles: int = 0
    n_nonauthoritative_alleles: int = 0  # RT fan-out breadth; reshape mints only authoritative alleles
    has_projection_group: bool = False
    n_clingen_alleles: int = 0
    n_gnomad_alleles: int = 0
    n_clinvar_alleles: int = 0
    n_vep_alleles: int = 0
    n_vep_current_alleles: int = 0  # only meaningful when VEP versions were pinned on the command line

    @property
    def is_reshaped(self) -> bool:
        """Every legacy-served variant has a live MappingRecord (the measured level is complete)."""
        return self.n_legacy_variants > 0 and self.n_reshaped_variants >= self.n_legacy_variants

    @property
    def is_partially_reshaped(self) -> bool:
        return 0 < self.n_reshaped_variants < self.n_legacy_variants

    @property
    def has_rt(self) -> bool:
        """Reverse translation ran: it is the only producer of non-authoritative sibling alleles."""
        return self.n_nonauthoritative_alleles > 0

    @property
    def has_gnomad(self) -> bool:
        return self.n_gnomad_alleles > 0

    @property
    def has_clinvar(self) -> bool:
        return self.n_clinvar_alleles > 0

    @property
    def has_clingen(self) -> bool:
        return self.n_clingen_alleles > 0

    def has_vep(self, *, require_current: bool) -> bool:
        return (self.n_vep_current_alleles if require_current else self.n_vep_alleles) > 0


def classify_state(cov: ScoreSetCoverage, *, require_current_vep: bool) -> str:
    """The furthest lifecycle state a score set has reached.

    ``enriched`` is defined by having all four source annotations present — NOT by the RT fan-out breadth.
    Reverse translation legitimately produces zero sibling alleles for a single-level score set (nothing to
    project across coordinate frames), so gating ``enriched`` on non-authoritative alleles would strand such a
    score set at ``reshaped`` forever even after it is fully annotated. VEP depends on reverse translation
    (SUCCESS_REQUIRED), so a live VEP consequence already implies RT ran; the fan-out count is reported as its
    own column rather than used as a state gate.

    A score set legitimately carrying no ClinVar or gnomAD overlap would never reach ``enriched`` by the
    presence rule; the campaign treats presence as the signal and relies on the reconcile parity report, not
    this roll-up, to catch a score set that *should* have had a link and doesn't.
    """
    if cov.n_reshaped_variants == 0:
        return STATE_NONE
    if cov.is_partially_reshaped:
        return STATE_PARTIAL_RESHAPE
    fully_enriched = (
        cov.has_gnomad and cov.has_clinvar and cov.has_clingen and cov.has_vep(require_current=require_current_vep)
    )
    if fully_enriched:
        return STATE_ENRICHED
    # RT has run (fan-out siblings exist, or a VEP consequence — which depends on RT — is present) but at least
    # one source annotation is still missing.
    if cov.has_rt or cov.has_vep(require_current=False):
        return STATE_RT
    return STATE_RESHAPED


# ---------------------------------------------------------------------------
# Coverage queries
# ---------------------------------------------------------------------------


def resolve_cohort_ids(
    db: Session,
    *,
    published_only: bool,
    include_private: bool,
    collection_urn: Optional[str],
    explicit_urns: Optional[Sequence[str]],
) -> list[int]:
    """Score-set ids in scope. Filters AND together; ``--published-only`` covers accession-based score sets
    identically to sequence-based ones (it discriminates only on ``published_date``), so the historical
    taxonomy-join pitfall that silently dropped accession-based score sets cannot recur here."""
    query = select(ScoreSet.id).where(ScoreSet.urn.is_not(None))
    if published_only:
        query = query.where(ScoreSet.published_date.is_not(None))
    if not include_private and not published_only:
        query = query.where(ScoreSet.private.is_(False))
    if explicit_urns:
        query = query.where(ScoreSet.urn.in_(list(explicit_urns)))
    if collection_urn:
        from mavedb.models.collection import Collection
        from mavedb.models.collection_score_set_association import CollectionScoreSetAssociation

        query = (
            query.join(CollectionScoreSetAssociation, CollectionScoreSetAssociation.score_set_id == ScoreSet.id)
            .join(Collection, Collection.id == CollectionScoreSetAssociation.collection_id)
            .where(Collection.urn == collection_urn)
        )
    return list(db.scalars(query).all())


def compute_coverage(
    db: Session,
    score_set_ids: Sequence[int],
    *,
    vep_source_version: Optional[str] = None,
    vep_resolver_version: Optional[str] = None,
) -> dict[int, ScoreSetCoverage]:
    """Substrate coverage for each score set id, as a handful of grouped aggregate passes joined in Python.

    Every count is over *live* substrate rows (``valid_to IS NULL``); a superseded record or link does not
    count as coverage. Allele-level counts run over the live authoritative-and-RT alleles reachable from each
    score set's live mapping records.
    """
    ids = list(score_set_ids)
    coverage = {sid: ScoreSetCoverage(score_set_id=sid, urn=None) for sid in ids}
    if not ids:
        return coverage

    for sid, urn in db.execute(select(ScoreSet.id, ScoreSet.urn).where(ScoreSet.id.in_(ids))):
        coverage[sid].urn = urn

    for sid, n in db.execute(
        select(Variant.score_set_id, func.count(Variant.id))
        .where(Variant.score_set_id.in_(ids))
        .group_by(Variant.score_set_id)
    ):
        coverage[sid].n_variants = n

    for sid, n in db.execute(
        select(Variant.score_set_id, func.count(distinct(MappedVariant.variant_id)))
        .join(Variant, Variant.id == MappedVariant.variant_id)
        .where(Variant.score_set_id.in_(ids), MappedVariant.current.is_(True))
        .group_by(Variant.score_set_id)
    ):
        coverage[sid].n_legacy_variants = n

    for sid, n in db.execute(
        select(Variant.score_set_id, func.count(distinct(MappingRecord.variant_id)))
        .join(Variant, Variant.id == MappingRecord.variant_id)
        .where(Variant.score_set_id.in_(ids), MappingRecord.valid_to.is_(None))
        .group_by(Variant.score_set_id)
    ):
        coverage[sid].n_reshaped_variants = n

    # Allele-level aggregates over live records -> live record-allele links -> alleles.
    live_allele_join = (
        select(
            Variant.score_set_id.label("sid"),
            func.count(distinct(MappingRecordAllele.allele_id)).label("n_alleles"),
            func.count(distinct(MappingRecordAllele.allele_id))
            .filter(MappingRecordAllele.is_authoritative.is_(False))
            .label("n_nonauth"),
            func.bool_or(MappingRecordAllele.projection_group.is_not(None)).label("has_pg"),
            func.count(distinct(MappingRecordAllele.allele_id))
            .filter(Allele.clingen_allele_id.is_not(None))
            .label("n_clingen"),
        )
        .select_from(Variant)
        .join(MappingRecord, and_(MappingRecord.variant_id == Variant.id, MappingRecord.valid_to.is_(None)))
        .join(
            MappingRecordAllele,
            and_(
                MappingRecordAllele.mapping_record_id == MappingRecord.id,
                MappingRecordAllele.valid_to.is_(None),
            ),
        )
        .join(Allele, Allele.id == MappingRecordAllele.allele_id)
        .where(Variant.score_set_id.in_(ids))
        .group_by(Variant.score_set_id)
    )
    for sid, n_alleles, n_nonauth, has_pg, n_clingen in db.execute(live_allele_join):
        cov = coverage[sid]
        cov.n_live_alleles = n_alleles
        cov.n_nonauthoritative_alleles = n_nonauth
        cov.has_projection_group = bool(has_pg)
        cov.n_clingen_alleles = n_clingen

    _fill_link_counts(db, ids, coverage, GnomadAlleleLink, "n_gnomad_alleles")
    _fill_link_counts(db, ids, coverage, ClinvarAlleleLink, "n_clinvar_alleles")
    _fill_vep_counts(db, ids, coverage, vep_source_version, vep_resolver_version)

    return coverage


def _live_allele_ids_for_scoreset():
    """A correlated live-allele subquery keyed by score set, shared by the per-source link counters."""
    return (
        select(Variant.score_set_id.label("sid"), MappingRecordAllele.allele_id.label("allele_id"))
        .select_from(Variant)
        .join(MappingRecord, and_(MappingRecord.variant_id == Variant.id, MappingRecord.valid_to.is_(None)))
        .join(
            MappingRecordAllele,
            and_(
                MappingRecordAllele.mapping_record_id == MappingRecord.id,
                MappingRecordAllele.valid_to.is_(None),
            ),
        )
    )


def _fill_link_counts(db: Session, ids: Sequence[int], coverage: dict, link_model, attr: str) -> None:
    live_alleles = _live_allele_ids_for_scoreset().where(Variant.score_set_id.in_(ids)).subquery()
    query = (
        select(live_alleles.c.sid, func.count(distinct(live_alleles.c.allele_id)))
        .join(link_model, and_(link_model.allele_id == live_alleles.c.allele_id, link_model.valid_to.is_(None)))
        .group_by(live_alleles.c.sid)
    )
    for sid, n in db.execute(query):
        setattr(coverage[sid], attr, n)


def _fill_vep_counts(
    db: Session,
    ids: Sequence[int],
    coverage: dict,
    vep_source_version: Optional[str],
    vep_resolver_version: Optional[str],
) -> None:
    live_alleles = _live_allele_ids_for_scoreset().where(Variant.score_set_id.in_(ids)).subquery()
    base = (
        select(live_alleles.c.sid, func.count(distinct(live_alleles.c.allele_id)))
        .join(
            VepAlleleConsequence,
            and_(VepAlleleConsequence.allele_id == live_alleles.c.allele_id, VepAlleleConsequence.valid_to.is_(None)),
        )
        .group_by(live_alleles.c.sid)
    )
    for sid, n in db.execute(base):
        coverage[sid].n_vep_alleles = n

    if vep_source_version is None:
        return
    # "Current" mirrors the VEP job's own skip predicate: both the Ensembl release and the resolver version
    # must match. A pinned resolver version of "" is treated as "match the source version only".
    conds = [VepAlleleConsequence.source_version == vep_source_version]
    if vep_resolver_version:
        conds.append(VepAlleleConsequence.resolver_version == vep_resolver_version)
    current = (
        select(live_alleles.c.sid, func.count(distinct(live_alleles.c.allele_id)))
        .join(
            VepAlleleConsequence,
            and_(VepAlleleConsequence.allele_id == live_alleles.c.allele_id, VepAlleleConsequence.valid_to.is_(None)),
        )
        .where(*conds)
        .group_by(live_alleles.c.sid)
    )
    for sid, n in db.execute(current):
        coverage[sid].n_vep_current_alleles = n


# ---------------------------------------------------------------------------
# coverage command
# ---------------------------------------------------------------------------


def _coverage_row(cov: ScoreSetCoverage, state: str) -> dict:
    return {
        "score_set_urn": cov.urn or "(no urn)",
        "state": state,
        "n_variants": cov.n_variants,
        "n_legacy": cov.n_legacy_variants,
        "n_reshaped": cov.n_reshaped_variants,
        "reshaped": cov.is_reshaped,
        "rt": cov.has_rt,
        "n_rt_alleles": cov.n_nonauthoritative_alleles,
        "gnomad": cov.has_gnomad,
        "clinvar": cov.has_clinvar,
        "clingen": cov.has_clingen,
        "vep": cov.has_vep(require_current=False),
        "vep_current": cov.n_vep_current_alleles,
    }


@script_environment.command(name="coverage")
@click.option("--published-only", is_flag=True, help="Only published score sets (covers accession-based sets).")
@click.option("--include-private", is_flag=True, help="Include private score sets (ignored with --published-only).")
@click.option("--collection-urn", default=None, help="Only score sets in this collection.")
@click.option("--score-set-urn", "score_set_urns", multiple=True, help="Restrict to these URNs (repeatable).")
@click.option("--vep-source-version", default=None, help="Ensembl release counted as current for VEP (e.g. 116).")
@click.option("--vep-resolver-version", default=None, help="Resolver version counted as current for VEP.")
@click.option("--incomplete-only", is_flag=True, help="Only score sets not yet fully enriched.")
@click.option("--limit", type=int, default=None, help="Cap rows (after filtering); the roll-up still counts all.")
@click.option("--json", "as_json", is_flag=True, help="Emit rows as JSON.")
async def coverage(
    published_only: bool,
    include_private: bool,
    collection_urn: Optional[str],
    score_set_urns: tuple[str, ...],
    vep_source_version: Optional[str],
    vep_resolver_version: Optional[str],
    incomplete_only: bool,
    limit: Optional[int],
    as_json: bool,
) -> None:
    """The campaign's map of what's left: substrate state of every in-scope score set."""
    require_current_vep = vep_source_version is not None
    with SessionLocal() as db:
        ids = resolve_cohort_ids(
            db,
            published_only=published_only,
            include_private=include_private,
            collection_urn=collection_urn,
            explicit_urns=score_set_urns or None,
        )
        coverage_by_id = compute_coverage(
            db, ids, vep_source_version=vep_source_version, vep_resolver_version=vep_resolver_version
        )

    covs = sorted(coverage_by_id.values(), key=lambda c: c.urn or "")
    states = {cov.score_set_id: classify_state(cov, require_current_vep=require_current_vep) for cov in covs}

    rollup = {state: 0 for state in STATE_ORDER}
    for state in states.values():
        rollup[state] += 1

    rows = [_coverage_row(cov, states[cov.score_set_id]) for cov in covs]
    if incomplete_only:
        rows = [r for r in rows if r["state"] != STATE_ENRICHED]

    if as_json:
        click.echo(
            json.dumps({"rollup": rollup, "total": len(covs), "score_sets": rows[:limit]}, indent=2, default=str)
        )
        return

    click.echo(f"Cohort: {len(covs)} score set(s)")
    click.echo("Campaign state roll-up (furthest state reached):")
    for state in STATE_ORDER:
        click.echo(f"  {state:<16} {rollup[state]:>6}")
    if require_current_vep:
        click.echo(f"  (VEP 'current' pinned to source={vep_source_version} resolver={vep_resolver_version or 'any'})")
    click.echo()

    shown = rows[:limit]
    if not shown:
        click.echo("No score sets to show.")
        return

    header = (
        f"{'URN':<26}  {'STATE':<16}  {'VARS':>7}  {'LEGACY':>7}  {'RESHAP':>7}  "
        f"{'RT':>3}  {'RTALL':>6}  {'GNO':>3}  {'CLV':>3}  {'CLG':>3}  {'VEP':>3}"
    )
    click.echo(header)
    click.echo("-" * len(header))
    for r in shown:
        click.echo(
            f"{r['score_set_urn']:<26}  {r['state']:<16}  {r['n_variants']:>7}  {r['n_legacy']:>7}  "
            f"{r['n_reshaped']:>7}  {_yn(r['rt']):>3}  {r['n_rt_alleles']:>6}  {_yn(r['gnomad']):>3}  "
            f"{_yn(r['clinvar']):>3}  {_yn(r['clingen']):>3}  {_yn(r['vep']):>3}"
        )
    if limit is not None and len(rows) > limit:
        click.echo(f"... {len(rows) - limit} more row(s) not shown (--limit {limit}).")


def _yn(value: bool) -> str:
    return "yes" if value else "-"


# ---------------------------------------------------------------------------
# reconcile command — the safe-to-drop gate
# ---------------------------------------------------------------------------


# Files allowed to query the legacy MappedVariant tables. The reshape migration reads them by design; the
# export script's legacy artifact and this campaign tool query them intentionally, and are retired/removed with
# the drop. Any other file issuing a query against the frozen tables would be stranded by the drop, so the
# reader scan fails on a query idiom found outside this set.
#
# Deliberately NOT in scope: ORM relationship declarations, imports, type aliases, and docstrings that merely
# name MappedVariant. Those are definitions, not readers — dropping the tables breaks them at mapper-config
# time, but they are removed as part of the drop changeset itself (the runbook enumerates them), not cleared
# ahead of it. The scan matches executable query idioms so those declarations do not produce noise.
LEGACY_READER_ALLOWLIST = {
    "src/mavedb/scripts/export_public_data.py",
    "src/mavedb/scripts/mapped_gene_from_mapped_variant.py",
    "src/mavedb/scripts/backfill_campaign.py",
}

# A reader is a SQLAlchemy query construct against the model or its association tables — a select/query
# targeting it, a join onto it, or an eager-loader pulling it. This intentionally does not match
# ``relationship(... "MappedVariant")``, ``secondary=..._association_table``, ``delete(MappedVariant)`` (a
# writer, removed with the drop), imports, ``Union[..., MappedVariant, ...]``, or prose.
_LEGACY_READER_PATTERNS = [
    re.compile(r"\bselect\(\s*MappedVariant\b"),
    re.compile(r"\.query\(\s*MappedVariant\b"),
    re.compile(r"\bjoin\(\s*MappedVariant\b"),
    re.compile(r"\b(?:joinedload|selectinload|contains_eager|subqueryload)\([^)]*MappedVariant\b"),
    # The association tables are only ever referenced as ``secondary=..._association_table`` in a relationship
    # (a declaration, ignored) or in a real query, where a call paren sits immediately before the raw table
    # name: ``select_from(gnomad_variants_mapped_variants)``, ``join(mapped_variants_clinical_controls, ...)``.
    # The trailing ``\b`` before ``_association_table`` fails to match, so the ``secondary=`` symbol is skipped.
    re.compile(r"\((?:gnomad_variants_mapped_variants|mapped_variants_clinical_controls)\b"),
]


@dataclasses.dataclass
class AnnotationParity:
    """Per-variant annotation coverage for one source, legacy vs substrate.

    ``regressed`` is the load-bearing number: variants the frozen tables annotated for this source that have
    NO live substrate annotation of it on any of their live alleles. Dropping the frozen tables while this is
    non-zero silently regresses that source's serving. It is expected to be non-zero after reshape-only for a
    protein-authoritative score set — the reshape force-closes gnomAD/ClinVar links on a protein allele, and
    only the RT fan-out's genomic/coding sibling plus re-annotation makes them live again — which is exactly
    why enrichment must complete before the drop.
    """

    source: str
    legacy_variants: int
    substrate_variants: int
    regressed_variants: int


@dataclasses.dataclass
class ReconcileReport:
    cohort_size: int
    legacy_current_variants: int
    reshaped_variants: int
    missing_reshape_variants: int  # legacy-current variants with no live MappingRecord — the coverage gate
    rt_fanout_alleles: int  # expected divergence: substrate breadth the legacy tables never had
    annotation_parity: list[AnnotationParity]
    unexpected_legacy_readers: list[str]

    @property
    def measured_parity_ok(self) -> bool:
        return self.missing_reshape_variants == 0

    @property
    def annotation_parity_ok(self) -> bool:
        """No source lost live per-variant coverage the frozen tables had."""
        return all(parity.regressed_variants == 0 for parity in self.annotation_parity)

    @property
    def readers_clear(self) -> bool:
        return not self.unexpected_legacy_readers

    @property
    def safe_to_drop(self) -> bool:
        return self.measured_parity_ok and self.annotation_parity_ok and self.readers_clear


def scan_legacy_readers(repo_root: pathlib.Path) -> list[str]:
    """Repo-relative ``path:line`` sites, outside the allowlist, that *query* the frozen MappedVariant tables.

    This is the static half of the drop gate: the reconcile DB parity proves the substrate *covers* the data,
    and this proves nothing outside the migration/export allowlist still *reads* the frozen tables. A new query
    reader added after the campaign lands trips this even if the data is fully backfilled.
    """
    src = repo_root / "src" / "mavedb"
    hits: list[str] = []
    for path in sorted(src.rglob("*.py")):
        rel = path.relative_to(repo_root).as_posix()
        if rel in LEGACY_READER_ALLOWLIST:
            continue
        try:
            text = path.read_text(encoding="utf-8")
        except (OSError, UnicodeDecodeError):
            continue
        for lineno, line in enumerate(text.splitlines(), start=1):
            if line.lstrip().startswith("#"):
                continue
            if any(pattern.search(line) for pattern in _LEGACY_READER_PATTERNS):
                hits.append(f"{rel}:{lineno}")
    return hits


def compute_reconcile(db: Session, score_set_ids: Sequence[int], *, repo_root: pathlib.Path) -> ReconcileReport:
    ids = list(score_set_ids)
    id_filter = Variant.score_set_id.in_(ids) if ids else None

    def scoped(query):
        return query.where(id_filter) if id_filter is not None else query

    legacy_current_variants = db.scalar(
        scoped(
            select(func.count(distinct(MappedVariant.variant_id)))
            .select_from(MappedVariant)
            .join(Variant, Variant.id == MappedVariant.variant_id)
            .where(MappedVariant.current.is_(True))
        )
    )

    reshaped_variants = db.scalar(
        scoped(
            select(func.count(distinct(MappingRecord.variant_id)))
            .select_from(MappingRecord)
            .join(Variant, Variant.id == MappingRecord.variant_id)
            .where(MappingRecord.valid_to.is_(None))
        )
    )

    # The gate: legacy-current variants with no live MappingRecord. Dropping the frozen tables while this is
    # non-zero strands those variants — they would have no substrate row to serve from.
    live_record_exists = (
        select(MappingRecord.id)
        .where(MappingRecord.variant_id == MappedVariant.variant_id, MappingRecord.valid_to.is_(None))
        .exists()
    )
    missing_reshape_variants = db.scalar(
        scoped(
            select(func.count(distinct(MappedVariant.variant_id)))
            .select_from(MappedVariant)
            .join(Variant, Variant.id == MappedVariant.variant_id)
            .where(MappedVariant.current.is_(True), ~live_record_exists)
        )
    )

    rt_fanout_alleles = db.scalar(
        scoped(
            select(func.count(distinct(MappingRecordAllele.allele_id)))
            .select_from(Variant)
            .join(MappingRecord, and_(MappingRecord.variant_id == Variant.id, MappingRecord.valid_to.is_(None)))
            .join(
                MappingRecordAllele,
                and_(
                    MappingRecordAllele.mapping_record_id == MappingRecord.id,
                    MappingRecordAllele.valid_to.is_(None),
                    MappingRecordAllele.is_authoritative.is_(False),
                ),
            )
        )
    )

    annotation_parity = _annotation_parity(db, id_filter)

    return ReconcileReport(
        cohort_size=len(ids),
        legacy_current_variants=legacy_current_variants or 0,
        reshaped_variants=reshaped_variants or 0,
        missing_reshape_variants=missing_reshape_variants or 0,
        rt_fanout_alleles=rt_fanout_alleles or 0,
        annotation_parity=annotation_parity,
        unexpected_legacy_readers=scan_legacy_readers(repo_root),
    )


def _substrate_link_exists(variant_id_col, link_model):
    """A correlated EXISTS: the variant has a live *link_model* row on a live allele of its live record."""
    return (
        select(1)
        .select_from(MappingRecord)
        .join(
            MappingRecordAllele,
            and_(MappingRecordAllele.mapping_record_id == MappingRecord.id, MappingRecordAllele.valid_to.is_(None)),
        )
        .join(link_model, and_(link_model.allele_id == MappingRecordAllele.allele_id, link_model.valid_to.is_(None)))
        .where(MappingRecord.variant_id == variant_id_col, MappingRecord.valid_to.is_(None))
        .exists()
    )


def _annotation_parity(db: Session, id_filter) -> list["AnnotationParity"]:
    """Per-variant gnomAD / ClinVar / VEP coverage, legacy vs substrate, with the regression that gates the drop.

    For each source: how many current-mapped variants the frozen tables annotate, how many the substrate
    annotates live, and — the gate — how many the frozen tables annotate that the substrate does NOT annotate
    live. A non-zero regression means dropping the frozen tables would lose that source's serving for those
    variants, so it must reach zero (via enrichment) before the drop.
    """
    from mavedb.models.clinical_control_mapped_variant import (
        mapped_variants_clinical_controls_association_table as clinvar_assoc,
    )
    from mavedb.models.gnomad_variant_mapped_variant import (
        gnomad_variants_mapped_variants_association_table as gnomad_assoc,
    )

    def legacy_variant_ids(join_or_predicate):
        query = (
            select(distinct(MappedVariant.variant_id).label("variant_id"))
            .select_from(MappedVariant)
            .join(Variant, Variant.id == MappedVariant.variant_id)
            .where(MappedVariant.current.is_(True))
        )
        query = join_or_predicate(query)
        if id_filter is not None:
            query = query.where(id_filter)
        return query.subquery()

    def substrate_variant_count(link_model):
        query = (
            select(func.count(distinct(Variant.id)))
            .select_from(Variant)
            .join(MappingRecord, and_(MappingRecord.variant_id == Variant.id, MappingRecord.valid_to.is_(None)))
            .join(
                MappingRecordAllele,
                and_(MappingRecordAllele.mapping_record_id == MappingRecord.id, MappingRecordAllele.valid_to.is_(None)),
            )
            .join(
                link_model, and_(link_model.allele_id == MappingRecordAllele.allele_id, link_model.valid_to.is_(None))
            )
        )
        if id_filter is not None:
            query = query.where(id_filter)
        return db.scalar(query) or 0

    def parity(source, legacy_ids_subq, link_model) -> AnnotationParity:
        legacy = db.scalar(select(func.count()).select_from(legacy_ids_subq)) or 0
        regressed = (
            db.scalar(
                select(func.count())
                .select_from(legacy_ids_subq)
                .where(~_substrate_link_exists(legacy_ids_subq.c.variant_id, link_model))
            )
            or 0
        )
        return AnnotationParity(
            source=source,
            legacy_variants=legacy,
            substrate_variants=substrate_variant_count(link_model),
            regressed_variants=regressed,
        )

    return [
        parity(
            "gnomad",
            legacy_variant_ids(lambda q: q.join(gnomad_assoc, gnomad_assoc.c.mapped_variant_id == MappedVariant.id)),
            GnomadAlleleLink,
        ),
        parity(
            "clinvar",
            legacy_variant_ids(lambda q: q.join(clinvar_assoc, clinvar_assoc.c.mapped_variant_id == MappedVariant.id)),
            ClinvarAlleleLink,
        ),
        parity(
            "vep",
            legacy_variant_ids(lambda q: q.where(MappedVariant.vep_functional_consequence.is_not(None))),
            VepAlleleConsequence,
        ),
    ]


@script_environment.command(name="reconcile")
@click.option("--published-only", is_flag=True, help="Only published score sets.")
@click.option("--include-private", is_flag=True, help="Include private score sets (ignored with --published-only).")
@click.option("--collection-urn", default=None, help="Only score sets in this collection.")
@click.option("--score-set-urn", "score_set_urns", multiple=True, help="Restrict to these URNs (repeatable).")
@click.option("--json", "as_json", is_flag=True, help="Emit the report as JSON.")
async def reconcile(
    published_only: bool,
    include_private: bool,
    collection_urn: Optional[str],
    score_set_urns: tuple[str, ...],
    as_json: bool,
) -> None:
    """Safe-to-drop gate: measured-level parity + expected divergence + a static legacy-reader scan.

    Exits non-zero unless the drop is safe (every legacy-served variant is reshaped and no code outside the
    migration/export allowlist still reads the frozen tables), so it can gate a runbook or CI step.
    """
    repo_root = pathlib.Path(__file__).resolve().parents[3]
    with SessionLocal() as db:
        ids = resolve_cohort_ids(
            db,
            published_only=published_only,
            include_private=include_private,
            collection_urn=collection_urn,
            explicit_urns=score_set_urns or None,
        )
        report = compute_reconcile(db, ids, repo_root=repo_root)

    if as_json:
        click.echo(
            json.dumps(dataclasses.asdict(report) | {"safe_to_drop": report.safe_to_drop}, indent=2, default=str)
        )
    else:
        _print_reconcile(report)

    raise SystemExit(0 if report.safe_to_drop else 1)


def _print_reconcile(report: ReconcileReport) -> None:
    click.echo(f"Reconcile over {report.cohort_size or 'ALL'} score set(s)")
    click.echo()
    click.echo("Measured-level parity (frozen mapped_variants -> substrate):")
    click.echo(f"  legacy current variants        {report.legacy_current_variants:>10}")
    click.echo(f"  reshaped variants (live record) {report.reshaped_variants:>10}")
    click.echo(f"  MISSING reshape (gate)          {report.missing_reshape_variants:>10}   (must be 0 to drop)")
    click.echo()
    click.echo(f"RT fan-out sibling alleles the substrate adds (legacy had none): {report.rt_fanout_alleles}")
    click.echo()
    click.echo("Per-variant annotation parity (legacy -> substrate live; REGRESSED gates the drop):")
    click.echo(f"  {'source':<8}  {'legacy':>8}  {'substrate':>9}  {'REGRESSED':>9}")
    for parity in report.annotation_parity:
        click.echo(
            f"  {parity.source:<8}  {parity.legacy_variants:>8}  "
            f"{parity.substrate_variants:>9}  {parity.regressed_variants:>9}"
        )
    click.echo("  (a variant is 'regressed' if the frozen tables annotate it and the live substrate does not;")
    click.echo("   reshape force-closes gnomAD/ClinVar on protein alleles, so enrichment must clear this to 0)")
    click.echo()
    click.echo("Legacy-table readers outside the allowlist:")
    if report.unexpected_legacy_readers:
        for hit in report.unexpected_legacy_readers:
            click.echo(f"  ! {hit}")
    else:
        click.echo("  none")
    click.echo()
    verdict = "SAFE TO DROP" if report.safe_to_drop else "NOT SAFE TO DROP"
    click.echo(f"==> {verdict}")
    if not report.safe_to_drop:
        if not report.measured_parity_ok:
            click.echo(f"    - {report.missing_reshape_variants} legacy-served variant(s) are not reshaped.")
        if not report.annotation_parity_ok:
            regressed = ", ".join(
                f"{p.source}={p.regressed_variants}" for p in report.annotation_parity if p.regressed_variants
            )
            click.echo(f"    - annotation coverage regressed for: {regressed} (run enrichment).")
        if not report.readers_clear:
            click.echo(f"    - {len(report.unexpected_legacy_readers)} unexpected legacy reader(s) remain.")


if __name__ == "__main__":
    script_environment()
