"""Fetching the rows a CSV export renders — a whole score set, or an explicit set of variants.

Which relationships are eager-loaded follows from the requested namespaces, so a caller cannot forget one
and silently pay for an N+1.
"""

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Optional, Sequence

from sqlalchemy import Select, and_, select
from sqlalchemy.orm import Session, selectinload

from mavedb.lib.csv.namespaces import CLINVAR_DB_NAME
from mavedb.lib.csv.specs import CsvMappedRow, namespace_spec
from mavedb.lib.gnomad import GNOMAD_DB_NAME
from mavedb.models.allele import Allele
from mavedb.models.clinical_control import ClinvarControl
from mavedb.models.clinvar_allele_link import ClinvarAlleleLink
from mavedb.models.gnomad_allele_link import GnomadAlleleLink
from mavedb.models.gnomad_variant import GnomADVariant
from mavedb.models.mapping_record import MappingRecord
from mavedb.models.mapping_record_allele import MappingRecordAllele
from mavedb.models.projection_group import ProjectionGroup
from mavedb.models.score_set import ScoreSet
from mavedb.models.variant import Variant
from mavedb.models.vep_allele_consequence import VepAlleleConsequence

# CSV rows come out in variant-number order (the integer after '#'); id breaks ties stably.
_VARIANT_NUMBER_ORDER = (Variant.variant_number, Variant.id)


@dataclass
class CsvFetchResult:
    variants: list[Variant]
    mappings: Optional[list[Optional[CsvMappedRow]]]
    gnomad_data: Optional[list[Optional[GnomADVariant]]]
    clinvar_per_variant: Optional[list[Optional[dict[str, Optional[ClinvarControl]]]]]


def _scope_query(
    query: Select[Any],
    *,
    score_set_id: Optional[int],
    variant_ids: Optional[Sequence[int]],
    start: Optional[int] = None,
    limit: Optional[int] = None,
) -> Select[Any]:
    """Narrow *query* to one score set (ordered by variant number, optionally one ``start``/``limit`` page of
    it) or an explicit variant set."""
    if score_set_id is None:
        return query.where(Variant.id.in_(variant_ids or []))

    if start is None and limit is None:
        return query.where(Variant.score_set_id == score_set_id).order_by(*_VARIANT_NUMBER_ORDER)

    # Joining to the page's ids, with no score set filter of its own, leaves the outer query nothing to do
    # but fetch those rows. Filtered by score set, the planner could serve its ORDER BY by walking the whole
    # score set in index order and reading every row, skipped ones included (#901).
    page = _page_variant_ids(score_set_id, start, limit).subquery("page")
    return query.join(page, page.c.id == Variant.id).order_by(*_VARIANT_NUMBER_ORDER)


def _restore_variant_id_order(rows: list[Any], variant_ids: Sequence[int], key) -> list[Any]:
    """Postgres does not preserve IN-list order, so restore the caller's ordering by variant id."""
    position = {variant_id: index for index, variant_id in enumerate(variant_ids)}
    return sorted(rows, key=lambda row: position.get(key(row), len(position)))


def _page_variant_ids(score_set_id: int, start: Optional[int], limit: Optional[int]) -> Select[Any]:
    """The ids of one ``start``/``limit`` page of a score set's variants. A ``None`` bound is left off.

    Selecting only ids lets the variant-number index skip the rows before ``start`` without joining them
    (#901).
    """
    query = select(Variant.id).where(Variant.score_set_id == score_set_id).order_by(*_VARIANT_NUMBER_ORDER)
    if start is not None:
        query = query.offset(start)
    if limit is not None:
        query = query.limit(limit)
    return query


def _substrate_query(
    *,
    score_set_id: Optional[int],
    variant_ids: Optional[Sequence[int]],
    start: Optional[int],
    limit: Optional[int],
    as_of: Optional[datetime],
) -> Select[Any]:
    """The base per-variant query: each variant LEFT-joined to its live mapping record, its authoritative
    (measured) allele (digest / ClinGen id / VEP consequence), and that allele's measured projection group
    (the mapped HGVS at every level). LEFT joins with ``live_at`` in the ON clause keep unmapped variants
    (with null mapped fields); a WHERE would collapse them out. The gnomAD/ClinVar dimensions are resolved by
    separate fetches and stitched on by id.
    """
    query = (
        select(
            Variant,
            MappingRecord.id.label("mapping_record_id"),
            MappingRecord.assay_level,
            MappingRecord.hgvs_assay_level,
            Allele.id.label("allele_id"),
            Allele.vrs_digest,
            Allele.clingen_allele_id,
            VepAlleleConsequence.functional_consequence,
            ProjectionGroup.hgvs_c,
            ProjectionGroup.hgvs_g,
            ProjectionGroup.hgvs_p,
        )
        .outerjoin(MappingRecord, and_(MappingRecord.variant_id == Variant.id, MappingRecord.live_at(as_of)))
        .outerjoin(
            MappingRecordAllele,
            and_(
                MappingRecordAllele.mapping_record_id == MappingRecord.id,
                MappingRecordAllele.is_authoritative.is_(True),
                MappingRecordAllele.live_at(as_of),
            ),
        )
        .outerjoin(Allele, Allele.id == MappingRecordAllele.allele_id)
        .outerjoin(
            VepAlleleConsequence,
            and_(VepAlleleConsequence.allele_id == Allele.id, VepAlleleConsequence.live_at(as_of)),
        )
        .outerjoin(
            ProjectionGroup,
            and_(ProjectionGroup.id == MappingRecordAllele.projection_group, ProjectionGroup.live_at(as_of)),
        )
    )
    return _scope_query(query, score_set_id=score_set_id, variant_ids=variant_ids, start=start, limit=limit)


def _mapped_row(row: Any) -> CsvMappedRow:
    """Assemble one variant's :class:`CsvMappedRow` from a substrate-query row: the measured projection
    group's HGVS at each level plus the assay-level string, VRS id, VEP consequence, and ClinGen id off the
    authoritative allele."""
    return CsvMappedRow(
        hgvs_g=row.hgvs_g,
        hgvs_c=row.hgvs_c,
        hgvs_p=row.hgvs_p,
        hgvs_assay_level=row.hgvs_assay_level,
        vrs_digest=row.vrs_digest,
        vep_functional_consequence=row.functional_consequence,
        clingen_allele_id=row.clingen_allele_id,
    )


def _gnomad_by_allele(db: Session, allele_ids: list[int], *, as_of: Optional[datetime]) -> dict[int, GnomADVariant]:
    """The live gnomAD frequency record for each of *allele_ids*, keyed by allele id.

    Liveness alone selects the release; there is deliberately no ``db_version`` predicate. The link is
    allele-keyed and single-live (``uq_gnomad_allele_links_live``), and ingestion supersedes on that key,
    so a release bump retires the prior link rather than accumulating one per version — whatever is live
    *is* the release we hold for that allele. Filtering on the currently-served constant on top of that
    was wrong in two ways:

    - The linker only visits alleles present in the release it ingests, so an allele absent from the new
      release keeps its prior-release link live. Mixed versions are the steady state, not a transient, and
      the constant turned every straggler's frequency into a permanent NA.
    - Under ``as_of`` the constant is *today's* release, so reconstructing a past instant found the
      then-live link and then discarded it — a silent dropout rather than a reconstruction.

    ``db_name`` is still constrained: that is source identity, not a version axis. The release each row
    came from is reported in the ``gnomad.gnomad_version`` column, so the output stays self-describing.
    """
    if not allele_ids:
        return {}

    rows = db.execute(
        select(GnomadAlleleLink.allele_id, GnomADVariant)
        .join(GnomADVariant, GnomADVariant.id == GnomadAlleleLink.gnomad_variant_id)
        .where(GnomadAlleleLink.allele_id.in_(allele_ids))
        .where(GnomadAlleleLink.live_at(as_of))
        .where(GnomADVariant.db_name == GNOMAD_DB_NAME)
    )
    return {allele_id: gnomad_variant for allele_id, gnomad_variant in rows.tuples()}


def _clinvar_by_allele(
    db: Session, allele_ids: list[int], clinvar_namespaces: dict[str, str], *, as_of: Optional[datetime]
) -> dict[str, dict[int, ClinvarControl]]:
    """For each requested ClinVar namespace, the live control at that release for each of *allele_ids*
    (``allele id -> control``). ClinVar links are multi-live (one per release), so each namespace's query
    is narrowed to its own ``db_version``."""
    by_namespace: dict[str, dict[int, ClinvarControl]] = {}
    for ns, db_version in clinvar_namespaces.items():
        per_allele: dict[int, ClinvarControl] = {}
        if allele_ids:
            rows = db.execute(
                select(ClinvarAlleleLink.allele_id, ClinvarControl)
                .join(ClinvarControl, ClinvarControl.id == ClinvarAlleleLink.clinvar_control_id)
                .where(ClinvarAlleleLink.allele_id.in_(allele_ids))
                .where(ClinvarAlleleLink.live_at(as_of))
                .where(ClinvarControl.db_name == CLINVAR_DB_NAME, ClinvarControl.db_version == db_version)
            )
            per_allele = {allele_id: control for allele_id, control in rows.tuples()}

        by_namespace[ns] = per_allele

    return by_namespace


def fetch_variant_csv_data(
    db: Session,
    namespaced_columns: dict[str, list[str]],
    clinvar_namespaces: dict[str, str],
    *,
    score_set: Optional[ScoreSet] = None,
    variant_ids: Optional[Sequence[int]] = None,
    start: Optional[int] = None,
    limit: Optional[int] = None,
    as_of: Optional[datetime] = None,
) -> CsvFetchResult:
    """Fetch variant data from the database for CSV generation.

    Args:
        score_set: every variant in a score set, ordered by URN suffix. Mutually exclusive with
            *variant_ids*, which returns an explicit set in the order given. Exactly one is required.
        start: rows of the score set to skip. Not valid with *variant_ids*.
        limit: the most rows of the score set to return. Not valid with *variant_ids*.
        as_of: reconstruct the mapping-derived namespaces (reference HGVS, VEP, gnomAD, ClinVar) as they
            stood at this instant, over the variant's immutable submitted HGVS/scores/counts. Defaults to
            currently-live rows. The deterministic replacement for the old ``current``-flag pinning: a
            live mapping record and its authoritative allele link are unique by construction, so there is
            no race to guard against by pinning specific row ids.
    """
    if (score_set is None) == (variant_ids is None):
        raise ValueError("exactly one of score_set or variant_ids must be provided")
    if variant_ids is not None and (start is not None or limit is not None):
        raise ValueError("start and limit page a score set, not an explicit variant set")

    # Driven by the namespaces' own descriptors, so a namespace cannot declare a relationship-backed
    # column and then quietly not have it loaded.
    specs = [spec for spec in (namespace_spec(ns) for ns in namespaced_columns) if spec is not None]

    need_mappings = any(spec.needs_mappings for spec in specs)
    need_gnomad = any(spec.needs_gnomad for spec in specs)
    need_score_set = any(spec.needs_score_set for spec in specs)

    score_set_id = score_set.id if score_set is not None else None
    score_set_options = (
        [
            selectinload(Variant.score_set).selectinload(ScoreSet.score_calibrations),
            selectinload(Variant.score_set).selectinload(ScoreSet.target_genes),
        ]
        if need_score_set
        else []
    )

    if not need_mappings:
        # Fast path: scores/counts (and bare core/score-set identity) are a straight paginated scan,
        # untouched by as_of (it only reconstructs the — absent here — mapping-derived namespaces).
        query = _scope_query(
            select(Variant), score_set_id=score_set_id, variant_ids=variant_ids, start=start, limit=limit
        ).options(*score_set_options)
        result = list(db.scalars(query).all())
        if variant_ids is not None:
            result = _restore_variant_id_order(result, variant_ids, key=lambda v: v.id)
        return CsvFetchResult(result, None, None, None)

    query = _substrate_query(
        score_set_id=score_set_id, variant_ids=variant_ids, start=start, limit=limit, as_of=as_of
    ).options(*score_set_options)
    rows = list(db.execute(query).all())
    if variant_ids is not None:
        rows = _restore_variant_id_order(rows, variant_ids, key=lambda row: row.Variant.id)

    # Protein slot: the standalone protein subquery over the whole set, stitched back by record id.
    # The authoritative allele ids on this page — the join key for the batch gnomAD/ClinVar fetches.
    allele_ids = [row.allele_id for row in rows if row.allele_id is not None]
    gnomad_by_allele = _gnomad_by_allele(db, allele_ids, as_of=as_of) if need_gnomad else {}
    clinvar_by_allele = _clinvar_by_allele(db, allele_ids, clinvar_namespaces, as_of=as_of)

    variants: list[Variant] = []
    mappings: list[Optional[CsvMappedRow]] = []
    gnomad_data: Optional[list[Optional[GnomADVariant]]] = [] if need_gnomad else None
    clinvar_per_variant: Optional[list[Optional[dict[str, Optional[ClinvarControl]]]]] = (
        [] if clinvar_namespaces else None
    )
    for row in rows:
        variants.append(row.Variant)
        allele_id = row.allele_id
        mappings.append(_mapped_row(row) if row.mapping_record_id is not None else None)
        if gnomad_data is not None:
            gnomad_data.append(gnomad_by_allele.get(allele_id) if allele_id is not None else None)
        if clinvar_per_variant is not None:
            clinvar_per_variant.append(
                {
                    ns: (clinvar_by_allele[ns].get(allele_id) if allele_id is not None else None)
                    for ns in clinvar_namespaces
                }
            )

    return CsvFetchResult(
        variants=variants,
        mappings=mappings,
        gnomad_data=gnomad_data,
        clinvar_per_variant=clinvar_per_variant,
    )
