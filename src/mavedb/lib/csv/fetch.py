"""Fetching the rows a CSV export renders — a whole score set, or an explicit set of variants.

Which relationships are eager-loaded follows from the requested namespaces, so a caller cannot forget one
and silently pay for an N+1.
"""

from dataclasses import dataclass
from typing import Any, Optional, Sequence

from sqlalchemy import Integer, and_, cast, func, select
from sqlalchemy.orm import Session, aliased, selectinload

from mavedb.lib.csv.namespaces import CLINVAR_DB_NAME
from mavedb.lib.csv.specs import namespace_spec
from mavedb.lib.gnomad import GNOMAD_DATA_VERSION, GNOMAD_DB_NAME
from mavedb.models.clinical_control import ClinicalControl
from mavedb.models.clinical_control_mapped_variant import mapped_variants_clinical_controls_association_table
from mavedb.models.gnomad_variant import GnomADVariant
from mavedb.models.mapped_variant import MappedVariant
from mavedb.models.score_set import ScoreSet
from mavedb.models.variant import Variant


@dataclass
class CsvFetchResult:
    variants: list[Variant]
    mappings: Optional[list[Optional[MappedVariant]]]
    gnomad_data: Optional[list[Optional[GnomADVariant]]]
    clinvar_per_variant: Optional[list[Optional[dict[str, Optional[ClinicalControl]]]]]


def fetch_variant_csv_data(
    db: Session,
    namespaced_columns: dict[str, list[str]],
    clinvar_namespaces: dict[str, str],
    *,
    score_set: Optional[ScoreSet] = None,
    variant_ids: Optional[Sequence[int]] = None,
    mapped_variant_ids: Optional[Sequence[int]] = None,
    start: Optional[int] = None,
    limit: Optional[int] = None,
) -> CsvFetchResult:
    """Fetch variant data from the database for CSV generation.

    Args:
        score_set: every variant in a score set, ordered by URN suffix. Mutually exclusive with
            *variant_ids*, which returns an explicit set in the order given. Exactly one is required.
        mapped_variant_ids: pins which mapping stands for each variant. Required from a caller that has
            already chosen one, since re-resolving on ``current`` alone could pick a different row and
            emit a variant twice — nothing in the schema stops two mappings claiming to be current.
    """
    if (score_set is None) == (variant_ids is None):
        raise ValueError("exactly one of score_set or variant_ids must be provided")

    # Driven by the namespaces' own descriptors, so a namespace cannot declare a relationship-backed
    # column and then quietly not have it loaded.
    specs = [spec for spec in (namespace_spec(ns) for ns in namespaced_columns) if spec is not None]

    need_mappings = any(spec.needs_mappings for spec in specs)
    need_gnomad = any(spec.needs_gnomad for spec in specs)
    need_score_set = any(spec.needs_score_set for spec in specs)

    variants: list[Variant] = []
    mappings: Optional[list[Optional[MappedVariant]]] = [] if need_mappings else None
    gnomad_data_list: Optional[list[Optional[GnomADVariant]]] = [] if need_gnomad else None

    select_columns: list[Any] = [Variant]
    if need_mappings:
        select_columns.append(MappedVariant)
    if need_gnomad:
        select_columns.append(GnomADVariant)

    query = select(*select_columns)

    if score_set is not None:
        query = query.where(Variant.score_set_id == score_set.id).order_by(
            cast(func.split_part(Variant.urn, "#", 2), Integer)
        )
    else:
        query = query.where(Variant.id.in_(variant_ids or []))

    if need_score_set:
        query = query.options(
            selectinload(Variant.score_set).selectinload(ScoreSet.score_calibrations),
            selectinload(Variant.score_set).selectinload(ScoreSet.target_genes),
        )

    if need_mappings:
        mapping_on_clause = (
            and_(Variant.id == MappedVariant.variant_id, MappedVariant.id.in_(mapped_variant_ids))
            if mapped_variant_ids is not None
            else and_(Variant.id == MappedVariant.variant_id, MappedVariant.current.is_(True))
        )
        query = query.join(MappedVariant, mapping_on_clause, isouter=True)

    # Version predicate belongs in the ON clause: in a WHERE it would drop any variant linked only to
    # other-version gnomAD records from the CSV entirely, instead of reporting its frequency as NA.
    if need_gnomad:
        query = query.join(
            MappedVariant.gnomad_variants.of_type(GnomADVariant).and_(
                GnomADVariant.db_name == GNOMAD_DB_NAME, GnomADVariant.db_version == GNOMAD_DATA_VERSION
            ),
            isouter=True,
        )

    if start:
        query = query.offset(start)
    if limit:
        query = query.limit(limit)

    result = db.execute(query).all()

    # Postgres does not preserve IN-list order, so restore the caller's ordering.
    if variant_ids is not None:
        position = {variant_id: index for index, variant_id in enumerate(variant_ids)}
        result = sorted(result, key=lambda row: position.get(row[0].id, len(position)))

    for row in result:
        variant = row[0]
        variants.append(variant)

        if need_mappings and mappings is not None:
            mappings.append(row[1])

        if need_gnomad and gnomad_data_list is not None:
            idx = 2 if need_mappings else 1
            gnomad_data_list.append(row[idx])

    clinvar_per_variant: Optional[list[Optional[dict[str, Optional[ClinicalControl]]]]] = None
    if clinvar_namespaces and mappings is not None:
        mv_ids = [m.id for m in mappings if m is not None]

        # One query per namespace, since each names a different release; keyed by MappedVariant id and
        # projected back onto row order below.
        clinvar_data_map: dict[str, dict[int, Optional[ClinicalControl]]] = {}
        for ns, db_version in clinvar_namespaces.items():
            mv_to_cc: dict[int, Optional[ClinicalControl]] = {}
            if mv_ids:
                aliased_cc = aliased(ClinicalControl)
                cc_query = (
                    select(
                        mapped_variants_clinical_controls_association_table.c.mapped_variant_id,
                        aliased_cc,
                    )
                    .join(
                        aliased_cc,
                        mapped_variants_clinical_controls_association_table.c.clinical_control_id == aliased_cc.id,
                    )
                    .where(
                        and_(
                            mapped_variants_clinical_controls_association_table.c.mapped_variant_id.in_(mv_ids),
                            aliased_cc.db_name == CLINVAR_DB_NAME,
                            aliased_cc.db_version == db_version,
                        )
                    )
                )

                for mv_id, cc in db.execute(cc_query).all():
                    mv_to_cc[mv_id] = cc

            clinvar_data_map[ns] = mv_to_cc

        clinvar_per_variant = [
            {
                ns: mv_to_cc.get(mapping.id) if mapping is not None and mapping.id is not None else None
                for ns, mv_to_cc in clinvar_data_map.items()
            }
            for mapping in mappings
        ]

    return CsvFetchResult(
        variants=variants,
        mappings=mappings,
        gnomad_data=gnomad_data_list,
        clinvar_per_variant=clinvar_per_variant,
    )
