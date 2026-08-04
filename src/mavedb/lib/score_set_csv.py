import csv
import io
from dataclasses import dataclass
from operator import attrgetter
from typing import Any, Callable, Iterable, List, Optional, Sequence

from sqlalchemy import Integer, and_, cast, func, or_, select
from sqlalchemy.orm import Session, aliased

from mavedb.lib.clinvar.constants import CLINVAR_NS_PATTERN
from mavedb.lib.clinvar.utils import parse_clinvar_namespace
from mavedb.lib.mave.constants import REQUIRED_SCORE_COLUMN
from mavedb.lib.mave.utils import NA_VALUE, is_csv_output_null
from mavedb.lib.validation.utilities import is_null as validate_is_null
from mavedb.lib.variants import get_digest_from_post_mapped, get_hgvs_from_post_mapped, is_hgvs_g, is_hgvs_p
from mavedb.models.clinical_control import ClinicalControl
from mavedb.models.clinical_control_mapped_variant import mapped_variants_clinical_controls_association_table
from mavedb.models.gnomad_variant import GnomADVariant
from mavedb.models.mapped_variant import MappedVariant
from mavedb.models.score_set import ScoreSet
from mavedb.models.variant import Variant


@dataclass(frozen=True)
class CsvColumnPlan:
    namespaced_columns: dict[str, list[str]]
    clinvar_namespaces: dict[str, str]


@dataclass
class CsvFetchResult:
    variants: list[Variant]
    mappings: Optional[list[Optional[MappedVariant]]]
    gnomad_data: Optional[list[Optional[GnomADVariant]]]
    clinvar_per_variant: Optional[list[Optional[dict[str, Optional[ClinicalControl]]]]]


# ---------------------------------------------------------------------------
# Column-key resolvers for fixed-column namespaces
# ---------------------------------------------------------------------------

_CORE_RESOLVERS: dict[str, Callable] = {
    "hgvs_nt": attrgetter("hgvs_nt"),
    "hgvs_pro": attrgetter("hgvs_pro"),
    "hgvs_splice": attrgetter("hgvs_splice"),
    "accession": attrgetter("urn"),
}

_VEP_RESOLVERS: dict[str, Callable] = {
    "vep_functional_consequence": lambda mapping: mapping.vep_functional_consequence if mapping else None,
}

_GNOMAD_RESOLVERS: dict[str, Callable] = {
    "gnomad_af": lambda gnomad_data: gnomad_data.allele_frequency if gnomad_data else None,
}

_CLINGEN_RESOLVERS: dict[str, Callable] = {
    "clingen_allele_id": lambda mapping: mapping.clingen_allele_id if mapping else None,
}

_CLINVAR_RESOLVERS: dict[str, Callable] = {
    "clinical_significance": attrgetter("clinical_significance"),
    "clinical_review_status": attrgetter("clinical_review_status"),
}


def _value_or_na(value: Any, na_rep: str = NA_VALUE) -> str:
    """Return the string representation of *value*, or *na_rep* if the value is None."""
    if is_csv_output_null(value):
        return na_rep
    return str(value)


def _format_column_key(namespace: str, column_key: str, namespaced: bool = False) -> str:
    """Shared key-formatting logic used by both header assembly and row assembly."""
    # ClinVar columns are always namespaced to differentiate versions, even if the user has requested un-namespaced output.
    if CLINVAR_NS_PATTERN.match(namespace):
        return f"{namespace}.{column_key}"

    # The "core" namespace is always un-namespaced, even if the user has requested namespaced output.
    if namespace == "core":
        return column_key

    # All other namespaces are namespaced if the user has requested namespaced output, and un-namespaced otherwise.
    if namespaced:
        return f"{namespace}.{column_key}"

    return column_key


def _custom_columns(dataset_columns: dict, col_name: str) -> list[str]:
    return [col for col in [str(x) for x in list(dataset_columns.get(col_name, []))]]


# ---------------------------------------------------------------------------
# Pure functions
# ---------------------------------------------------------------------------


def plan_csv_columns(
    dataset_columns: dict,
    namespaces: list[str],
    *,
    include_custom_columns: bool = True,
    include_post_mapped_hgvs: bool = False,
) -> CsvColumnPlan:
    """Build the namespaced column map and ClinVar namespace mapping."""
    namespaced_score_set_columns: dict[str, list[str]] = {
        "core": ["accession", "hgvs_nt", "hgvs_splice", "hgvs_pro"],
        "mavedb": [],
    }

    if include_post_mapped_hgvs:
        namespaced_score_set_columns["mavedb"].append("post_mapped_hgvs_g")
        namespaced_score_set_columns["mavedb"].append("post_mapped_hgvs_p")
        namespaced_score_set_columns["mavedb"].append("post_mapped_hgvs_c")
        namespaced_score_set_columns["mavedb"].append("post_mapped_hgvs_at_assay_level")
        namespaced_score_set_columns["mavedb"].append("post_mapped_vrs_digest")

    for namespace in namespaces:
        namespaced_score_set_columns[namespace] = []

    if "scores" in namespaced_score_set_columns:
        if include_custom_columns:
            # the required score column is transitively included via the _custom_columns function.
            namespaced_score_set_columns["scores"] = _custom_columns(dataset_columns, "score_columns")
        else:
            namespaced_score_set_columns["scores"] = [REQUIRED_SCORE_COLUMN]
    if "counts" in namespaced_score_set_columns:
        if include_custom_columns:
            namespaced_score_set_columns["counts"] = _custom_columns(dataset_columns, "count_columns")
    if "vep" in namespaced_score_set_columns:
        namespaced_score_set_columns["vep"].append("vep_functional_consequence")
    if "gnomad" in namespaced_score_set_columns:
        namespaced_score_set_columns["gnomad"].append("gnomad_af")
    if "clingen" in namespaced_score_set_columns:
        namespaced_score_set_columns["clingen"].append("clingen_allele_id")

    clinvar_namespaces: dict[str, str] = {}
    for ns in namespaces:
        db_version = parse_clinvar_namespace(ns)
        if db_version is not None:
            clinvar_namespaces[ns] = db_version
            namespaced_score_set_columns[ns] = ["clinical_significance", "clinical_review_status"]

    return CsvColumnPlan(
        namespaced_columns=namespaced_score_set_columns,
        clinvar_namespaces=clinvar_namespaces,
    )


def assemble_csv_headers(namespaced_columns: dict[str, list[str]], namespaced: bool = False) -> list[str]:
    """Build the flat column-header list from the namespace dict."""
    return [
        _format_column_key(namespace, col, namespaced) for namespace, cols in namespaced_columns.items() for col in cols
    ]


# ---------------------------------------------------------------------------
# Row assembly
# ---------------------------------------------------------------------------


def variant_to_csv_row(
    variant: Variant,
    columns: dict[str, list[str]],
    mapping: Optional[MappedVariant] = None,
    gnomad_data: Optional[GnomADVariant] = None,
    clinvar_data_by_ns: Optional[dict[str, Optional[ClinicalControl]]] = None,
    namespaced: bool = False,
    na_rep=NA_VALUE,
) -> dict[str, Any]:
    """Format a variant into a dict containing the keys specified in *columns*."""
    row: dict[str, Any] = {}

    for column_key in columns.get("core", []):
        resolver = _CORE_RESOLVERS.get(column_key)
        if resolver is None:
            raise ValueError(f"unrecognized core column: {column_key}")

        value = str(resolver(variant))
        row[column_key] = _value_or_na(value, na_rep)

    for column_key in columns.get("mavedb", []):
        if column_key == "post_mapped_hgvs_g":
            value = str(mapping.hgvs_g) if mapping and mapping.hgvs_g else na_rep
            if value == na_rep:
                fallback_hgvs = (
                    get_hgvs_from_post_mapped(mapping.post_mapped) if mapping and mapping.post_mapped else None
                )
                if fallback_hgvs is not None and is_hgvs_g(fallback_hgvs):
                    value = fallback_hgvs
                else:
                    value = na_rep

        elif column_key == "post_mapped_hgvs_p":
            value = str(mapping.hgvs_p) if mapping and mapping.hgvs_p else na_rep
            if value == na_rep:
                fallback_hgvs = (
                    get_hgvs_from_post_mapped(mapping.post_mapped) if mapping and mapping.post_mapped else None
                )
                if fallback_hgvs is not None and is_hgvs_p(fallback_hgvs):
                    value = fallback_hgvs
                else:
                    value = na_rep

        elif column_key == "post_mapped_hgvs_c":
            value = str(mapping.hgvs_c) if mapping and mapping.hgvs_c else na_rep
        elif column_key == "post_mapped_hgvs_at_assay_level":
            value = str(mapping.hgvs_assay_level) if mapping and mapping.hgvs_assay_level else na_rep
        elif column_key == "post_mapped_vrs_digest":
            digest = get_digest_from_post_mapped(mapping.post_mapped) if mapping and mapping.post_mapped else None
            value = digest if digest is not None else na_rep
        else:
            raise ValueError(f"unrecognized mavedb column: {column_key}")

        row[_format_column_key("mavedb", column_key, namespaced=namespaced)] = _value_or_na(value, na_rep)

    for ns in ("vep", "gnomad", "clingen"):
        resolvers = {"vep": _VEP_RESOLVERS, "gnomad": _GNOMAD_RESOLVERS, "clingen": _CLINGEN_RESOLVERS}[ns]
        source = {"vep": mapping, "gnomad": gnomad_data, "clingen": mapping}[ns]
        for column_key in columns.get(ns, []):
            resolver = resolvers.get(column_key)
            if resolver is None:
                raise ValueError(f"unrecognized {ns} column: {column_key}")
            value = resolver(source)
            row[_format_column_key(ns, column_key, namespaced=namespaced)] = _value_or_na(value, na_rep)

    for data_ns in ("scores", "counts"):
        data_key = f"{data_ns[:-1]}_data"
        parent = variant.data.get(data_key) if variant.data else None
        for column_key in columns.get(data_ns, []):
            value = str(parent.get(column_key)) if parent else na_rep
            row[_format_column_key(data_ns, column_key, namespaced=namespaced)] = _value_or_na(value, na_rep)

    for namespace_key, namespace_cols in columns.items():
        if not CLINVAR_NS_PATTERN.match(namespace_key):
            continue
        clinvar_entry = (clinvar_data_by_ns or {}).get(namespace_key)
        for column_key in namespace_cols:
            resolver = _CLINVAR_RESOLVERS.get(column_key)
            if resolver is None:
                raise ValueError(f"unrecognized clinvar column: {column_key}")
            value = str(resolver(clinvar_entry)) if clinvar_entry else na_rep
            row[_format_column_key(namespace_key, column_key, namespaced=namespaced)] = _value_or_na(value, na_rep)

    return row


def variants_to_csv_rows(
    variants: Sequence[Variant],
    columns: dict[str, list[str]],
    mappings: Optional[Sequence[Optional[MappedVariant]]] = None,
    gnomad_data: Optional[Sequence[Optional[GnomADVariant]]] = None,
    clinvar_data_by_ns: Optional[Sequence[Optional[dict[str, Optional[ClinicalControl]]]]] = None,
    namespaced: bool = False,
    na_rep=NA_VALUE,
) -> Iterable[dict[str, Any]]:
    """Format each variant into a dictionary row containing the keys specified in *columns*."""
    n = len(variants)
    _mappings: Sequence[Optional[MappedVariant]] = mappings if mappings is not None else [None] * n
    _gnomad: Sequence[Optional[GnomADVariant]] = gnomad_data if gnomad_data is not None else [None] * n
    _clinvar: Sequence[Optional[dict[str, Optional[ClinicalControl]]]] = (
        clinvar_data_by_ns if clinvar_data_by_ns is not None else [None] * n
    )
    return map(
        lambda t: variant_to_csv_row(
            t[0],
            columns,
            mapping=t[1],
            gnomad_data=t[2],
            clinvar_data_by_ns=t[3],
            namespaced=namespaced,
            na_rep=na_rep,
        ),
        zip(variants, _mappings, _gnomad, _clinvar),
    )


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def drop_na_columns_from_csv_file_rows(
    rows_data: Iterable[dict[str, Any]], columns: list[str]
) -> tuple[list[dict[str, Any]], list[str]]:
    """Process rows_data for downloadable CSV by removing empty columns."""
    rows_data = list(rows_data)
    columns_to_check = ["hgvs_nt", "hgvs_splice", "hgvs_pro"]
    columns_to_remove = []

    for col in columns_to_check:
        if all(validate_is_null(row[col]) for row in rows_data):
            columns_to_remove.append(col)
            for row in rows_data:
                row.pop(col, None)

    columns = [col for col in columns if col not in columns_to_remove]
    return rows_data, columns


# ---------------------------------------------------------------------------
# DB-bound fetching
# ---------------------------------------------------------------------------


def fetch_variant_csv_data(
    db: Session,
    score_set: ScoreSet,
    namespaced_columns: dict[str, list[str]],
    clinvar_namespaces: dict[str, str],
    *,
    include_post_mapped_hgvs: bool = False,
    start: Optional[int] = None,
    limit: Optional[int] = None,
) -> CsvFetchResult:
    """Fetch variant data from the database for CSV generation."""
    namespaces = list(namespaced_columns.keys())

    need_mappings = (
        include_post_mapped_hgvs
        or "clingen" in namespaces
        or "vep" in namespaces
        or "gnomad" in namespaces
        or bool(clinvar_namespaces)
    )
    need_gnomad = "gnomad" in namespaces

    variants: list[Variant] = []
    mappings: Optional[list[Optional[MappedVariant]]] = [] if need_mappings else None
    gnomad_data_list: Optional[list[Optional[GnomADVariant]]] = [] if need_gnomad else None

    select_columns: list[Any] = [Variant]
    if need_mappings:
        select_columns.append(MappedVariant)
    if need_gnomad:
        select_columns.append(GnomADVariant)

    query = (
        select(*select_columns)
        .where(Variant.score_set_id == score_set.id)
        .order_by(cast(func.split_part(Variant.urn, "#", 2), Integer))
    )

    if need_mappings:
        query = query.join(
            MappedVariant,
            and_(Variant.id == MappedVariant.variant_id, MappedVariant.current.is_(True)),
            isouter=True,
        )

    if need_gnomad:
        query = query.join(
            MappedVariant.gnomad_variants.of_type(GnomADVariant),
            isouter=True,
        ).where(
            or_(
                and_(GnomADVariant.db_name == "gnomAD", GnomADVariant.db_version == "v4.1"),
                GnomADVariant.id.is_(None),
            )
        )

    if start:
        query = query.offset(start)
    if limit:
        query = query.limit(limit)

    result = db.execute(query).all()

    for row in result:
        variant = row[0]
        variants.append(variant)

        if need_mappings and mappings is not None:
            mappings.append(row[1])

        if need_gnomad and gnomad_data_list is not None:
            idx = 2 if need_mappings else 1
            gnomad_data_list.append(row[idx])

    clinvar_data_map: dict[str, dict[int, Optional[ClinicalControl]]] = {}
    if clinvar_namespaces and mappings is not None:
        mv_ids = [m.id for m in mappings if m is not None]
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
                            aliased_cc.db_name == "ClinVar",
                            aliased_cc.db_version == db_version,
                        )
                    )
                )
                for mv_id, cc in db.execute(cc_query).all():
                    mv_to_cc[mv_id] = cc
            clinvar_data_map[ns] = mv_to_cc

    clinvar_per_variant: Optional[list[Optional[dict[str, Optional[ClinicalControl]]]]] = None
    if clinvar_namespaces and mappings is not None:
        clinvar_per_variant = []
        for mapping in mappings:
            row_clinvar: dict[str, Optional[ClinicalControl]] = {}
            for ns, mv_to_cc in clinvar_data_map.items():
                if mapping is not None and mapping.id is not None:
                    row_clinvar[ns] = mv_to_cc.get(mapping.id)
                else:
                    row_clinvar[ns] = None
            clinvar_per_variant.append(row_clinvar)

    return CsvFetchResult(
        variants=variants,
        mappings=mappings,
        gnomad_data=gnomad_data_list,
        clinvar_per_variant=clinvar_per_variant,
    )


# ---------------------------------------------------------------------------
# Public composer
# ---------------------------------------------------------------------------


def get_score_set_variants_as_csv(
    db: Session,
    score_set: ScoreSet,
    namespaces: List[str],
    namespaced: bool = False,
    start: Optional[int] = None,
    limit: Optional[int] = None,
    drop_na_columns: Optional[bool] = None,
    include_custom_columns: Optional[bool] = True,
    include_post_mapped_hgvs: Optional[bool] = False,
) -> str:
    """Get the variant data from a score set as a CSV string."""
    assert type(score_set.dataset_columns) is dict

    plan = plan_csv_columns(
        score_set.dataset_columns,
        namespaces,
        include_custom_columns=bool(include_custom_columns),
        include_post_mapped_hgvs=bool(include_post_mapped_hgvs),
    )

    fetched = fetch_variant_csv_data(
        db,
        score_set,
        plan.namespaced_columns,
        plan.clinvar_namespaces,
        include_post_mapped_hgvs=bool(include_post_mapped_hgvs),
        start=start,
        limit=limit,
    )

    rows_data = variants_to_csv_rows(
        fetched.variants,
        columns=plan.namespaced_columns,
        namespaced=namespaced,
        mappings=fetched.mappings,
        gnomad_data=fetched.gnomad_data,
        clinvar_data_by_ns=fetched.clinvar_per_variant,
    )

    rows_columns = assemble_csv_headers(plan.namespaced_columns, namespaced=namespaced)

    if drop_na_columns:
        rows_data, rows_columns = drop_na_columns_from_csv_file_rows(rows_data, rows_columns)

    stream = io.StringIO()
    writer = csv.DictWriter(stream, fieldnames=rows_columns, quoting=csv.QUOTE_MINIMAL)
    writer.writeheader()
    writer.writerows(rows_data)
    return stream.getvalue()
