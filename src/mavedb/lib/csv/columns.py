"""Column planning and row assembly for the CSV exports.

Pure functions over already-fetched objects; nothing here touches the database. What a namespace *is*
lives in ``specs``; this module only applies it.
"""

import csv
import io
from dataclasses import dataclass
from typing import Any, Iterable, Optional, Sequence

from mavedb.lib.annotation.flatten import FlatAnnotation
from mavedb.lib.csv.namespaces import (
    CALIBRATION_NS_PATTERN,
    CLINVAR_NS_PATTERN,
    parse_calibration_namespace,
    parse_clinvar_namespace,
)
from mavedb.lib.csv.specs import CORE_NAMESPACE, CsvMappedRow, RowSource, namespace_spec
from mavedb.lib.mave.utils import NA_VALUE, NULL_VALUES
from mavedb.lib.validation.constants.general import hgvs_columns
from mavedb.models.clinical_control import ClinvarControl
from mavedb.models.gnomad_variant import GnomADVariant
from mavedb.models.variant import Variant

_OUTPUT_NULL_STRINGS = frozenset(value.lower() for value in NULL_VALUES if value)
"""The null tokens this export recognises, derived from the shared vocabulary rather than restated.

The empty string is dropped because ``_is_output_null`` tests emptiness directly, and ``NA_VALUE`` folds
into ``"na"`` once lowercased.
"""


@dataclass(frozen=True)
class CsvColumnPlan:
    namespaced_columns: dict[str, list[str]]
    """Namespace -> list of column keys to emit for that namespace."""
    clinvar_namespaces: dict[str, str]
    """Requested ClinVar namespace -> the ``"MM_YYYY"`` db_version it names."""
    calibration_namespaces: dict[str, str]
    """Requested calibration namespace -> the calibration URN it names."""


def _is_output_null(value: Any) -> bool:
    """Whether *value* should be written as the NA sentinel rather than rendered.

    Shares its token vocabulary with ``lib.mave.utils.is_csv_null`` but **not its behaviour**. That one decides
    whether a value read *from* an uploaded file counts as missing, so it copes with pandas NA types and
    treats 0 specially.
    """
    text = str(value).strip().lower()
    return not text or text in _OUTPUT_NULL_STRINGS


def _value_or_na(value: Any, na_rep: str = NA_VALUE) -> str:
    """Return the string representation of *value*, or *na_rep* if the value is null-ish."""
    if _is_output_null(value):
        return na_rep
    return str(value)


def _format_column_key(namespace: str, column_key: str, namespaced: bool = False) -> str:
    """Shared key-formatting logic used by both header assembly and row assembly."""
    # Always namespaced regardless of the caller's preference: the release or calibration URN is what
    # disambiguates columns when several are requested at once.
    if CLINVAR_NS_PATTERN.match(namespace) or CALIBRATION_NS_PATTERN.match(namespace):
        return f"{namespace}.{column_key}"

    if namespace == CORE_NAMESPACE:  # core is never namespaced
        return column_key

    if namespaced:
        spec = namespace_spec(namespace)
        prefix = spec.emit_under if spec is not None and spec.emit_under is not None else namespace
        return f"{prefix}.{column_key}"

    return column_key


def plan_csv_columns(dataset_columns: dict, namespaces: list[str]) -> CsvColumnPlan:
    """Build the namespaced column map and the ClinVar and calibration namespace mappings.

    An unknown namespace is kept with no columns rather than rejected — validating the vocabulary belongs
    to the request layer.
    """
    namespaced_columns: dict[str, list[str]] = {}
    clinvar_namespaces: dict[str, str] = {}
    calibration_namespaces: dict[str, str] = {}

    for namespace in dict.fromkeys([CORE_NAMESPACE, *namespaces]):
        spec = namespace_spec(namespace)
        namespaced_columns[namespace] = spec.columns(dataset_columns) if spec else []

        db_version = parse_clinvar_namespace(namespace)
        if db_version is not None:
            clinvar_namespaces[namespace] = db_version

        calibration_urn = parse_calibration_namespace(namespace)
        if calibration_urn is not None:
            calibration_namespaces[namespace] = calibration_urn

    return CsvColumnPlan(
        namespaced_columns=namespaced_columns,
        clinvar_namespaces=clinvar_namespaces,
        calibration_namespaces=calibration_namespaces,
    )


def assemble_csv_headers(namespaced_columns: dict[str, list[str]], namespaced: bool = False) -> list[str]:
    """Build the flat column-header list from the namespace dict.

    Raises:
        ValueError: if two namespaces resolve to the same header. Un-namespaced output strips the prefix
            that would otherwise keep them apart, so requesting two namespaces that share a column name
            would emit it twice; the callers that ask for un-namespaced output request one namespace each,
            and this holds them to it rather than letting a future caller find out from a broken file.
    """
    headers = [
        _format_column_key(namespace, col, namespaced) for namespace, cols in namespaced_columns.items() for col in cols
    ]

    duplicates = sorted({header for header in headers if headers.count(header) > 1})
    if duplicates:
        raise ValueError(
            f"CSV namespaces resolve to duplicate columns: {', '.join(duplicates)}."
            f" Requested namespaces: {', '.join(namespaced_columns)}."
        )

    return headers


def variant_to_csv_row(
    variant: Variant,
    columns: dict[str, list[str]],
    mapping: Optional[CsvMappedRow] = None,
    gnomad_data: Optional[GnomADVariant] = None,
    clinvar_data_by_ns: Optional[dict[str, Optional[ClinvarControl]]] = None,
    annotations_by_ns: Optional[dict[str, Optional[FlatAnnotation]]] = None,
    match_type: Optional[str] = None,
    namespaced: bool = False,
    na_rep=NA_VALUE,
) -> dict[str, Any]:
    """Format a variant into a dict containing the keys specified in *columns*.

    Args:
        clinvar_data_by_ns, annotations_by_ns: per-row data for the parameterized namespaces, keyed by
            requested namespace. A namespace with no entry renders as *na_rep*.
    """
    row: dict[str, Any] = {}

    # Built once per row, not per namespace: a 100k-variant export with ten namespaces would otherwise
    # build this dict, and walk `variant.data` twice, a million times over.
    row_sources: dict[RowSource, Any] = {
        RowSource.VARIANT: variant,
        RowSource.MAPPING: mapping,
        RowSource.GNOMAD: gnomad_data,
        RowSource.MATCH_TYPE: match_type,
        RowSource.SCORE_DATA: (variant.data or {}).get("score_data"),
        RowSource.COUNT_DATA: (variant.data or {}).get("count_data"),
    }

    for namespace, column_keys in columns.items():
        spec = namespace_spec(namespace)
        if spec is None:
            continue

        source: Any
        # Only the parameterized namespaces carry a distinct datum per namespace.
        if spec.source is RowSource.CLINVAR_ENTRY:
            source = (clinvar_data_by_ns or {}).get(namespace)
        elif spec.source is RowSource.ANNOTATION:
            source = (annotations_by_ns or {}).get(namespace)
        else:
            source = row_sources[spec.source]

        for column_key in column_keys:
            resolver = spec.resolver(column_key)
            if resolver is None:
                raise ValueError(f"unrecognized {namespace} column: {column_key}")

            row[_format_column_key(namespace, column_key, namespaced=namespaced)] = _value_or_na(
                resolver(source), na_rep
            )

    return row


def variants_to_csv_rows(
    variants: Sequence[Variant],
    columns: dict[str, list[str]],
    mappings: Optional[Sequence[Optional[CsvMappedRow]]] = None,
    gnomad_data: Optional[Sequence[Optional[GnomADVariant]]] = None,
    clinvar_data_by_ns: Optional[Sequence[Optional[dict[str, Optional[ClinvarControl]]]]] = None,
    annotations_by_ns: Optional[Sequence[Optional[dict[str, Optional[FlatAnnotation]]]]] = None,
    match_types: Optional[Sequence[Optional[str]]] = None,
    namespaced: bool = False,
    na_rep=NA_VALUE,
) -> Iterable[dict[str, Any]]:
    """Format each variant into a dictionary row containing the keys specified in *columns*."""
    n = len(variants)
    _mappings: Sequence[Optional[CsvMappedRow]] = mappings if mappings is not None else [None] * n
    _gnomad: Sequence[Optional[GnomADVariant]] = gnomad_data if gnomad_data is not None else [None] * n
    _clinvar: Sequence[Optional[dict[str, Optional[ClinvarControl]]]] = (
        clinvar_data_by_ns if clinvar_data_by_ns is not None else [None] * n
    )
    _annotations: Sequence[Optional[dict[str, Optional[FlatAnnotation]]]] = (
        annotations_by_ns if annotations_by_ns is not None else [None] * n
    )
    _match_types: Sequence[Optional[str]] = match_types if match_types is not None else [None] * n
    return map(
        lambda t: variant_to_csv_row(
            t[0],
            columns,
            mapping=t[1],
            gnomad_data=t[2],
            clinvar_data_by_ns=t[3],
            annotations_by_ns=t[4],
            match_type=t[5],
            namespaced=namespaced,
            na_rep=na_rep,
        ),
        zip(variants, _mappings, _gnomad, _clinvar, _annotations, _match_types),
    )


def rows_to_csv(rows: Iterable[dict[str, Any]], columns: list[str]) -> str:
    """Serialize *rows* to a CSV string headed by *columns*."""
    stream = io.StringIO()
    writer = csv.DictWriter(stream, fieldnames=columns, quoting=csv.QUOTE_MINIMAL)
    writer.writeheader()
    writer.writerows(rows)
    return stream.getvalue()


def drop_unused_hgvs_columns(
    rows_data: Iterable[dict[str, Any]], columns: list[str]
) -> tuple[list[dict[str, Any]], list[str]]:
    """Omit the HGVS coordinate columns this score set does not use.

    A protein-only score set never has ``hgvs_nt``; that is a property of the score set, not sparse data.
    Limited to the three core HGVS columns on purpose — dropping data-dependent columns elsewhere would
    make a download's shape vary with its contents.

    Assumes the "core" namespace is present, which ``plan_csv_columns`` guarantees.
    """
    rows_data = list(rows_data)
    columns_to_remove = []

    for col in hgvs_columns:
        if all(_is_output_null(row[col]) for row in rows_data):
            columns_to_remove.append(col)
            for row in rows_data:
                row.pop(col, None)

    columns = [col for col in columns if col not in columns_to_remove]
    return rows_data, columns
