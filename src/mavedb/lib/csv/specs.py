"""What each CSV namespace is: the columns it produces and how each is read off a row."""

from dataclasses import dataclass
from enum import StrEnum
from operator import attrgetter
from typing import Callable, Optional

from mavedb.lib.csv.namespaces import CALIBRATION_NS_PATTERN, CLINVAR_NS_PATTERN, CsvNamespace
from mavedb.lib.mave.constants import REQUIRED_SCORE_COLUMN
from mavedb.lib.validation.constants.general import hgvs_nt_column, hgvs_pro_column, hgvs_splice_column
from mavedb.models.variant import Variant


@dataclass(frozen=True)
class CsvMappedRow:
    """The mapping-derived CSV fields for one variant, resolved from its live mapping record's
    authoritative allele (post-mapped HGVS per level, assay-level HGVS, VRS digest, VEP consequence,
    ClinGen id). HGVS strings are already resolved to their canonical per-level form by the fetch layer
    (see ``mavedb.lib.score_set_variants.mapped_hgvs_by_level``) — no raw VRS payload to fall back on
    parsing here, unlike the pre-allele-graph ``MappedVariant`` substrate."""

    hgvs_g: Optional[str]
    hgvs_c: Optional[str]
    hgvs_p: Optional[str]
    hgvs_assay_level: Optional[str]
    vrs_digest: Optional[str]
    vep_functional_consequence: Optional[str]
    clingen_allele_id: Optional[str]


# One entry per namespace, so adding one is a single edit. This previously took three unrelated changes
# — column plan, row builder, fetch-layer eager loading — with nothing to catch a partial addition.


CORE_NAMESPACE = "core"
"""The identity columns every export carries, never namespaced and never opted out of."""


class DatasetColumnSelection(StrEnum):
    """Which of a ``dataset_columns`` entry's columns a namespace claims.

    Only the score columns are split, because ``score`` is the one column dataframe validation mandates,
    which makes "the required column" and "everything else" well defined.
    """

    ALL = "all"

    REQUIRED_SCORE_ONLY = "required_score_only"
    """Just ``score``, without consulting the record: dataframe validation mandates the column, so callers
    with no ``dataset_columns`` to hand (the variant CSV) still resolve it."""

    EXCEPT_REQUIRED_SCORE = "except_required_score"
    """Everything else, which only the record can enumerate."""


class RowSource(StrEnum):
    """Which per-row datum a namespace's resolvers are called with."""

    VARIANT = "variant"
    MAPPING = "mapping"
    GNOMAD = "gnomad"
    MATCH_TYPE = "match_type"
    SCORE_DATA = "score_data"
    COUNT_DATA = "count_data"

    # Parameterized namespaces are keyed by the namespace string, since one row carries a separate datum
    # for every requested release or calibration.
    CLINVAR_ENTRY = "clinvar_entry"
    ANNOTATION = "annotation"


@dataclass(frozen=True)
class CsvNamespaceSpec:
    """Everything one namespace contributes to an export."""

    source: RowSource
    """Which per-row datum the resolvers are called with."""

    resolvers: Optional[dict[str, Callable]] = None
    """Column key -> how to read it off *source*. None means the columns are not known ahead of time and
    are read by key, which is how a score set's own score and count columns work."""

    dataset_columns_key: Optional[str] = None
    """The ``dataset_columns`` entry listing this namespace's columns, for namespaces whose columns come
    from the score set rather than from this module."""

    dataset_columns: DatasetColumnSelection = DatasetColumnSelection.ALL
    """Which of that entry's columns this namespace claims."""

    emit_under: Optional[str] = None
    """Prefix these columns are emitted under, when it differs from the namespace's own name.

    Lets a namespace be a request token without becoming a column prefix, so ``scores_custom`` selects
    while its columns stay ``scores.*``. None means emit under this namespace's own name.
    """

    needs_mappings: bool = False
    """Whether the fetch layer has to load the row's mapping for this namespace to work."""
    needs_gnomad: bool = False
    """Whether the fetch layer has to load the row's gnomAD data for this namespace to work."""
    needs_score_set: bool = False
    """Whether the fetch layer has to load the row's score set for this namespace to work."""

    def columns(self, dataset_columns: dict) -> list[str]:
        """The column keys this namespace produces for a given score set."""
        if self.resolvers is not None:
            return list(self.resolvers.keys())
        if self.dataset_columns_key is None:
            return []

        if self.dataset_columns is DatasetColumnSelection.REQUIRED_SCORE_ONLY:
            return [REQUIRED_SCORE_COLUMN]

        available = [str(column) for column in dataset_columns.get(self.dataset_columns_key, [])]
        if self.dataset_columns is DatasetColumnSelection.EXCEPT_REQUIRED_SCORE:
            return [column for column in available if column != REQUIRED_SCORE_COLUMN]

        return available

    def resolver(self, column_key: str) -> Optional[Callable]:
        """How to read *column_key* off a row, or None if it is read by key rather than by resolver."""
        if self.resolvers is not None:
            return self.resolvers.get(column_key)

        # Dynamic columns are read straight out of the variant's score or count data by name.
        return _optional(lambda data: data.get(column_key))


def _target_genes(variant: Variant) -> Optional[str]:
    """The target genes of a variant's score set, joined by ``"; "`` or None if there are none."""
    if not variant.score_set:
        return None
    return "; ".join(str(tg.name) for tg in variant.score_set.target_genes if tg.name) or None


def _optional(getter: Callable) -> Callable:
    """Lift a resolver over a source that may be absent, which is how a row reports "no data here"."""
    return lambda source: getter(source) if source is not None else None


_NAMESPACE_SPECS: dict[str, CsvNamespaceSpec] = {
    CORE_NAMESPACE: CsvNamespaceSpec(
        source=RowSource.VARIANT,
        resolvers={
            "accession": attrgetter("urn"),
            hgvs_nt_column: attrgetter(hgvs_nt_column),
            hgvs_splice_column: attrgetter(hgvs_splice_column),
            hgvs_pro_column: attrgetter(hgvs_pro_column),
        },
    ),
    CsvNamespace.SCORES: CsvNamespaceSpec(
        source=RowSource.SCORE_DATA,
        dataset_columns_key="score_columns",
        dataset_columns=DatasetColumnSelection.REQUIRED_SCORE_ONLY,
    ),
    CsvNamespace.SCORES_CUSTOM: CsvNamespaceSpec(
        source=RowSource.SCORE_DATA,
        dataset_columns_key="score_columns",
        dataset_columns=DatasetColumnSelection.EXCEPT_REQUIRED_SCORE,
        emit_under=CsvNamespace.SCORES,
    ),
    CsvNamespace.COUNTS: CsvNamespaceSpec(source=RowSource.COUNT_DATA, dataset_columns_key="count_columns"),
    CsvNamespace.REFERENCE_HGVS: CsvNamespaceSpec(
        source=RowSource.MAPPING,
        resolvers={
            "post_mapped_hgvs_g": _optional(attrgetter("hgvs_g")),
            "post_mapped_hgvs_p": _optional(attrgetter("hgvs_p")),
            "post_mapped_hgvs_c": _optional(attrgetter("hgvs_c")),
            "post_mapped_hgvs_at_assay_level": _optional(attrgetter("hgvs_assay_level")),
            "post_mapped_vrs_id": _optional(attrgetter("vrs_digest")),
        },
        needs_mappings=True,
    ),
    CsvNamespace.VEP: CsvNamespaceSpec(
        source=RowSource.MAPPING,
        resolvers={"vep_functional_consequence": _optional(lambda mapping: mapping.vep_functional_consequence)},
        needs_mappings=True,
    ),
    CsvNamespace.GNOMAD: CsvNamespaceSpec(
        source=RowSource.GNOMAD,
        resolvers={
            "gnomad_af": _optional(attrgetter("allele_frequency")),
            "gnomad_ac": _optional(attrgetter("allele_count")),
            "gnomad_an": _optional(attrgetter("allele_number")),
            "gnomad_faf95_max": _optional(attrgetter("faf95_max")),
            "gnomad_faf95_max_ancestry": _optional(attrgetter("faf95_max_ancestry")),
            "gnomad_id": _optional(attrgetter("db_identifier")),
            "gnomad_version": _optional(attrgetter("db_version")),
        },
        needs_mappings=True,
        needs_gnomad=True,
    ),
    CsvNamespace.CLINGEN: CsvNamespaceSpec(
        source=RowSource.MAPPING,
        resolvers={"clingen_allele_id": _optional(lambda mapping: mapping.clingen_allele_id)},
        needs_mappings=True,
    ),
    CsvNamespace.SCORE_SET: CsvNamespaceSpec(
        source=RowSource.VARIANT,
        resolvers={
            "score_set_urn": lambda variant: variant.score_set.urn if variant.score_set else None,
            "target_gene": _target_genes,
        },
        needs_score_set=True,
    ),
    # TODO(#784): once the variant CSV emits sibling rows, report the shared `projection_group` here
    # alongside `match_type`, so a consumer can tell a projected sibling from an independent equivalent.
    CsvNamespace.RELATIONSHIP: CsvNamespaceSpec(
        source=RowSource.MATCH_TYPE,
        # Caller-supplied: only an export that widens beyond one record knows how a row relates to it.
        resolvers={"match_type": lambda match_type: match_type},
    ),
}


_CLINVAR_SPEC = CsvNamespaceSpec(
    source=RowSource.CLINVAR_ENTRY,
    resolvers={
        "clinical_significance": _optional(attrgetter("clinical_significance")),
        "clinical_review_status": _optional(attrgetter("clinical_review_status")),
    },
    needs_mappings=True,
)

_CALIBRATION_SPEC = CsvNamespaceSpec(
    source=RowSource.ANNOTATION,
    # The calibration's URN is carried in the column header, so it is not repeated as a column.
    resolvers={
        "title": _optional(attrgetter("calibration_title")),
        "research_use_only": _optional(attrgetter("research_use_only")),
        "functional_classification": _optional(attrgetter("functional_classification")),
        "acmg_criterion": _optional(attrgetter("acmg_criterion")),
        "acmg_evidence_strength": _optional(attrgetter("acmg_evidence_strength")),
        "acmg_evidence_outcome_code": _optional(attrgetter("acmg_evidence_outcome_code")),
        "pathogenicity_classification": _optional(attrgetter("pathogenicity_classification")),
    },
    needs_mappings=True,
    needs_score_set=True,
)


def namespace_spec(namespace: str) -> Optional[CsvNamespaceSpec]:
    """The descriptor for *namespace*, or None if it names nothing this module can produce."""
    if namespace in _NAMESPACE_SPECS:
        return _NAMESPACE_SPECS[namespace]
    if CLINVAR_NS_PATTERN.match(namespace):
        return _CLINVAR_SPEC
    if CALIBRATION_NS_PATTERN.match(namespace):
        return _CALIBRATION_SPEC
    return None
