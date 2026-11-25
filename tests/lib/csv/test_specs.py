"""The descriptors are what keep planning, row assembly and fetching from drifting apart."""

import pytest

from mavedb.lib.csv.columns import CsvPlan, csv_records
from mavedb.lib.csv.namespaces import CsvNamespace
from mavedb.lib.csv.sources import SCOPE_SOURCES, SOURCE_LOADERS
from mavedb.lib.csv.specs import CORE_NAMESPACE, RowSource, namespace_spec
from tests.helpers.constants import VALID_CALIBRATION_URN

CALIBRATION_NS = f"calibration.{VALID_CALIBRATION_URN}"

SAMPLE_DATASET_COLUMNS = {
    "score_columns": ["score", "se", "epsilon"],
    "count_columns": ["count1", "count2"],
}

EVERY_NAMESPACE = [CORE_NAMESPACE, *CsvNamespace, "clinvar.2024_01", CALIBRATION_NS]


class _TargetGene:
    def __init__(self, name):
        self.name = name


class _ScoreSet:
    urn = "urn:mavedb:00000001-a-1"
    target_genes = [_TargetGene("BRCA1")]


class _Variant:
    """Enough of a Variant for every namespace's resolvers to run against."""

    urn = "urn:mavedb:00000001-a-1#1"
    hgvs_nt = "c.1A>G"
    hgvs_splice = None
    hgvs_pro = "p.Met1Val"
    data = {"score_data": {"score": 1.0}, "count_data": {"count1": 2}}
    score_set = _ScoreSet()


# ---------------------------------------------------------------------------
# TestNamespaceSpecs
# ---------------------------------------------------------------------------


class TestNamespaceSpecs:
    def test_every_static_namespace_has_a_spec(self):
        """A namespace in the published vocabulary with no descriptor would silently produce no columns."""
        for namespace in CsvNamespace:
            assert namespace_spec(namespace) is not None, namespace

    def test_parameterized_namespaces_resolve_to_a_spec(self):
        assert namespace_spec("clinvar.2024_01") is not None
        assert namespace_spec(CALIBRATION_NS) is not None
        assert namespace_spec("not_a_namespace") is None

    @pytest.mark.parametrize("namespace", [ns for ns in CsvNamespace] + ["clinvar.2024_01"])
    def test_every_declared_column_can_be_resolved(self, namespace):
        """A column with no resolver raises at row-assembly time, one row into a download."""
        spec = namespace_spec(namespace)
        assert spec is not None
        for column_key in spec.columns(SAMPLE_DATASET_COLUMNS):
            assert spec.resolver(column_key) is not None, f"{namespace}.{column_key}"

    def test_a_namespace_reading_through_a_relationship_declares_the_eager_load_it_needs(self):
        """Otherwise the variant query would not eager-load it and every row would pay a query."""
        assert namespace_spec(CsvNamespace.SCORE_SET).needs_score_set
        assert namespace_spec(CALIBRATION_NS).needs_score_set

    def test_every_static_namespace_is_labeled_for_a_picker(self):
        for namespace in CsvNamespace:
            spec = namespace_spec(namespace)
            assert spec.label and spec.group, namespace

    def test_resolvers_report_missing_data_rather_than_raising(self):
        """Every source is optional on some row: an unmapped variant, a release with no record for it."""
        for namespace in CsvNamespace:
            spec = namespace_spec(namespace)
            if spec.source in (RowSource.VARIANT, RowSource.MATCH_TYPE):
                continue
            for column_key in spec.columns(SAMPLE_DATASET_COLUMNS):
                assert spec.resolver(column_key)(None) is None, f"{namespace}.{column_key}"


# ---------------------------------------------------------------------------
# TestRowSourceDispatch
#
# A spec names its source; the scope or a registered loader is what puts that source on each row. A source
# with neither would render NA in every row of every export, silently, which is the failure mode these
# descriptors exist to prevent.
# ---------------------------------------------------------------------------


class TestRowSourceDispatch:
    def test_every_row_source_is_exercised_below(self):
        """Guards the parametrization: a new RowSource no namespace here uses would go untested."""
        sources = {namespace_spec(namespace).source for namespace in EVERY_NAMESPACE}

        assert sources == set(RowSource)

    def test_every_row_source_is_supplied_by_the_scope_or_a_loader(self):
        assert SCOPE_SOURCES.isdisjoint(SOURCE_LOADERS)
        assert SCOPE_SOURCES | set(SOURCE_LOADERS) == set(RowSource)

    def test_every_loader_runs_after_the_sources_it_requires(self):
        loaded = set(SCOPE_SOURCES)
        for source, loader in SOURCE_LOADERS.items():
            assert loader.requires <= loaded, source
            loaded.add(source)

    @pytest.mark.parametrize("namespace", EVERY_NAMESPACE)
    def test_every_planned_column_produces_a_cell(self, namespace):
        spec = namespace_spec(namespace)
        plan = CsvPlan.from_columns({namespace: spec.columns(SAMPLE_DATASET_COLUMNS)}, namespaced=True)

        row = next(csv_records(plan, [{RowSource.VARIANT: _Variant()}]))

        # Sources other than the variant are absent here, so theirs are NA; the point is that every planned
        # column reached a cell.
        assert len(row) == len(spec.columns(SAMPLE_DATASET_COLUMNS)), namespace
