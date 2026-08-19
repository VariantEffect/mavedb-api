"""The descriptors are what keep planning, row assembly and fetching from drifting apart."""

import pytest

from mavedb.lib.csv.columns import variant_to_csv_row
from mavedb.lib.csv.namespaces import CsvNamespace
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

    def test_a_namespace_reading_through_a_relationship_declares_the_fetch_it_needs(self):
        """Otherwise the fetch layer would not eager-load it and every row would pay a query."""
        for namespace in (CsvNamespace.REFERENCE_HGVS, CsvNamespace.VEP, CsvNamespace.CLINGEN):
            assert namespace_spec(namespace).needs_mappings, namespace

        gnomad = namespace_spec(CsvNamespace.GNOMAD)
        assert gnomad.needs_gnomad and gnomad.needs_mappings

        assert namespace_spec(CsvNamespace.SCORE_SET).needs_score_set
        calibration = namespace_spec(CALIBRATION_NS)
        assert calibration.needs_mappings and calibration.needs_score_set

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
# A spec names its source; `variant_to_csv_row` is what turns that name into the datum the resolvers are
# called with. That mapping is the one part of the descriptor contract the tests above cannot see, and a
# source with no entry in it raises KeyError at row-assembly time — one row into a download, which is the
# failure mode these descriptors exist to prevent.
# ---------------------------------------------------------------------------


class TestRowSourceDispatch:
    def test_every_row_source_is_exercised_below(self):
        """Guards the parametrization: a new RowSource no namespace here uses would go untested."""
        sources = {namespace_spec(namespace).source for namespace in EVERY_NAMESPACE}

        assert sources == set(RowSource)

    @pytest.mark.parametrize("namespace", EVERY_NAMESPACE)
    def test_every_declared_source_resolves_to_a_row_datum(self, namespace):
        spec = namespace_spec(namespace)
        columns = {namespace: spec.columns(SAMPLE_DATASET_COLUMNS)}

        row = variant_to_csv_row(_Variant(), columns, namespaced=True)

        # Every planned column produced a cell. The parameterized namespaces are passed no per-row datum,
        # so theirs are NA — the point here is that the dispatch reached them at all.
        assert len(row) == len(columns[namespace]), namespace
