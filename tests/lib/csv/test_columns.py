import csv
from io import StringIO

import pytest

from mavedb.lib.annotation.flatten import FlatAnnotation
from mavedb.lib.csv.columns import _OUTPUT_NULL_STRINGS, CsvPlan, _is_output_null, csv_records, render_csv
from mavedb.lib.csv.namespaces import CsvNamespace
from mavedb.lib.csv.specs import RowSource
from mavedb.lib.mave.utils import NA_VALUE
from tests.helpers.constants import VALID_CALIBRATION_URN
from tests.helpers.mocks.factories import create_mock_mapped_variant
from tests.helpers.variant_shapes import VARIANT_SHAPES, shape_ids, to_csv_mapped_row

# ---------------------------------------------------------------------------
# MockVariant
# ---------------------------------------------------------------------------


class MockVariant:
    """Lightweight mock for Variant used in row tests."""

    def __init__(self, urn="urn:mavedb:00000001-a-1#1", hgvs_nt=None, hgvs_splice=None, hgvs_pro=None, data=None):
        self.urn = urn
        self.hgvs_nt = hgvs_nt
        self.hgvs_splice = hgvs_splice
        self.hgvs_pro = hgvs_pro
        self.data = data


def _row(variant, columns, *, namespaced=False, na_rep=NA_VALUE, sources=None):
    """One row planned from exactly *columns*, read off *variant* and any other *sources*."""
    plan = CsvPlan.from_columns(columns, namespaced=namespaced)
    return next(csv_records(plan, [{RowSource.VARIANT: variant, **(sources or {})}], na_rep=na_rep))


# ---------------------------------------------------------------------------
# TestVariantToCsvRowNullHandling
# ---------------------------------------------------------------------------


class TestVariantToCsvRowNullHandling:
    """How a row reports absent data.

    The value -> NA rule itself is specified by ``test_is_output_null``; every namespace reaches it through
    the single ``_value_or_na`` call in ``csv_values``, so it is not re-asserted per namespace here.
    What these cover is the layer above it: the per-row datum a resolver is handed, which is where "no data"
    has several distinct shapes.
    """

    # The score and count namespaces are one mechanism selected by their data key, so they are covered together.
    DATA_NAMESPACES = [("scores", "score_data", "score"), ("counts", "count_data", "count1")]

    @pytest.mark.parametrize("namespace, data_key, column", DATA_NAMESPACES)
    @pytest.mark.parametrize(
        "build_data",
        [lambda data_key: None, lambda data_key: {}, lambda data_key: {data_key: {}}],
        ids=["no_data_at_all", "no_entry_for_this_namespace", "entry_present_but_column_missing"],
    )
    def test_dynamic_columns_survive_every_shape_of_absent_data(self, namespace, data_key, column, build_data):
        """``variant.data`` may be missing, lack this namespace's entry, or lack the column within it."""
        variant = MockVariant(data=build_data(data_key))

        row = _row(variant, {namespace: [column]})

        assert row[column] == "NA"

    @pytest.mark.parametrize("namespace, data_key, column", DATA_NAMESPACES)
    def test_a_present_value_is_stringified(self, namespace, data_key, column):
        """The non-null half of ``_value_or_na``, which nothing else asserts directly."""
        variant = MockVariant(data={data_key: {column: 1.5}})

        row = _row(variant, {namespace: [column]})

        assert row[column] == "1.5"

    def test_na_rep_is_configurable(self):
        variant = MockVariant(data={"score_data": {"score": None}})

        row = _row(variant, {"scores": ["score"]}, na_rep="N/A")

        assert row["score"] == "N/A"

    def test_an_absent_column_does_not_disturb_its_neighbours(self):
        """One missing datum must not drop a key or corrupt another column in the same row."""
        variant = MockVariant(
            hgvs_nt="g.1A>G",
            hgvs_pro=None,
            data={"score_data": {"score": None, "se": 0.1}, "count_data": {"count1": None, "count2": 5}},
        )
        columns = {"core": ["hgvs_nt", "hgvs_pro"], "scores": ["score", "se"], "counts": ["count1", "count2"]}

        row = _row(variant, columns)

        assert row == {
            "hgvs_nt": "g.1A>G",
            "hgvs_pro": "NA",
            "score": "NA",
            "se": "0.1",
            "count1": "NA",
            "count2": "5",
        }


# ---------------------------------------------------------------------------
# TestVariantToCsvRowUnrecognizedKey
# ---------------------------------------------------------------------------


@pytest.mark.unit
@pytest.mark.parametrize(
    "namespace, columns",
    [
        ("core", {"core": ["bogus_col"]}),
        ("mavedb", {"mavedb": ["bogus_col"]}),
        ("vep", {"vep": ["bogus_col"]}),
        ("gnomad", {"gnomad": ["bogus_col"]}),
        ("clingen", {"clingen": ["bogus_col"]}),
        ("clinvar.2024_01", {"clinvar.2024_01": ["bogus_col"]}),
        ("score_set", {"score_set": ["bogus_col"]}),
        ("relationship", {"relationship": ["bogus_col"]}),
        ("calibration", {f"calibration.{VALID_CALIBRATION_URN}": ["bogus_col"]}),
    ],
)
def test_unrecognized_column_key_raises(namespace, columns):
    """At planning, before any row is read."""
    with pytest.raises(ValueError, match="unrecognized .* column: bogus_col"):
        CsvPlan.from_columns(columns)


# ---------------------------------------------------------------------------
# TestCalibrationScoreSetAndRelationshipNamespaces
# ---------------------------------------------------------------------------


CALIBRATION_NS = f"calibration.{VALID_CALIBRATION_URN}"

CALIBRATION_COLUMNS = [
    "title",
    "functional_classification",
    "acmg_criterion",
    "acmg_evidence_strength",
    "acmg_evidence_outcome_code",
    "pathogenicity_classification",
]


class MockTargetGene:
    def __init__(self, name):
        self.name = name


class MockScoreSetForContext:
    def __init__(self, urn="urn:mavedb:00000001-a-1", target_gene_names=()):
        self.urn = urn
        self.target_genes = [MockTargetGene(name) for name in target_gene_names]


class MockVariantWithScoreSet(MockVariant):
    def __init__(self, score_set=None, **kwargs):
        super().__init__(**kwargs)
        self.score_set = score_set


class TestCalibrationNamespace:
    """Tests that a calibration namespace renders a FlatAnnotation, keyed by the calibration's URN."""

    def test_populated_annotation(self):
        annotation = FlatAnnotation(
            functional_classification="abnormal",
            acmg_criterion="PS3",
            acmg_evidence_strength="MODERATE",
            acmg_evidence_outcome_code="PS3_moderate",
            pathogenicity_classification="PATHOGENIC",
            calibration_urn=VALID_CALIBRATION_URN,
            calibration_title="Clinical Calibration",
        )

        row = _row(
            MockVariant(),
            {CALIBRATION_NS: CALIBRATION_COLUMNS},
            sources={RowSource.ANNOTATION: {CALIBRATION_NS: annotation}},
        )

        assert row[f"{CALIBRATION_NS}.title"] == "Clinical Calibration"
        assert row[f"{CALIBRATION_NS}.functional_classification"] == "abnormal"
        assert row[f"{CALIBRATION_NS}.acmg_criterion"] == "PS3"
        assert row[f"{CALIBRATION_NS}.acmg_evidence_strength"] == "MODERATE"
        assert row[f"{CALIBRATION_NS}.acmg_evidence_outcome_code"] == "PS3_moderate"
        assert row[f"{CALIBRATION_NS}.pathogenicity_classification"] == "PATHOGENIC"

    def test_calibration_columns_are_always_namespaced(self):
        """The URN in the header is what disambiguates, so it is kept even for un-namespaced output."""
        annotation = FlatAnnotation(acmg_criterion="PS3")

        row = _row(
            MockVariant(),
            {CALIBRATION_NS: ["acmg_criterion"]},
            sources={RowSource.ANNOTATION: {CALIBRATION_NS: annotation}},
            namespaced=False,
        )

        assert row == {f"{CALIBRATION_NS}.acmg_criterion": "PS3"}

    def test_no_annotation_uses_na_rep(self):
        row = _row(MockVariant(), {CALIBRATION_NS: CALIBRATION_COLUMNS})

        assert all(row[f"{CALIBRATION_NS}.{column}"] == "NA" for column in CALIBRATION_COLUMNS)

    def test_annotation_absent_for_this_namespace_uses_na_rep(self):
        other_ns = "calibration.urn:mavedb:calibration-00000000-0000-0000-0000-000000000000"

        row = _row(
            MockVariant(),
            {CALIBRATION_NS: ["acmg_criterion"]},
            sources={RowSource.ANNOTATION: {other_ns: FlatAnnotation(acmg_criterion="PS3")}},
        )

        assert row[f"{CALIBRATION_NS}.acmg_criterion"] == "NA"

    def test_multiple_calibration_namespaces_are_independent(self):
        first_ns = CALIBRATION_NS
        second_ns = "calibration.urn:mavedb:calibration-00000000-0000-0000-0000-000000000000"

        row = _row(
            MockVariant(),
            {first_ns: ["acmg_criterion"], second_ns: ["acmg_criterion"]},
            sources={
                RowSource.ANNOTATION: {
                    first_ns: FlatAnnotation(acmg_criterion="PS3"),
                    second_ns: FlatAnnotation(acmg_criterion="BS3"),
                }
            },
        )

        assert row[f"{first_ns}.acmg_criterion"] == "PS3"
        assert row[f"{second_ns}.acmg_criterion"] == "BS3"


class TestScoreSetNamespace:
    """Tests that the score_set namespace reports the row's score set and target genes."""

    def test_populated_score_set(self):
        variant = MockVariantWithScoreSet(score_set=MockScoreSetForContext(target_gene_names=["BRCA1", "BRCA2"]))
        columns = {"score_set": ["score_set_urn", "target_gene"]}

        row = _row(variant, columns)

        assert row["score_set_urn"] == "urn:mavedb:00000001-a-1"
        assert row["target_gene"] == "BRCA1; BRCA2"

    def test_empty_collections_use_na_rep(self):
        variant = MockVariantWithScoreSet(score_set=MockScoreSetForContext())

        row = _row(variant, {"score_set": ["target_gene"]})

        assert row["target_gene"] == "NA"

    def test_publication_identifiers_is_not_a_column(self):
        """Dropped: it repeats on every row of a score set and score_set_urn already resolves to it."""
        with pytest.raises(ValueError, match="unrecognized score_set column: publication_identifiers"):
            CsvPlan.from_columns({"score_set": ["publication_identifiers"]})


class TestRelationshipNamespace:
    """Tests that the relationship namespace reports the caller-supplied match type."""

    def test_populated_match_type(self):
        row = _row(MockVariant(), {"relationship": ["match_type"]}, sources={RowSource.MATCH_TYPE: "exact"})

        assert row["match_type"] == "exact"

    def test_missing_match_type_uses_na_rep(self):
        row = _row(MockVariant(), {"relationship": ["match_type"]})

        assert row["match_type"] == "NA"


# ---------------------------------------------------------------------------
# TestRenderCsv
# ---------------------------------------------------------------------------


class TestRenderCsv:
    PLAN = CsvPlan.from_columns({"core": ["accession", "hgvs_nt"]})

    @staticmethod
    def _rows(*variants):
        return [{RowSource.VARIANT: variant} for variant in variants]

    def test_header_only_when_no_rows(self):
        assert "".join(render_csv(self.PLAN, [])).splitlines() == ["accession,hgvs_nt"]

    def test_writes_rows_in_column_order(self):
        rows = self._rows(MockVariant(urn="u1", hgvs_nt="c.1A>G"), MockVariant(urn="u2", hgvs_nt="c.2A>G"))

        assert "".join(render_csv(self.PLAN, [rows])).splitlines() == ["accession,hgvs_nt", "u1,c.1A>G", "u2,c.2A>G"]

    def test_quotes_values_containing_commas(self):
        rows = self._rows(MockVariant(urn="u1", hgvs_nt="x,y"))

        assert "".join(render_csv(self.PLAN, [rows])).splitlines()[1] == 'u1,"x,y"'

    def test_one_chunk_per_batch_with_the_header_leading_the_first(self):
        batches = [self._rows(MockVariant(urn="u1")), self._rows(MockVariant(urn="u2"))]

        chunks = list(render_csv(self.PLAN, batches))

        assert chunks == ["accession,hgvs_nt\r\nu1,NA\r\n", "u2,NA\r\n"]


# ---------------------------------------------------------------------------
# TestPlanCsvColumns
# ---------------------------------------------------------------------------


SAMPLE_DATASET_COLUMNS = {
    "score_columns": ["score", "se", "epsilon"],
    "count_columns": ["count1", "count2"],
}


CORE_HEADERS = ["accession", "hgvs_nt", "hgvs_splice", "hgvs_pro"]
GNOMAD_HEADERS = [
    "gnomad.gnomad_af",
    "gnomad.gnomad_ac",
    "gnomad.gnomad_an",
    "gnomad.gnomad_faf95_max",
    "gnomad.gnomad_faf95_max_ancestry",
    "gnomad.gnomad_id",
    "gnomad.gnomad_version",
]
REFERENCE_HGVS_HEADERS = [
    "mavedb.post_mapped_hgvs_g",
    "mavedb.post_mapped_hgvs_p",
    "mavedb.post_mapped_hgvs_c",
    "mavedb.post_mapped_hgvs_at_assay_level",
    "mavedb.post_mapped_vrs_id",
]


def _clinvar_headers(namespace):
    return [f"{namespace}.clinical_significance", f"{namespace}.clinical_review_status"]


@pytest.mark.unit
@pytest.mark.parametrize(
    "namespaces, expected_headers, expected_parameters",
    [
        # `scores` is the one column dataframe validation mandates, nothing more.
        (["scores"], ["scores.score"], {}),
        # The investigator's remaining score columns are their own request token.
        (["scores_custom"], ["scores.se", "scores.epsilon"], {}),
        # Asking for both reproduces the whole score group, `score` first.
        (["scores", "scores_custom"], ["scores.score", "scores.se", "scores.epsilon"], {}),
        # Counts have no required column, so they are always taken in full.
        (["counts"], ["counts.count1", "counts.count2"], {}),
        (["scores", "counts"], ["scores.score", "counts.count1", "counts.count2"], {}),
        (["vep"], ["vep.vep_functional_consequence"], {}),
        (["gnomad"], GNOMAD_HEADERS, {}),
        (["clingen"], ["clingen.clingen_allele_id"], {}),
        (["scores", "mavedb"], ["scores.score", *REFERENCE_HGVS_HEADERS], {}),
        (
            ["clinvar.2024_01"],
            _clinvar_headers("clinvar.2024_01"),
            {RowSource.CLINVAR_ENTRY: {"clinvar.2024_01": "01_2024"}},
        ),
        (
            ["clinvar.2024_01", "clinvar.2025_06"],
            [*_clinvar_headers("clinvar.2024_01"), *_clinvar_headers("clinvar.2025_06")],
            {RowSource.CLINVAR_ENTRY: {"clinvar.2024_01": "01_2024", "clinvar.2025_06": "06_2025"}},
        ),
        # A namespace requested twice is planned once, or it would emit its columns twice.
        (["scores", "scores"], ["scores.score"], {}),
    ],
)
def test_csv_plan_build(namespaces, expected_headers, expected_parameters):
    plan = CsvPlan.build(SAMPLE_DATASET_COLUMNS, namespaces, namespaced=True)

    assert plan.headers == [*CORE_HEADERS, *expected_headers]
    assert plan.parameters == expected_parameters


# ---------------------------------------------------------------------------
# TestCsvPlanHeaders
# ---------------------------------------------------------------------------


@pytest.mark.unit
@pytest.mark.parametrize(
    "namespaced_columns, namespaced, expected",
    [
        # Unnamespaced: flat column names
        (
            {"core": ["accession", "hgvs_nt"], "scores": ["score", "se"]},
            False,
            ["accession", "hgvs_nt", "score", "se"],
        ),
        # Namespaced: scores get prefix, core does not
        (
            {"core": ["accession", "hgvs_nt"], "scores": ["score"]},
            True,
            ["accession", "hgvs_nt", "scores.score"],
        ),
        # mavedb namespace always gets prefix when namespaced
        (
            {"core": ["accession"], "mavedb": ["post_mapped_hgvs_g"]},
            True,
            ["accession", "mavedb.post_mapped_hgvs_g"],
        ),
        # ClinVar namespaces always get prefix regardless of namespaced flag
        (
            {"core": ["accession"], "clinvar.2024_01": ["clinical_significance"]},
            False,
            ["accession", "clinvar.2024_01.clinical_significance"],
        ),
        # Mixed: respects insertion order
        (
            {
                "core": ["accession"],
                "mavedb": [],
                "scores": ["score"],
                "clinvar.2024_01": ["clinical_significance"],
            },
            True,
            ["accession", "scores.score", "clinvar.2024_01.clinical_significance"],
        ),
        # Empty mavedb namespace when not namespaced produces nothing
        (
            {"core": ["hgvs_nt"], "mavedb": []},
            False,
            ["hgvs_nt"],
        ),
    ],
)
def test_csv_plan_from_columns_headers(namespaced_columns, namespaced, expected):
    assert CsvPlan.from_columns(namespaced_columns, namespaced=namespaced).headers == expected


# ---------------------------------------------------------------------------
# TestCsvPlanWithout
# ---------------------------------------------------------------------------


def test_csv_plan_without_drops_only_the_named_columns():
    plan = CsvPlan.build(SAMPLE_DATASET_COLUMNS, ["scores"])

    assert plan.without(["hgvs_splice", "not_a_header"]).headers == ["accession", "hgvs_nt", "hgvs_pro", "score"]


def test_csv_plan_build_reads_no_mapping_when_none_is_requested():
    """It used to be a boolean flag, so the key was always present even when empty."""
    plan = CsvPlan.build(SAMPLE_DATASET_COLUMNS, ["scores"])

    assert not any(header.startswith("post_mapped") for header in plan.headers)
    assert RowSource.MAPPING not in plan.sources


# ---------------------------------------------------------------------------
# TestIsOutputNull
# ---------------------------------------------------------------------------


@pytest.mark.unit
@pytest.mark.parametrize(
    "value, expected",
    [
        (None, True),
        ("", True),
        ("  ", True),
        ("NA", True),
        ("na", True),
        ("None", True),
        ("none", True),
        ("NaN", True),
        ("nan", True),
        ("null", True),
        ("NULL", True),
        ("nil", True),
        ("N/A", True),
        ("undefined", True),
        ("1.5", False),
        ("0", False),
        ("hello", False),
        ("p.Met1Val", False),
    ],
)
def test_is_output_null(value, expected):
    assert _is_output_null(value) is expected


@pytest.mark.unit
def test_the_output_null_vocabulary_is_closed():
    """Spelled out because `_OUTPUT_NULL_STRINGS` is derived from an upload-parsing constant.

    ``mave.utils.NULL_VALUES`` exists to decide what a value read *from* a submitted file means. These
    tokens decide what the export *writes*, and the export's output is published. The parametrization
    above catches a token being dropped; only an equality check catches one being added, which is how a
    change made for the reading side would otherwise start rendering NA in a published dump.
    """
    assert _OUTPUT_NULL_STRINGS == frozenset({"n/a", "na", "nan", "nil", "none", "null", "undefined"})


@pytest.mark.unit
class TestCsvPlanRejectsCollisions:
    """Un-namespaced output strips the prefix that keeps two namespaces' columns apart.

    The endpoints that ask for un-namespaced output request one namespace each; this holds them to it
    rather than letting a future caller discover the problem from a CSV with a column written twice.
    """

    def test_un_namespaced_collision_raises(self):
        with pytest.raises(ValueError, match="duplicate columns"):
            CsvPlan.from_columns({"scores": ["score"], "counts": ["score"]}, namespaced=False)

    def test_the_error_names_the_offending_column_and_namespaces(self):
        with pytest.raises(ValueError) as excinfo:
            CsvPlan.from_columns({"scores": ["shared"], "counts": ["shared"]}, namespaced=False)

        assert "shared" in str(excinfo.value)
        assert "scores" in str(excinfo.value) and "counts" in str(excinfo.value)

    def test_namespacing_resolves_what_would_otherwise_collide(self):
        headers = CsvPlan.from_columns({"scores": ["shared"], "counts": ["shared"]}, namespaced=True).headers

        assert headers == ["scores.shared", "counts.shared"]

    def test_scores_and_custom_scores_share_a_prefix_without_colliding(self):
        """They emit under one prefix by design, so the guard must not fire on disjoint column sets."""
        headers = CsvPlan.from_columns({"scores": ["score"], "scores_custom": ["se"]}, namespaced=True).headers

        assert headers == ["scores.score", "scores.se"]

    def test_a_namespace_sharing_a_prefix_still_collides_on_a_repeated_column(self):
        with pytest.raises(ValueError, match="duplicate columns"):
            CsvPlan.from_columns({"scores": ["score"], "scores_custom": ["score"]}, namespaced=True)


# ---------------------------------------------------------------------------
# TestCsvRowAcrossVariantShapes
# ---------------------------------------------------------------------------


class TestCsvRowAcrossVariantShapes:
    """Compose a row for every mapped-variant shape the export surfaces have to survive.

    The tests above specify the row machinery against purpose-built inputs. These run the same composer
    over the shared shape list, which is where payload-dependent breakage lives: the ``mavedb`` namespace
    resolves ``post_mapped_hgvs_g``, ``post_mapped_hgvs_p``, and ``post_mapped_vrs_digest`` by walking the
    stored VRS object, and that object takes every form in the list — VRS 1.x nesting, a cis-phased block,
    the three state types, and an allele carrying no expressions at all.

    The contract is deliberately narrow, matching the annotation conformance suite: composing a row never
    raises, and the row carries exactly the planned columns. What each value should *be* is specified per
    namespace above, not re-asserted per shape.
    """

    # Every namespace whose resolvers read the variant or its mapping. gnomAD and ClinVar are excluded:
    # they are separate per-row arguments rather than properties of a shape, so they vary independently.
    SHAPE_SENSITIVE_NAMESPACES = [
        CsvNamespace.REFERENCE_HGVS,
        CsvNamespace.SCORES,
        CsvNamespace.VEP,
        CsvNamespace.CLINGEN,
    ]

    DATASET_COLUMNS = {"score_columns": ["score"], "count_columns": []}

    def _plan(self):
        return CsvPlan.build(self.DATASET_COLUMNS, [str(ns) for ns in self.SHAPE_SENSITIVE_NAMESPACES])

    @staticmethod
    def _source_row(mapped_variant):
        return {RowSource.VARIANT: mapped_variant.variant, RowSource.MAPPING: to_csv_mapped_row(mapped_variant)}

    @pytest.mark.parametrize("shape", VARIANT_SHAPES, ids=shape_ids())
    def test_a_row_composes_and_carries_every_planned_column(self, shape):
        mapped_variant = shape.build(create_mock_mapped_variant)
        plan = self._plan()

        row = next(csv_records(plan, [self._source_row(mapped_variant)]))

        assert set(row) == set(plan.headers)

    @pytest.mark.parametrize("shape", VARIANT_SHAPES, ids=shape_ids())
    def test_no_cell_leaks_a_mock(self, shape):
        """Guards the fixtures rather than the code, and is here because it caught a real mistake.

        ``_value_or_na`` stringifies whatever it is handed, so a resolver reading a MappedVariant field
        the factory never set gets a truthy MagicMock and writes its repr into the CSV as a perfectly
        well-formed string. Asserting cells are strings does not catch that; asserting they are not mocks
        does. Every shape here would have passed a type check while carrying ``<MagicMock ...>`` in three
        columns.
        """
        mapped_variant = shape.build(create_mock_mapped_variant)
        plan = self._plan()

        row = next(csv_records(plan, [self._source_row(mapped_variant)]))

        leaked = {column: value for column, value in row.items() if "Mock" in str(value)}
        assert not leaked, f"{shape.name} leaked mock reprs into the row: {leaked}"

    def test_a_mapping_without_hgvs_columns_renders_them_na(self):
        """The CSV-only axis: fields absent on the mapping must render NA, not a stand-in."""
        shape = next(s for s in VARIANT_SHAPES if s.name == "unmapped_hgvs_columns")
        mapped_variant = shape.build(create_mock_mapped_variant)
        plan = self._plan()

        row = next(csv_records(plan, [self._source_row(mapped_variant)]))

        assert row["post_mapped_hgvs_c"] == "NA"
        assert row["post_mapped_hgvs_at_assay_level"] == "NA"
        assert row["vep_functional_consequence"] == "NA"

    @pytest.mark.parametrize("shape", VARIANT_SHAPES, ids=shape_ids())
    def test_the_row_serializes_to_csv(self, shape):
        """The row is only useful if it survives the writer; a stray newline would split the record."""
        mapped_variant = shape.build(create_mock_mapped_variant)
        plan = self._plan()

        rendered = "".join(render_csv(plan, [[self._source_row(mapped_variant)]]))

        assert len(list(csv.reader(StringIO(rendered)))) == 2
