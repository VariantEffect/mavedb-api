import pytest

from mavedb.lib.annotation.flatten import FlatAnnotation
from mavedb.lib.csv.columns import (
    _is_output_null,
    assemble_csv_headers,
    drop_unused_hgvs_columns,
    plan_csv_columns,
    rows_to_csv,
    variant_to_csv_row,
)
from tests.helpers.constants import VALID_CALIBRATION_URN

# ---------------------------------------------------------------------------
# MockVariant
# ---------------------------------------------------------------------------


class MockVariant:
    """Lightweight mock for Variant used in variant_to_csv_row tests."""

    def __init__(self, urn="urn:mavedb:00000001-a-1#1", hgvs_nt=None, hgvs_splice=None, hgvs_pro=None, data=None):
        self.urn = urn
        self.hgvs_nt = hgvs_nt
        self.hgvs_splice = hgvs_splice
        self.hgvs_pro = hgvs_pro
        self.data = data


# ---------------------------------------------------------------------------
# TestVariantToCsvRowNullHandling
# ---------------------------------------------------------------------------


class TestVariantToCsvRowNullHandling:
    """Tests that variant_to_csv_row represents missing data as na_rep, not 'None'."""

    def test_score_data_with_none_value_uses_na_rep(self):
        variant = MockVariant(data={"score_data": {"score": None}})
        columns = {"scores": ["score"]}

        row = variant_to_csv_row(variant, columns)

        assert row["score"] == "NA"

    def test_score_data_with_missing_key_uses_na_rep(self):
        variant = MockVariant(data={"score_data": {}})
        columns = {"scores": ["score"]}

        row = variant_to_csv_row(variant, columns)

        assert row["score"] == "NA"

    def test_score_data_with_no_score_data_key_uses_na_rep(self):
        variant = MockVariant(data={})
        columns = {"scores": ["score"]}

        row = variant_to_csv_row(variant, columns)

        assert row["score"] == "NA"

    def test_score_data_with_no_data_uses_na_rep(self):
        variant = MockVariant(data=None)
        columns = {"scores": ["score"]}

        row = variant_to_csv_row(variant, columns)

        assert row["score"] == "NA"

    def test_count_data_with_none_value_uses_na_rep(self):
        variant = MockVariant(data={"count_data": {"count1": None}})
        columns = {"counts": ["count1"]}

        row = variant_to_csv_row(variant, columns)

        assert row["count1"] == "NA"

    def test_count_data_with_missing_key_uses_na_rep(self):
        variant = MockVariant(data={"count_data": {}})
        columns = {"counts": ["count1"]}

        row = variant_to_csv_row(variant, columns)

        assert row["count1"] == "NA"

    def test_count_data_with_no_count_data_key_uses_na_rep(self):
        variant = MockVariant(data={})
        columns = {"counts": ["count1"]}

        row = variant_to_csv_row(variant, columns)

        assert row["count1"] == "NA"

    def test_count_data_with_no_data_uses_na_rep(self):
        variant = MockVariant(data=None)
        columns = {"counts": ["count1"]}

        row = variant_to_csv_row(variant, columns)

        assert row["count1"] == "NA"

    def test_score_data_with_valid_value_preserved(self):
        variant = MockVariant(data={"score_data": {"score": 1.5}})
        columns = {"scores": ["score"]}

        row = variant_to_csv_row(variant, columns)

        assert row["score"] == "1.5"

    def test_count_data_with_valid_value_preserved(self):
        variant = MockVariant(data={"count_data": {"count1": 42}})
        columns = {"counts": ["count1"]}

        row = variant_to_csv_row(variant, columns)

        assert row["count1"] == "42"

    def test_score_data_with_custom_na_rep(self):
        variant = MockVariant(data={"score_data": {"score": None}})
        columns = {"scores": ["score"]}

        row = variant_to_csv_row(variant, columns, na_rep="N/A")

        assert row["score"] == "N/A"

    def test_namespaced_score_data_with_none_value_uses_na_rep(self):
        variant = MockVariant(data={"score_data": {"score": None}})
        columns = {"scores": ["score"]}

        row = variant_to_csv_row(variant, columns, namespaced=True)

        assert row["scores.score"] == "NA"

    def test_namespaced_count_data_with_none_value_uses_na_rep(self):
        variant = MockVariant(data={"count_data": {"count1": None}})
        columns = {"counts": ["count1"]}

        row = variant_to_csv_row(variant, columns, namespaced=True)

        assert row["counts.count1"] == "NA"

    def test_core_columns_with_none_hgvs_uses_na_rep(self):
        variant = MockVariant(hgvs_nt=None, hgvs_pro=None, hgvs_splice=None, urn="urn:mavedb:00000001-a-1#1")
        columns = {"core": ["accession", "hgvs_nt", "hgvs_splice", "hgvs_pro"]}

        row = variant_to_csv_row(variant, columns)

        assert row["hgvs_nt"] == "NA"
        assert row["hgvs_pro"] == "NA"
        assert row["hgvs_splice"] == "NA"
        assert row["accession"] == "urn:mavedb:00000001-a-1#1"

    def test_mixed_columns_with_missing_data(self):
        variant = MockVariant(
            hgvs_nt="g.1A>G",
            hgvs_pro="p.Met1Val",
            data={"score_data": {"score": None, "se": 0.1}, "count_data": {"count1": None, "count2": 5}},
        )
        columns = {
            "core": ["hgvs_nt", "hgvs_pro"],
            "scores": ["score", "se"],
            "counts": ["count1", "count2"],
        }

        row = variant_to_csv_row(variant, columns)

        assert row["hgvs_nt"] == "g.1A>G"
        assert row["hgvs_pro"] == "p.Met1Val"
        assert row["score"] == "NA"
        assert row["se"] == "0.1"
        assert row["count1"] == "NA"
        assert row["count2"] == "5"


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
    variant = MockVariant()
    with pytest.raises(ValueError, match="unrecognized .* column: bogus_col"):
        variant_to_csv_row(variant, columns)


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

        row = variant_to_csv_row(
            MockVariant(),
            {CALIBRATION_NS: CALIBRATION_COLUMNS},
            annotations_by_ns={CALIBRATION_NS: annotation},
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

        row = variant_to_csv_row(
            MockVariant(),
            {CALIBRATION_NS: ["acmg_criterion"]},
            annotations_by_ns={CALIBRATION_NS: annotation},
            namespaced=False,
        )

        assert row == {f"{CALIBRATION_NS}.acmg_criterion": "PS3"}

    def test_no_annotation_uses_na_rep(self):
        row = variant_to_csv_row(MockVariant(), {CALIBRATION_NS: CALIBRATION_COLUMNS})

        assert all(row[f"{CALIBRATION_NS}.{column}"] == "NA" for column in CALIBRATION_COLUMNS)

    def test_annotation_absent_for_this_namespace_uses_na_rep(self):
        other_ns = "calibration.urn:mavedb:calibration-00000000-0000-0000-0000-000000000000"

        row = variant_to_csv_row(
            MockVariant(),
            {CALIBRATION_NS: ["acmg_criterion"]},
            annotations_by_ns={other_ns: FlatAnnotation(acmg_criterion="PS3")},
        )

        assert row[f"{CALIBRATION_NS}.acmg_criterion"] == "NA"

    def test_multiple_calibration_namespaces_are_independent(self):
        first_ns = CALIBRATION_NS
        second_ns = "calibration.urn:mavedb:calibration-00000000-0000-0000-0000-000000000000"

        row = variant_to_csv_row(
            MockVariant(),
            {first_ns: ["acmg_criterion"], second_ns: ["acmg_criterion"]},
            annotations_by_ns={
                first_ns: FlatAnnotation(acmg_criterion="PS3"),
                second_ns: FlatAnnotation(acmg_criterion="BS3"),
            },
        )

        assert row[f"{first_ns}.acmg_criterion"] == "PS3"
        assert row[f"{second_ns}.acmg_criterion"] == "BS3"


class TestScoreSetNamespace:
    """Tests that the score_set namespace reports the row's score set and target genes."""

    def test_populated_score_set(self):
        variant = MockVariantWithScoreSet(score_set=MockScoreSetForContext(target_gene_names=["BRCA1", "BRCA2"]))
        columns = {"score_set": ["score_set_urn", "target_gene"]}

        row = variant_to_csv_row(variant, columns)

        assert row["score_set_urn"] == "urn:mavedb:00000001-a-1"
        assert row["target_gene"] == "BRCA1; BRCA2"

    def test_empty_collections_use_na_rep(self):
        variant = MockVariantWithScoreSet(score_set=MockScoreSetForContext())

        row = variant_to_csv_row(variant, {"score_set": ["target_gene"]})

        assert row["target_gene"] == "NA"

    def test_publication_identifiers_is_not_a_column(self):
        """Dropped: it repeats on every row of a score set and score_set_urn already resolves to it."""
        variant = MockVariantWithScoreSet(score_set=MockScoreSetForContext())

        with pytest.raises(ValueError, match="unrecognized score_set column: publication_identifiers"):
            variant_to_csv_row(variant, {"score_set": ["publication_identifiers"]})


class TestRelationshipNamespace:
    """Tests that the relationship namespace reports the caller-supplied match type."""

    def test_populated_match_type(self):
        row = variant_to_csv_row(MockVariant(), {"relationship": ["match_type"]}, match_type="exact")

        assert row["match_type"] == "exact"

    def test_missing_match_type_uses_na_rep(self):
        row = variant_to_csv_row(MockVariant(), {"relationship": ["match_type"]})

        assert row["match_type"] == "NA"


# ---------------------------------------------------------------------------
# TestRowsToCsv
# ---------------------------------------------------------------------------


class TestRowsToCsv:
    def test_header_only_when_no_rows(self):
        assert rows_to_csv([], ["a", "b"]).splitlines() == ["a,b"]

    def test_writes_rows_in_column_order(self):
        rows = [{"b": "2", "a": "1"}, {"a": "3", "b": "4"}]

        assert rows_to_csv(rows, ["a", "b"]).splitlines() == ["a,b", "1,2", "3,4"]

    def test_quotes_values_containing_commas(self):
        assert rows_to_csv([{"a": "x,y"}], ["a"]).splitlines()[1] == '"x,y"'


# ---------------------------------------------------------------------------
# TestPlanCsvColumns
# ---------------------------------------------------------------------------


SAMPLE_DATASET_COLUMNS = {
    "score_columns": ["score", "se", "epsilon"],
    "count_columns": ["count1", "count2"],
}


@pytest.mark.unit
@pytest.mark.parametrize(
    "namespaces, expected_ns_keys, expected_columns, expected_clinvar",
    [
        # `scores` is the one column dataframe validation mandates, nothing more.
        (["scores"], {"core", "scores"}, {"scores": ["score"]}, {}),
        # The investigator's remaining score columns are their own request token.
        (["scores_custom"], {"core", "scores_custom"}, {"scores_custom": ["se", "epsilon"]}, {}),
        # Asking for both reproduces the whole score group, `score` first.
        (
            ["scores", "scores_custom"],
            {"core", "scores", "scores_custom"},
            {"scores": ["score"], "scores_custom": ["se", "epsilon"]},
            {},
        ),
        # Counts have no required column, so they are always taken in full.
        (["counts"], {"core", "counts"}, {"counts": ["count1", "count2"]}, {}),
        (
            ["scores", "counts"],
            {"core", "scores", "counts"},
            {"scores": ["score"], "counts": ["count1", "count2"]},
            {},
        ),
        (["vep"], {"core", "vep"}, {"vep": ["vep_functional_consequence"]}, {}),
        (["gnomad"], {"core", "gnomad"}, {"gnomad": ["gnomad_af"]}, {}),
        (["clingen"], {"core", "clingen"}, {"clingen": ["clingen_allele_id"]}, {}),
        (["scores", "mavedb"], {"core", "scores", "mavedb"}, {"scores": ["score"]}, {}),
        (["clinvar.2024_01"], {"core", "clinvar.2024_01"}, {}, {"clinvar.2024_01": "01_2024"}),
        (
            ["clinvar.2024_01", "clinvar.2025_06"],
            {"core", "clinvar.2024_01", "clinvar.2025_06"},
            {},
            {"clinvar.2024_01": "01_2024", "clinvar.2025_06": "06_2025"},
        ),
        # A namespace requested twice is planned once, or it would emit its columns twice.
        (["scores", "scores"], {"core", "scores"}, {"scores": ["score"]}, {}),
    ],
)
def test_plan_csv_columns(namespaces, expected_ns_keys, expected_columns, expected_clinvar):
    plan = plan_csv_columns(SAMPLE_DATASET_COLUMNS, namespaces)

    assert set(plan.namespaced_columns.keys()) == expected_ns_keys
    assert plan.clinvar_namespaces == expected_clinvar
    for namespace, columns in expected_columns.items():
        assert plan.namespaced_columns[namespace] == columns

    assert plan.namespaced_columns["core"] == ["accession", "hgvs_nt", "hgvs_splice", "hgvs_pro"]

    for ns in expected_clinvar:
        assert plan.namespaced_columns[ns] == ["clinical_significance", "clinical_review_status"]


def test_plan_csv_columns_reference_hgvs_namespace_populates_columns():
    plan = plan_csv_columns(SAMPLE_DATASET_COLUMNS, ["scores", "mavedb"])
    assert plan.namespaced_columns["mavedb"] == [
        "post_mapped_hgvs_g",
        "post_mapped_hgvs_p",
        "post_mapped_hgvs_c",
        "post_mapped_hgvs_at_assay_level",
        "post_mapped_vrs_digest",
    ]


# ---------------------------------------------------------------------------
# TestAssembleCsvHeaders
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
def test_assemble_csv_headers(namespaced_columns, namespaced, expected):
    assert assemble_csv_headers(namespaced_columns, namespaced) == expected


# ---------------------------------------------------------------------------
# TestDropNaColumns
# ---------------------------------------------------------------------------


class TestDropNaColumns:
    def test_removes_all_na_hgvs_column(self):
        rows = [
            {"hgvs_nt": "g.1A>G", "hgvs_splice": "NA", "hgvs_pro": "p.Met1Val"},
            {"hgvs_nt": "g.2C>T", "hgvs_splice": "NA", "hgvs_pro": "p.Ala2Gly"},
        ]
        columns = ["hgvs_nt", "hgvs_splice", "hgvs_pro"]

        new_rows, new_cols = drop_unused_hgvs_columns(rows, columns)

        assert "hgvs_splice" not in new_cols
        assert "hgvs_nt" in new_cols
        assert "hgvs_pro" in new_cols
        for row in new_rows:
            assert "hgvs_splice" not in row

    def test_keeps_column_with_some_values(self):
        rows = [
            {"hgvs_nt": "g.1A>G", "hgvs_splice": "NA", "hgvs_pro": "p.Met1Val"},
            {"hgvs_nt": "g.2C>T", "hgvs_splice": "c.1A>G", "hgvs_pro": "p.Ala2Gly"},
        ]
        columns = ["hgvs_nt", "hgvs_splice", "hgvs_pro"]

        new_rows, new_cols = drop_unused_hgvs_columns(rows, columns)

        assert new_cols == ["hgvs_nt", "hgvs_splice", "hgvs_pro"]

    def test_does_not_touch_non_hgvs_columns(self):
        rows = [
            {"hgvs_nt": "g.1A>G", "hgvs_splice": "NA", "hgvs_pro": "NA", "score": "NA"},
        ]
        columns = ["hgvs_nt", "hgvs_splice", "hgvs_pro", "score"]

        new_rows, new_cols = drop_unused_hgvs_columns(rows, columns)

        assert "score" in new_cols
        assert "hgvs_splice" not in new_cols

    def test_empty_rows_does_not_crash(self):
        rows = []
        columns = ["hgvs_nt", "hgvs_splice", "hgvs_pro"]

        new_rows, new_cols = drop_unused_hgvs_columns(rows, columns)

        assert new_rows == []
        assert new_cols == []


def test_plan_csv_columns_omits_reference_hgvs_when_not_requested():
    """It used to be a boolean flag, so the key was always present even when empty."""
    plan = plan_csv_columns(SAMPLE_DATASET_COLUMNS, ["scores"])

    assert "mavedb" not in plan.namespaced_columns


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
class TestAssembleCsvHeadersRejectsCollisions:
    """Un-namespaced output strips the prefix that keeps two namespaces' columns apart.

    The endpoints that ask for un-namespaced output request one namespace each; this holds them to it
    rather than letting a future caller discover the problem from a CSV with a column written twice.
    """

    def test_un_namespaced_collision_raises(self):
        with pytest.raises(ValueError, match="duplicate columns"):
            assemble_csv_headers({"scores": ["score"], "counts": ["score"]}, namespaced=False)

    def test_the_error_names_the_offending_column_and_namespaces(self):
        with pytest.raises(ValueError) as excinfo:
            assemble_csv_headers({"scores": ["shared"], "counts": ["shared"]}, namespaced=False)

        assert "shared" in str(excinfo.value)
        assert "scores" in str(excinfo.value) and "counts" in str(excinfo.value)

    def test_namespacing_resolves_what_would_otherwise_collide(self):
        headers = assemble_csv_headers({"scores": ["shared"], "counts": ["shared"]}, namespaced=True)

        assert headers == ["scores.shared", "counts.shared"]

    def test_scores_and_custom_scores_share_a_prefix_without_colliding(self):
        """They emit under one prefix by design, so the guard must not fire on disjoint column sets."""
        headers = assemble_csv_headers({"scores": ["score"], "scores_custom": ["se"]}, namespaced=True)

        assert headers == ["scores.score", "scores.se"]

    def test_a_namespace_sharing_a_prefix_still_collides_on_a_repeated_column(self):
        with pytest.raises(ValueError, match="duplicate columns"):
            assemble_csv_headers({"scores": ["score"], "scores_custom": ["score"]}, namespaced=True)
