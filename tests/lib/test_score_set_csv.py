import pytest

from mavedb.lib.score_set_csv import (
    assemble_csv_headers,
    drop_na_columns_from_csv_file_rows,
    plan_csv_columns,
    variant_to_csv_row,
)

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
    ],
)
def test_unrecognized_column_key_raises(namespace, columns):
    variant = MockVariant()
    with pytest.raises(ValueError, match="unrecognized .* column: bogus_col"):
        variant_to_csv_row(variant, columns)


# ---------------------------------------------------------------------------
# TestPlanCsvColumns
# ---------------------------------------------------------------------------


SAMPLE_DATASET_COLUMNS = {
    "score_columns": ["score", "se", "epsilon"],
    "count_columns": ["count1", "count2"],
}


@pytest.mark.unit
@pytest.mark.parametrize(
    "namespaces, kwargs, expected_ns_keys, expected_score_cols, expected_clinvar",
    [
        # scores-only
        (
            ["scores"],
            {},
            {"core", "mavedb", "scores"},
            ["score", "se", "epsilon"],
            {},
        ),
        # counts-only
        (
            ["counts"],
            {},
            {"core", "mavedb", "counts"},
            None,
            {},
        ),
        # both scores and counts
        (
            ["scores", "counts"],
            {},
            {"core", "mavedb", "scores", "counts"},
            ["score", "se", "epsilon"],
            {},
        ),
        # vep adds its column
        (
            ["vep"],
            {},
            {"core", "mavedb", "vep"},
            None,
            {},
        ),
        # gnomad adds its column
        (
            ["gnomad"],
            {},
            {"core", "mavedb", "gnomad"},
            None,
            {},
        ),
        # clingen adds its column
        (
            ["clingen"],
            {},
            {"core", "mavedb", "clingen"},
            None,
            {},
        ),
        # include_custom_columns=False -> only REQUIRED_SCORE_COLUMN for scores
        (
            ["scores"],
            {"include_custom_columns": False},
            {"core", "mavedb", "scores"},
            ["score"],
            {},
        ),
        # include_post_mapped_hgvs populates mavedb namespace
        (
            ["scores"],
            {"include_post_mapped_hgvs": True},
            {"core", "mavedb", "scores"},
            ["score", "se", "epsilon"],
            {},
        ),
        # single ClinVar namespace
        (
            ["clinvar.2024_01"],
            {},
            {"core", "mavedb", "clinvar.2024_01"},
            None,
            {"clinvar.2024_01": "01_2024"},
        ),
        # multiple ClinVar versions
        (
            ["clinvar.2024_01", "clinvar.2025_06"],
            {},
            {"core", "mavedb", "clinvar.2024_01", "clinvar.2025_06"},
            None,
            {"clinvar.2024_01": "01_2024", "clinvar.2025_06": "06_2025"},
        ),
    ],
)
def test_plan_csv_columns(namespaces, kwargs, expected_ns_keys, expected_score_cols, expected_clinvar):
    plan = plan_csv_columns(SAMPLE_DATASET_COLUMNS, namespaces, **kwargs)

    assert set(plan.namespaced_columns.keys()) == expected_ns_keys
    assert plan.clinvar_namespaces == expected_clinvar

    if expected_score_cols is not None:
        assert plan.namespaced_columns["scores"] == expected_score_cols

    # core always has the standard 4 columns
    assert plan.namespaced_columns["core"] == ["accession", "hgvs_nt", "hgvs_splice", "hgvs_pro"]

    # vep, gnomad, clingen get their fixed columns when present
    if "vep" in plan.namespaced_columns:
        assert plan.namespaced_columns["vep"] == ["vep_functional_consequence"]
    if "gnomad" in plan.namespaced_columns:
        assert plan.namespaced_columns["gnomad"] == ["gnomad_af"]
    if "clingen" in plan.namespaced_columns:
        assert plan.namespaced_columns["clingen"] == ["clingen_allele_id"]

    # ClinVar namespaces get their standard columns
    for ns in expected_clinvar:
        assert plan.namespaced_columns[ns] == ["clinical_significance", "clinical_review_status"]


def test_plan_csv_columns_post_mapped_hgvs_populates_mavedb():
    plan = plan_csv_columns(SAMPLE_DATASET_COLUMNS, ["scores"], include_post_mapped_hgvs=True)
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

        new_rows, new_cols = drop_na_columns_from_csv_file_rows(rows, columns)

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

        new_rows, new_cols = drop_na_columns_from_csv_file_rows(rows, columns)

        assert new_cols == ["hgvs_nt", "hgvs_splice", "hgvs_pro"]

    def test_does_not_touch_non_hgvs_columns(self):
        rows = [
            {"hgvs_nt": "g.1A>G", "hgvs_splice": "NA", "hgvs_pro": "NA", "score": "NA"},
        ]
        columns = ["hgvs_nt", "hgvs_splice", "hgvs_pro", "score"]

        new_rows, new_cols = drop_na_columns_from_csv_file_rows(rows, columns)

        assert "score" in new_cols
        assert "hgvs_splice" not in new_cols

    def test_empty_rows_does_not_crash(self):
        rows = []
        columns = ["hgvs_nt", "hgvs_splice", "hgvs_pro"]

        new_rows, new_cols = drop_na_columns_from_csv_file_rows(rows, columns)

        assert new_rows == []
        assert new_cols == []
