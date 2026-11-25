# ruff: noqa: E402

import pytest

pytest.importorskip("psycopg2")

from sqlalchemy import insert, text

from mavedb.lib.csv.columns import CsvPlan, _is_output_null
from mavedb.lib.csv.fetch import ScoreSetScope, iter_csv_rows, unused_hgvs_columns
from mavedb.lib.mave.utils import NULL_VALUES
from mavedb.models.variant import Variant
from tests.helpers.util.query_plan import captured_statements

VARIANT_NUMBER_INDEX = "ix_variants_scoreset_number_covering"


def _plan_nodes(node, ancestors=()):
    """Every node of an ``EXPLAIN (FORMAT JSON)`` plan, with the node types above it."""
    yield node, ancestors
    for child in node.get("Plans", []):
        yield from _plan_nodes(child, (*ancestors, node["Node Type"]))


def _conditions(node) -> str:
    return " ".join(str(node.get(key, "")) for key in ("Index Cond", "Filter", "Recheck Cond"))


def _captured_variant_query(session, namespaces, start=None, limit=None):
    """Run the score set CSV fetch and return the statement and parameters it sent for the variant rows."""
    plan = CsvPlan.build({"score_columns": ["score"]}, namespaces)
    with captured_statements(session) as captured:
        list(iter_csv_rows(session, plan, ScoreSetScope(score_set_id=1, start=start, limit=limit)))

    return next((statement, parameters) for statement, parameters in captured if "FROM variants" in statement)


@pytest.mark.unit
class TestVariantNumberOrdering:
    @pytest.mark.parametrize("namespaces", [["scores"], ["scores", "mavedb"]], ids=["scores", "with_mappings"])
    def test_score_set_query_is_served_by_the_variant_number_index(self, session, namespaces):
        statement, parameters = _captured_variant_query(session, namespaces)

        # Rule out the alternatives so the plan shows whether the index can serve the ORDER BY at all.
        session.execute(text("SET LOCAL enable_seqscan = off"))
        session.execute(text("SET LOCAL enable_sort = off"))
        cursor = session.connection().connection.cursor()
        cursor.execute("EXPLAIN " + statement, parameters)
        plan = "\n".join(row[0] for row in cursor.fetchall())

        assert VARIANT_NUMBER_INDEX in plan
        assert "Sort" not in plan

    @pytest.mark.parametrize("namespaces", [["scores"], ["scores", "mavedb"]], ids=["scores", "with_mappings"])
    def test_iter_csv_rows_score_set_page_scans_the_score_set_only_for_its_ids(self, session, namespaces):
        statement, parameters = _captured_variant_query(session, namespaces, start=250000, limit=50000)

        # Rule out the alternatives, so a query that still lets the planner walk the score set for its full
        # rows, or sort the whole score set, gets that plan.
        session.execute(text("SET LOCAL enable_seqscan = off"))
        session.execute(text("SET LOCAL enable_bitmapscan = off"))
        session.execute(text("SET LOCAL enable_sort = off"))
        cursor = session.connection().connection.cursor()
        cursor.execute("EXPLAIN (FORMAT JSON) " + statement, parameters)
        nodes = list(_plan_nodes(cursor.fetchone()[0][0]["Plan"]))

        assert any(
            node["Node Type"] == "Index Only Scan" and node.get("Index Name") == VARIANT_NUMBER_INDEX
            for node, _ in nodes
        )
        # Only the page's id selection may scan the score set; everything else is driven by the page's ids.
        score_set_scans = [
            (node, ancestors)
            for node, ancestors in nodes
            if node.get("Relation Name") == "variants" and "scoreset_id" in _conditions(node)
        ]
        assert score_set_scans
        for node, ancestors in score_set_scans:
            assert "Limit" in ancestors, f"{node['Node Type']} over the whole score set outside the page's LIMIT"


# Variants numbered past 9 so variant-number order differs from URN order, with HGVS that exercises the null
# tokens: `hgvs_splice` is null on every row, `hgvs_pro` on all but one.
_VARIANTS = [
    (1, "c.1A>T", "NA", None),
    (2, "c.2A>T", "NA", None),
    (10, "c.10A>T", " na ", "NULL"),
    (3, "c.3A>T", None, ""),
    (11, "c.11A>T", "p.Thr11Ser", "n/a"),
    (4, "c.4A>T", "None", "  "),
]


def _add_variants(db, score_set, variants):
    db.execute(
        insert(Variant.__table__),
        [
            {
                "urn": f"{score_set.urn}#{number}",
                "scoreset_id": score_set.id,
                "data": {"score_data": {"score": number / 10}, "count_data": {}},
                "hgvs_nt": hgvs_nt,
                "hgvs_pro": hgvs_pro,
                "hgvs_splice": hgvs_splice,
            }
            for number, hgvs_nt, hgvs_pro, hgvs_splice in variants
        ],
    )
    db.commit()


@pytest.mark.parametrize(
    "start,limit,expected",
    [
        (None, None, {"hgvs_splice"}),
        # Only `#11` has a protein HGVS, so a page without it drops the column.
        (1, 3, {"hgvs_pro", "hgvs_splice"}),
        (100, None, {"hgvs_nt", "hgvs_pro", "hgvs_splice"}),
    ],
    ids=["whole", "page_without_protein", "empty_page"],
)
def test_unused_hgvs_columns_judges_only_the_requested_rows(
    session, setup_lib_db_with_score_set, start, limit, expected
):
    score_set = setup_lib_db_with_score_set
    _add_variants(session, score_set, _VARIANTS)

    assert unused_hgvs_columns(session, ScoreSetScope(score_set.id, start=start, limit=limit)) == expected


_NULL_TOKEN_SPELLINGS = sorted(
    {
        spelling
        for token in NULL_VALUES
        for spelling in (token, token.upper(), token.title(), f" {token}\t", f"\n{token} ")
    }
)


@pytest.mark.parametrize("value", [*_NULL_TOKEN_SPELLINGS, "c.1A>T", "nana", "N", "-", "0"])
def test_unused_hgvs_columns_agrees_with_the_rendered_null_check(session, setup_lib_db_with_score_set, value):
    """The database check stands in for rendering every row, so it must call a value null exactly when the
    renderer would write it as NA."""
    score_set = setup_lib_db_with_score_set
    _add_variants(session, score_set, [(1, value, None, None)])

    unused = unused_hgvs_columns(session, ScoreSetScope(score_set.id))

    assert ("hgvs_nt" in unused) == _is_output_null(value)
