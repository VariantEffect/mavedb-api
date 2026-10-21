# ruff: noqa: E402

from types import SimpleNamespace

import pytest

pytest.importorskip("psycopg2")

from sqlalchemy import text

from mavedb.lib.csv.columns import plan_csv_columns
from mavedb.lib.csv.fetch import fetch_variant_csv_data
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
    plan = plan_csv_columns({"score_columns": ["score"]}, namespaces)
    with captured_statements(session) as captured:
        fetch_variant_csv_data(
            session,
            plan.namespaced_columns,
            plan.clinvar_namespaces,
            score_set=SimpleNamespace(id=1),
            start=start,
            limit=limit,
        )

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
    def test_fetch_variant_csv_data_score_set_page_scans_the_score_set_only_for_its_ids(self, session, namespaces):
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

    @pytest.mark.parametrize("start,limit", [(1, 1), (0, None), (None, 0)], ids=["page", "zero_start", "zero_limit"])
    def test_fetch_variant_csv_data_paged_variant_set_raises(self, session, start, limit):
        plan = plan_csv_columns({"score_columns": ["score"]}, ["scores"])
        with pytest.raises(ValueError, match="start and limit"):
            fetch_variant_csv_data(
                session, plan.namespaced_columns, plan.clinvar_namespaces, variant_ids=[1], start=start, limit=limit
            )
