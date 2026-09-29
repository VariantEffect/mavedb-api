# ruff: noqa: E402

from types import SimpleNamespace

import pytest

pytest.importorskip("psycopg2")

from sqlalchemy import event, text

from mavedb.lib.csv.columns import plan_csv_columns
from mavedb.lib.csv.fetch import fetch_variant_csv_data

VARIANT_NUMBER_INDEX = "ix_variants_scoreset_number"


def _captured_variant_query(session, namespaces, start=None, limit=None):
    """Run the score set CSV fetch and return the statement and parameters it sent for the variant rows."""
    captured = []
    engine = session.get_bind()

    def capture(_conn, _cursor, statement, parameters, _context, _executemany):
        if "FROM variants" in statement and not captured:
            captured.append((statement, parameters))

    event.listen(engine, "before_cursor_execute", capture)
    try:
        plan = plan_csv_columns({"score_columns": ["score"]}, namespaces)
        fetch_variant_csv_data(
            session,
            plan.namespaced_columns,
            plan.clinvar_namespaces,
            score_set=SimpleNamespace(id=1),
            start=start,
            limit=limit,
        )
    finally:
        event.remove(engine, "before_cursor_execute", capture)

    return captured[0]


@pytest.mark.unit
class TestVariantNumberOrdering:
    @pytest.mark.parametrize("namespaces", [["scores"], ["scores", "mavedb"]], ids=["scores", "with_mappings"])
    def test_score_set_query_is_served_by_the_variant_number_index(self, session, namespaces):
        statement, parameters = _captured_variant_query(session, namespaces, start=250000, limit=50000)

        # Rule out the alternatives so the plan shows whether the index can serve the ORDER BY at all.
        session.execute(text("SET LOCAL enable_seqscan = off"))
        session.execute(text("SET LOCAL enable_sort = off"))
        cursor = session.connection().connection.cursor()
        cursor.execute("EXPLAIN " + statement, parameters)
        plan = "\n".join(row[0] for row in cursor.fetchall())

        assert VARIANT_NUMBER_INDEX in plan
        assert "Sort" not in plan
