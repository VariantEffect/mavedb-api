"""Query-plan assertions for reads that must stay O(variants) regardless of planner statistics.

A score-set read that joins a large per-variant table can flip from per-row index lookups to a hash join
over the whole table once the planner's row estimate crosses its cost threshold. These helpers EXPLAIN a
captured statement with nested loops and sequential scans disabled, so any join the planner *could* turn
into a whole-table read does, and report scans of the given tables that are not keyed on the outer row.
"""

from contextlib import contextmanager
from typing import Iterator, Mapping

from sqlalchemy import event, text


@contextmanager
def captured_statements(session) -> Iterator[list[tuple[str, object]]]:
    """Collect every ``(statement, parameters)`` the session's engine sends while the block runs."""
    captured: list[tuple[str, object]] = []
    engine = session.get_bind()

    def capture(_conn, _cursor, statement, parameters, _context, _executemany):
        captured.append((statement, parameters))

    event.listen(engine, "before_cursor_execute", capture)
    try:
        yield captured
    finally:
        event.remove(engine, "before_cursor_execute", capture)


def unkeyed_scans(session, statement: str, parameters, keys: Mapping[str, str]) -> list[str]:
    """Scans of the tables in ``keys`` whose condition does not constrain the mapped key column.

    ``keys`` maps a table name to the column that must appear as ``<column> =`` in the scan's index or
    recheck condition, i.e. the scan is a lookup driven by the outer row rather than a read of the table.
    Runs inside the caller's transaction; the ``SET LOCAL``s end with it.
    """
    session.execute(text("SET LOCAL enable_nestloop = off"))
    session.execute(text("SET LOCAL enable_seqscan = off"))
    cursor = session.connection().connection.cursor()
    cursor.execute("EXPLAIN (FORMAT JSON) " + statement, parameters)
    plan = cursor.fetchone()[0][0]["Plan"]

    def walk(node):
        yield node
        for child in node.get("Plans", []):
            yield from walk(child)

    bad = []
    for node in walk(plan):
        table = node.get("Relation Name")
        if table not in keys:
            continue
        condition = node.get("Index Cond", "") + node.get("Recheck Cond", "")
        if f"{keys[table]} =" not in condition:
            bad.append(f"{node['Node Type']} on {table} ({condition or 'no condition'})")
    return bad
