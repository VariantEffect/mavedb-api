"""Tests for bin/check-migrations, the CI guard that keeps the migration history replayable."""

import importlib.util
from importlib.machinery import SourceFileLoader
from pathlib import Path

import pytest

# bin/check-migrations has no .py suffix, so it needs an explicit loader.
_loader = SourceFileLoader("check_migrations", str(Path(__file__).resolve().parents[2] / "bin" / "check-migrations"))
_spec = importlib.util.spec_from_loader(_loader.name, _loader)
assert _spec is not None
check_migrations = importlib.util.module_from_spec(_spec)
_loader.exec_module(check_migrations)


def problems_in(tmp_path: Path, source: str) -> list[str]:
    migration = tmp_path / "abc123_example.py"
    migration.write_text(source)
    return check_migrations.check(migration)


@pytest.mark.unit
@pytest.mark.parametrize(
    "source, reason",
    [
        ("from mavedb.models.variant import Variant\n", "imports application code"),
        ("import mavedb.lib.urns\n", "imports application code"),
        ("from sqlalchemy.orm import Session\n", "uses the ORM"),
        ("import requests\n", "makes network calls"),
        ("def upgrade():\n    with op.get_context().autocommit_block():\n        pass\n", "autocommit_block"),
        ("def upgrade():\n    op.execute('CREATE INDEX concurrently ix ON t (c)')\n", "CONCURRENTLY"),
        ("def upgrade():\n    op.get_bind().commit()\n", "commits mid-upgrade"),
        ("def upgrade():\n    op.execute(\"ALTER TYPE state ADD  value 'done'\")\n", "adds an enum value"),
    ],
    ids=[
        "mavedb-from-import",
        "mavedb-import",
        "orm",
        "network",
        "autocommit-block",
        "concurrently",
        "commit",
        "enum-add-value",
    ],
)
def test_check_forbidden_pattern_reports_one_problem(tmp_path, source, reason):
    problems = problems_in(tmp_path, source)

    assert len(problems) == 1
    assert reason in problems[0]


@pytest.mark.unit
def test_check_concurrently_in_docstrings_passes(tmp_path):
    source = (
        '"""Build by hand first: CREATE INDEX CONCURRENTLY ix ON t (c)."""\n'
        "def upgrade():\n"
        '    """Then CONCURRENTLY is skipped here."""\n'
        "    op.execute('CREATE INDEX IF NOT EXISTS ix ON t (c)')\n"
    )

    assert problems_in(tmp_path, source) == []


@pytest.mark.unit
def test_check_alembic_and_sqlalchemy_core_imports_pass(tmp_path):
    source = "import sqlalchemy as sa\nfrom alembic import op\nfrom sqlalchemy.dialects import postgresql\n"

    assert problems_in(tmp_path, source) == []


@pytest.mark.unit
def test_main_current_migrations_pass():
    assert check_migrations.main([]) == 0
