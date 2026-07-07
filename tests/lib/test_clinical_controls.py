"""Unit tests for the clinical-controls lib helpers (``lib/clinical_controls.py``).

The live-link query paths are exercised end-to-end by the score-set router tests; these pin the pure
``clinvar_version_sort_key``, which is the ``MM_YYYY`` release-ordering contract the UI parses in
parallel (``lib/clinical-controls.ts``) and so must stay stable.
"""

from mavedb.lib.clinical_controls import clinvar_version_sort_key


def test_version_sort_key_orders_by_year_then_month():
    versions = ["01_2024", "12_2023", "06_2024", "03_2025"]
    assert sorted(versions, key=clinvar_version_sort_key, reverse=True) == [
        "03_2025",
        "06_2024",
        "01_2024",
        "12_2023",
    ]


def test_version_sort_key_parses_month_and_year():
    # (year, month) — a later month within the same year sorts higher.
    assert clinvar_version_sort_key("06_2024") == (2024, 6)
    assert clinvar_version_sort_key("11_2024") > clinvar_version_sort_key("06_2024")


def test_version_sort_key_unparseable_sorts_to_bottom():
    # Malformed versions collapse to (0, 0) rather than raising, so they sink under any real release.
    assert clinvar_version_sort_key("garbage") == (0, 0)
    assert clinvar_version_sort_key("2024") == (0, 0)
    assert clinvar_version_sort_key("13_20_2024") == (0, 0)
    assert clinvar_version_sort_key("01_2020") > clinvar_version_sort_key("garbage")
