# ruff: noqa: E402
"""Byte-for-byte snapshots of both CSV exports, so a change to how they're built can't change what they emit.

After a deliberate output change, rewrite the snapshots with ``MAVEDB_UPDATE_GOLDEN=1`` and review their diff.
``test_coverage`` fails until every export, parameter and namespace has a snapshot here or a test elsewhere.
"""

import os
from pathlib import Path

import pytest

pytest.importorskip("psycopg2")

from mavedb.lib.csv.score_set import get_score_set_variants_as_csv
from mavedb.lib.csv.variant import get_variant_csv
from tests.helpers.util.csv_export import (
    BENIGN_ABSENT_VARIANT,
    CALIBRATION_NS,
    FAILED_VARIANT,
    RESEARCH_CALIBRATION_NS,
    RETIRED_CALIBRATION_NS,
)

GOLDEN_DIR = Path(__file__).parent / "golden"

_MAPPING_NAMESPACES = ["mavedb", "vep", "gnomad", "clingen", "clinvar.2024_01", "clinvar.2025_01"]
_CALIBRATION_NAMESPACES = [CALIBRATION_NS, RESEARCH_CALIBRATION_NS, RETIRED_CALIBRATION_NS]

SCORE_SET_NAMESPACES = {
    "core": [],
    "scores": ["scores", "scores_custom"],
    "counts": ["counts"],
    "wide": ["scores", "scores_custom", "counts", *_MAPPING_NAMESPACES, "score_set", *_CALIBRATION_NAMESPACES],
}

VARIANT_NAMESPACES = {
    "default": None,
    "wide": ["scores", "score_set", "relationship", *_MAPPING_NAMESPACES, *_CALIBRATION_NAMESPACES],
}


def _assert_matches_golden(name: str, text: str) -> None:
    path = GOLDEN_DIR / f"{name}.csv"
    if os.environ.get("MAVEDB_UPDATE_GOLDEN"):
        GOLDEN_DIR.mkdir(exist_ok=True)
        path.write_bytes(text.encode())

    assert path.exists(), f"no snapshot at {path}; run with MAVEDB_UPDATE_GOLDEN=1 to write it"
    assert text == path.read_bytes().decode()


@pytest.mark.parametrize("start,limit", [(None, None), (1, 3)], ids=["whole", "page"])
@pytest.mark.parametrize("drop_unused", [False, True], ids=["all_hgvs", "drop_unused_hgvs"])
@pytest.mark.parametrize("namespaced", [False, True], ids=["bare", "namespaced"])
@pytest.mark.parametrize("namespaces", list(SCORE_SET_NAMESPACES), ids=list(SCORE_SET_NAMESPACES))
def test_get_score_set_variants_as_csv_matches_its_snapshot(
    session, csv_export_score_set, namespaces, namespaced, drop_unused, start, limit
):
    if namespaces == "wide" and not namespaced:
        pytest.skip("no caller asks for the wide set without namespaces")

    text = get_score_set_variants_as_csv(
        session,
        csv_export_score_set,
        SCORE_SET_NAMESPACES[namespaces],
        namespaced=namespaced,
        start=start,
        limit=limit,
        drop_unused_hgvs_columns_flag=drop_unused,
    )

    page = "whole" if start is None else "page"
    hgvs = "drop_unused_hgvs" if drop_unused else "all_hgvs"
    _assert_matches_golden(f"score_set-{namespaces}-{'namespaced' if namespaced else 'bare'}-{hgvs}-{page}", text)


def test_get_score_set_variants_as_csv_as_of_matches_its_snapshot(session, csv_export):
    text = get_score_set_variants_as_csv(
        session,
        csv_export.score_set,
        SCORE_SET_NAMESPACES["wide"],
        namespaced=True,
        as_of=csv_export.before_update,
    )

    _assert_matches_golden("score_set-wide-namespaced-all_hgvs-whole-as_of", text)


_VARIANT_CASES = {"mapped": 1, "unmapped": 2, "benign_absent": BENIGN_ABSENT_VARIANT, "failed": FAILED_VARIANT}


@pytest.mark.parametrize("namespaces", list(VARIANT_NAMESPACES), ids=list(VARIANT_NAMESPACES))
@pytest.mark.parametrize("case", list(_VARIANT_CASES), ids=list(_VARIANT_CASES))
def test_get_variant_csv_matches_its_snapshot(session, csv_export_score_set, case, namespaces):
    urn = f"{csv_export_score_set.urn}#{_VARIANT_CASES[case]}"
    text = get_variant_csv(session, urn, namespaces=VARIANT_NAMESPACES[namespaces])

    _assert_matches_golden(f"variant-{case}-{namespaces}", text)


def test_get_variant_csv_as_of_matches_its_snapshot(session, csv_export):
    urn = f"{csv_export.score_set.urn}#1"
    text = get_variant_csv(session, urn, namespaces=VARIANT_NAMESPACES["wide"], as_of=csv_export.before_update)

    _assert_matches_golden("variant-mapped-wide-as_of", text)
