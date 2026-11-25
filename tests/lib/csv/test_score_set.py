# ruff: noqa: E402
"""Streaming the score set CSV. What the bytes are is the golden snapshots' job; these check that batching
never changes them, and how the stream is chunked."""

import csv
import io
from types import SimpleNamespace

import pytest

pytest.importorskip("psycopg2")

from sqlalchemy import select

from mavedb.lib.csv.score_set import get_score_set_variants_as_csv, stream_score_set_variants_as_csv
from mavedb.models.variant import Variant
from tests.helpers.util.annotation import add_clinvar_control
from tests.helpers.util.csv_export import CALIBRATION_NS, EXTRA_VARIANTS
from tests.helpers.util.query_plan import captured_statements

_NAMESPACE_SETS = {
    "scores": ["scores", "scores_custom"],
    "counts": ["counts"],
    "wide": ["scores", "scores_custom", "mavedb", "vep", "gnomad", "clingen", "score_set", "clinvar.2024_01"],
    "calibration": ["scores", CALIBRATION_NS],
}

_ROWS = 1 + len(EXTRA_VARIANTS)


@pytest.mark.parametrize("namespaces", list(_NAMESPACE_SETS.values()), ids=list(_NAMESPACE_SETS))
@pytest.mark.parametrize("drop_unused", [False, True], ids=["all_hgvs", "drop_unused_hgvs"])
@pytest.mark.parametrize(
    "start,limit", [(None, None), (1, 3), (2, None), (None, 2)], ids=["whole", "page", "from_start", "first_rows"]
)
@pytest.mark.parametrize("batch_size", [1, 2], ids=["batch_1", "batch_2"])
def test_stream_score_set_variants_as_csv_batching_does_not_change_the_bytes(
    session, csv_export_score_set, namespaces, drop_unused, start, limit, batch_size
):
    def stream(size):
        return "".join(
            stream_score_set_variants_as_csv(
                session,
                csv_export_score_set,
                namespaces,
                namespaced=True,
                start=start,
                limit=limit,
                drop_unused_hgvs_columns_flag=drop_unused,
                batch_size=size,
            )
        )

    assert stream(batch_size) == stream(_ROWS)


def test_get_score_set_variants_as_csv_is_the_whole_stream(session, csv_export_score_set):
    streamed = "".join(stream_score_set_variants_as_csv(session, csv_export_score_set, ["scores"], batch_size=2))

    assert get_score_set_variants_as_csv(session, csv_export_score_set, ["scores"]) == streamed


def test_stream_score_set_variants_as_csv_one_chunk_per_batch_with_the_header_first(session, csv_export_score_set):
    chunks = list(stream_score_set_variants_as_csv(session, csv_export_score_set, ["scores"], batch_size=2))

    assert len(chunks) == -(-_ROWS // 2)
    assert chunks[0].startswith("accession,hgvs_nt,hgvs_splice,hgvs_pro,score\r\n")
    assert all(not chunk.startswith("accession") for chunk in chunks[1:])
    accessions = [row["accession"] for row in csv.DictReader(io.StringIO("".join(chunks)))]
    assert accessions == [f"{csv_export_score_set.urn}#{n}" for n in (1, 2, 3, 4, 5, 10, 11, 12)]


def test_stream_score_set_variants_as_csv_empty_page_is_the_header_alone(session, csv_export_score_set):
    chunks = list(stream_score_set_variants_as_csv(session, csv_export_score_set, ["scores"], start=100))

    assert chunks == ["accession,hgvs_nt,hgvs_splice,hgvs_pro,score\r\n"]


def test_stream_score_set_variants_as_csv_drops_an_all_na_hgvs_column_and_keeps_a_sparse_one(
    session, csv_export_score_set
):
    header = next(
        stream_score_set_variants_as_csv(session, csv_export_score_set, ["scores"], drop_unused_hgvs_columns_flag=True)
    ).splitlines()[0]

    # `hgvs_splice` is a null token on every row; `hgvs_pro` has one real value.
    assert header.split(",") == ["accession", "hgvs_nt", "hgvs_pro", "score"]


def test_stream_score_set_variants_as_csv_reads_every_clinvar_release_in_one_query(session, csv_export_score_set):
    first = session.scalars(select(Variant).where(Variant.urn == f"{csv_export_score_set.urn}#1")).one()
    add_clinvar_control(
        session, SimpleNamespace(variant_id=first.id), "Uncertain significance", "practice guideline", "01_2026"
    )
    releases = ["clinvar.2024_01", "clinvar.2025_01", "clinvar.2026_01", "clinvar.2027_01"]

    with captured_statements(session) as captured:
        text = "".join(stream_score_set_variants_as_csv(session, csv_export_score_set, releases, batch_size=_ROWS))

    assert len([statement for statement, _ in captured if "FROM clinvar_allele_links" in statement]) == 1
    row = next(row for row in csv.DictReader(io.StringIO(text)) if row["accession"] == first.urn)
    assert row["clinvar.2024_01.clinical_significance"] == "Pathogenic"
    assert row["clinvar.2025_01.clinical_significance"] == "Likely benign"
    assert row["clinvar.2026_01.clinical_significance"] == "Uncertain significance"
    assert row["clinvar.2027_01.clinical_significance"] == "NA"
