"""Unit tests for the pure allele-grouping primitive shared by the annotation jobs."""

from mavedb.lib.clingen.alleles import ScoreSetAlleleRow, group_alleles_for_annotation


def _row(allele_id, variant_id, *, caid="CA1"):
    return ScoreSetAlleleRow(
        allele_id=allele_id,
        post_mapped={"type": "Allele"},
        clingen_allele_id=caid,
        variant_id=variant_id,
    )


def test_collapses_rows_to_one_payload_per_allele():
    rows = [
        _row(1, 10),
        _row(1, 11),
        _row(2, 12, caid="CA2"),
    ]

    groups = group_alleles_for_annotation(rows, payload=lambda r: r.clingen_allele_id)

    assert groups == {1: "CA1", 2: "CA2"}


def test_shared_allele_yields_a_single_entry():
    """An allele linked by multiple variants is deduped to one work-unit (events are allele-keyed)."""
    rows = [
        _row(1, 10),
        _row(1, 11),
    ]

    groups = group_alleles_for_annotation(rows, payload=lambda r: r.clingen_allele_id)

    assert groups == {1: "CA1"}


def test_payload_returning_none_skips_the_allele():
    """Returning None from payload drops the allele entirely — replacing each job's ad-hoc
    'no CAID / no HGVS -> continue' filter."""
    rows = [
        _row(1, 10, caid=None),
        _row(2, 11, caid="CA2"),
    ]

    groups = group_alleles_for_annotation(rows, payload=lambda r: r.clingen_allele_id)

    assert groups == {2: "CA2"}


def test_empty_rows_yield_empty_grouping():
    assert group_alleles_for_annotation([], payload=lambda r: r.clingen_allele_id) == {}
