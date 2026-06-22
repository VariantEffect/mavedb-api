"""Unit tests for the pure allele-grouping primitive shared by the annotation jobs."""

from mavedb.lib.clingen.alleles import ScoreSetAlleleRow, group_alleles_for_annotation


def _row(allele_id, variant_id, *, is_authoritative, caid="CA1"):
    return ScoreSetAlleleRow(
        allele_id=allele_id,
        post_mapped={"type": "Allele"},
        clingen_allele_id=caid,
        variant_id=variant_id,
        is_authoritative=is_authoritative,
    )


def test_collapses_rows_to_one_group_per_allele():
    rows = [
        _row(1, 10, is_authoritative=True),
        _row(1, 11, is_authoritative=True),
        _row(2, 12, is_authoritative=True, caid="CA2"),
    ]

    groups = group_alleles_for_annotation(rows, payload=lambda r: r.clingen_allele_id)

    assert set(groups) == {1, 2}
    assert groups[1].payload == "CA1"
    assert groups[1].authoritative_variant_ids == [10, 11]
    assert groups[2].authoritative_variant_ids == [12]


def test_only_authoritative_links_contribute_variant_ids():
    """An allele that is authoritative for one variant and RT-derived for another fans status only to
    the authoritative variant — the bandaid invariant."""
    rows = [
        _row(1, 10, is_authoritative=True),
        _row(1, 11, is_authoritative=False),
    ]

    groups = group_alleles_for_annotation(rows, payload=lambda r: r.clingen_allele_id)

    assert groups[1].authoritative_variant_ids == [10]


def test_purely_rt_derived_allele_is_grouped_with_empty_fan_out():
    """A non-authoritative-only allele is still grouped (so it gets linked/annotated at the allele
    level) but contributes no per-variant VAS row."""
    rows = [_row(1, 10, is_authoritative=False)]

    groups = group_alleles_for_annotation(rows, payload=lambda r: r.clingen_allele_id)

    assert set(groups) == {1}
    assert groups[1].authoritative_variant_ids == []


def test_payload_returning_none_skips_the_allele():
    """Returning None from payload drops the allele entirely — replacing each job's ad-hoc
    'no CAID / no HGVS -> continue' filter."""
    rows = [
        _row(1, 10, is_authoritative=True, caid=None),
        _row(2, 11, is_authoritative=True, caid="CA2"),
    ]

    groups = group_alleles_for_annotation(rows, payload=lambda r: r.clingen_allele_id)

    assert set(groups) == {2}


def test_empty_rows_yield_empty_grouping():
    assert group_alleles_for_annotation([], payload=lambda r: r.clingen_allele_id) == {}
