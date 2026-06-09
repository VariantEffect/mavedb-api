import pytest

from mavedb.lib.hgvs import (
    extract_accession,
    join_cis_phased_hgvs,
    split_cis_phased_hgvs,
    strip_protein_prediction_parens,
)


@pytest.mark.parametrize(
    ("hgvs", "expected"),
    [
        ("NP_000456.2:p.(Ala222Val)", "NP_000456.2:p.Ala222Val"),
        ("NP_000456.2:p.(Tyr745Ter)", "NP_000456.2:p.Tyr745Ter"),
        ("NP_000456.2:p.(Ala222_Val225del)", "NP_000456.2:p.Ala222_Val225del"),
        # no prediction parens -> unchanged
        ("NP_000456.2:p.Ala222Val", "NP_000456.2:p.Ala222Val"),
        ("NM_000001.1:c.5A>G", "NM_000001.1:c.5A>G"),
    ],
)
def test_strip_protein_prediction_parens(hgvs, expected):
    assert strip_protein_prediction_parens(hgvs) == expected


@pytest.mark.parametrize(
    ("hgvs", "expected"),
    [
        ("NC_000001.11:g.1000A>G", "NC_000001.11"),
        ("NM_000001.1:c.5A>G", "NM_000001.1"),
        ("NC_000001.11:g.[1000A>G;1002T>C]", "NC_000001.11"),
        ("no-colon-here", ""),
        ("", ""),
    ],
)
def test_extract_accession(hgvs, expected):
    assert extract_accession(hgvs) == expected


def test_split_cis_phased_hgvs_passes_through_single_variant():
    assert split_cis_phased_hgvs("NC_000001.11:g.1000A>G") == ["NC_000001.11:g.1000A>G"]


def test_split_cis_phased_hgvs_splits_bracketed_genomic_expression():
    # Non-adjacent codon components are emitted as one bracketed genomic expression; each
    # component must regain the accession and prefix to be VRS-translatable on its own.
    assert split_cis_phased_hgvs("NC_000001.11:g.[1000A>G;1002T>C]") == [
        "NC_000001.11:g.1000A>G",
        "NC_000001.11:g.1002T>C",
    ]


def test_split_cis_phased_hgvs_handles_three_components_and_coding_prefix():
    assert split_cis_phased_hgvs("NM_000001.1:c.[1A>G;3T>C;5G>A]") == [
        "NM_000001.1:c.1A>G",
        "NM_000001.1:c.3T>C",
        "NM_000001.1:c.5G>A",
    ]


def test_join_cis_phased_hgvs_passes_through_single_component():
    assert join_cis_phased_hgvs(["NC_000001.11:g.1000A>G"]) == "NC_000001.11:g.1000A>G"


def test_join_cis_phased_hgvs_combines_genomic_components():
    assert (
        join_cis_phased_hgvs(["NC_000001.11:g.1000A>G", "NC_000001.11:g.1002T>C"]) == "NC_000001.11:g.[1000A>G;1002T>C]"
    )


@pytest.mark.parametrize(
    "expression",
    [
        "NC_000001.11:g.[1000A>G;1002T>C]",
        "NM_000001.1:c.[1A>G;3T>C;5G>A]",
    ],
)
def test_join_is_inverse_of_split(expression):
    assert join_cis_phased_hgvs(split_cis_phased_hgvs(expression)) == expression


def test_join_cis_phased_hgvs_returns_none_for_empty():
    assert join_cis_phased_hgvs([]) is None


def test_join_cis_phased_hgvs_returns_none_for_mixed_accessions():
    # Members on different sequences are not one cis-phased block.
    assert join_cis_phased_hgvs(["NC_000001.11:g.1A>G", "NC_000002.12:g.2T>C"]) is None


def test_join_cis_phased_hgvs_returns_none_for_mixed_coordinate_prefixes():
    assert join_cis_phased_hgvs(["NM_000001.1:c.1A>G", "NM_000001.1:g.2T>C"]) is None


def test_join_cis_phased_hgvs_returns_none_for_component_without_accession():
    assert join_cis_phased_hgvs(["g.1A>G", "g.2T>C"]) is None
