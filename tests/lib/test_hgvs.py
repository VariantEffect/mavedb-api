import pytest

from mavedb.lib.hgvs import (
    SequenceBlock,
    extract_accession,
    join_cis_phased_hgvs,
    parse_simple_nucleotide_substitution,
    parse_simple_protein_substitution,
    parse_simple_substitution,
    split_cis_phased_hgvs,
    strip_protein_prediction_parens,
)


@pytest.mark.unit
@pytest.mark.parametrize(
    ("hgvs", "expected"),
    [
        # auto-detects the level from the disjoint coordinate prefix
        ("NM_000546.6:c.1216G>A", SequenceBlock(position=1216, ref="G", alt="A")),
        ("NP_000537.3:p.Ala406Thr", SequenceBlock(position=406, ref="Ala", alt="Thr")),
        ("g.1000A>G", SequenceBlock(position=1000, ref="A", alt="G")),
        # non-placeable in either grammar
        ("c.122-6T>A", None),
        ("c.[197A>G;472T>C]", None),
        (None, None),
    ],
)
def test_parse_simple_substitution(hgvs, expected):
    assert parse_simple_substitution(hgvs) == expected


@pytest.mark.unit
@pytest.mark.parametrize(
    ("hgvs", "expected"),
    [
        # accession-qualified and bare coding substitutions
        ("NM_000546.6:c.1216G>A", SequenceBlock(position=1216, ref="G", alt="A")),
        ("c.5A>G", SequenceBlock(position=5, ref="A", alt="G")),
        # genomic and non-coding levels parse the same way
        ("NC_000001.11:g.1000A>G", SequenceBlock(position=1000, ref="A", alt="G")),
        ("n.42C>T", SequenceBlock(position=42, ref="C", alt="T")),
        # lowercase nucleotides are tolerated
        ("c.5a>g", SequenceBlock(position=5, ref="a", alt="g")),
        # non-placeable: UTR/intron positions, multivariant, indels, non-substitutions, empty
        ("c.*123A>G", None),
        ("c.-12A>G", None),
        ("c.12+3A>G", None),
        ("NM_000546.6:c.[197A>G;472T>C]", None),
        ("c.76_78del", None),
        ("p.Ala406Thr", None),
        ("", None),
        (None, None),
    ],
)
def test_parse_simple_nucleotide_substitution(hgvs, expected):
    assert parse_simple_nucleotide_substitution(hgvs) == expected


@pytest.mark.unit
@pytest.mark.parametrize(
    ("hgvs", "expected"),
    [
        # accession-qualified, bare, and prediction-wrapped substitutions
        ("NP_000537.3:p.Ala406Thr", SequenceBlock(position=406, ref="Ala", alt="Thr")),
        ("p.Ala406Thr", SequenceBlock(position=406, ref="Ala", alt="Thr")),
        ("NP_000537.3:p.(Ala406Thr)", SequenceBlock(position=406, ref="Ala", alt="Thr")),
        # synonymous, stop, and deletion tokens are preserved as the raw alt
        ("p.Ala406=", SequenceBlock(position=406, ref="Ala", alt="=")),
        ("p.Tyr745*", SequenceBlock(position=745, ref="Tyr", alt="*")),
        ("p.Ala406-", SequenceBlock(position=406, ref="Ala", alt="-")),
        # non-placeable: multivariant, frameshift, single-letter codes, empty
        ("p.[Ala406Thr;Gly12Cys]", None),
        ("p.Arg97fs", None),
        ("p.A406T", None),
        ("", None),
        (None, None),
    ],
)
def test_parse_simple_protein_substitution(hgvs, expected):
    assert parse_simple_protein_substitution(hgvs) == expected


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


def test_split_cis_phased_hgvs_passes_through_bracketed_without_accession():
    # Bracketed but accession-less input is not a cis-phased multivariant we can qualify; it must
    # degrade to a single-element list rather than raising on the missing ":".
    assert split_cis_phased_hgvs("g.[1000A>G;1002T>C]") == ["g.[1000A>G;1002T>C]"]


def test_join_cis_phased_hgvs_orders_components_by_position():
    # Out-of-order members are emitted in coordinate order.
    assert (
        join_cis_phased_hgvs(["NC_000001.11:g.1002T>C", "NC_000001.11:g.1000A>G"]) == "NC_000001.11:g.[1000A>G;1002T>C]"
    )


def test_join_cis_phased_hgvs_is_order_independent():
    # The same set of members yields the same string regardless of input ordering (the VRS block
    # digest is order-independent; the exported HGVS string must be too).
    forward = join_cis_phased_hgvs(["NC_000001.11:g.1000A>G", "NC_000001.11:g.1002T>C"])
    reverse = join_cis_phased_hgvs(["NC_000001.11:g.1002T>C", "NC_000001.11:g.1000A>G"])
    assert forward == reverse


def test_join_cis_phased_hgvs_orders_protein_components_by_position():
    # The first integer is the position for protein forms too (Arg123Gly), not just genomic.
    assert (
        join_cis_phased_hgvs(["NP_000001.1:p.Arg223Gly", "NP_000001.1:p.Ala12Val"])
        == "NP_000001.1:p.[Ala12Val;Arg223Gly]"
    )
