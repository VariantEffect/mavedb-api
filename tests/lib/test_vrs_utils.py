# ruff: noqa: E402

import pytest

pytest.importorskip("ga4gh.vrs")

from ga4gh.vrs.models import (
    Allele,
    CisPhasedBlock,
    LiteralSequenceExpression,
    SequenceLocation,
    SequenceReference,
)

from mavedb.lib import vrs_utils
from mavedb.lib.vrs_utils import identify_variation, translate_hgvs_to_variation

_SQ = "SQ." + "a" * 32


def _allele(start: int, alt: str) -> Allele:
    return Allele(
        location=SequenceLocation(
            sequenceReference=SequenceReference(refgetAccession=_SQ),
            start=start,
            end=start + 1,
        ),
        state=LiteralSequenceExpression(sequence=alt),
    )


def _patch_component_translator(monkeypatch, alleles_by_hgvs: dict[str, Allele]) -> None:
    """Stub the per-component single-allele translator that translate_hgvs_to_variation calls."""

    def _fake(hgvs: str, translator=None) -> Allele:
        return alleles_by_hgvs[hgvs]

    monkeypatch.setattr(vrs_utils, "translate_hgvs_to_vrs", _fake)


def test_single_variant_returns_a_bare_allele(monkeypatch):
    allele = _allele(1000, "G")
    _patch_component_translator(monkeypatch, {"NC_000001.11:g.1000A>G": allele})

    result = translate_hgvs_to_variation("NC_000001.11:g.1000A>G", translator=None)

    assert isinstance(result, Allele)
    assert result is allele


def test_cis_phased_multivariant_returns_an_identified_block(monkeypatch):
    members = {
        "NC_000001.11:g.1000A>G": _allele(1000, "G"),
        "NC_000001.11:g.1002T>C": _allele(1002, "C"),
    }
    _patch_component_translator(monkeypatch, members)

    result = translate_hgvs_to_variation("NC_000001.11:g.[1000A>G;1002T>C]", translator=None)

    assert isinstance(result, CisPhasedBlock)
    assert len(result.members) == 2
    assert result.id is not None and result.id.startswith("ga4gh:CPB.")


def test_cis_phased_block_digest_is_order_independent(monkeypatch):
    members = {
        "NC_000001.11:g.1000A>G": _allele(1000, "G"),
        "NC_000001.11:g.1002T>C": _allele(1002, "C"),
    }
    _patch_component_translator(monkeypatch, members)

    forward = translate_hgvs_to_variation("NC_000001.11:g.[1000A>G;1002T>C]", translator=None)
    reverse = translate_hgvs_to_variation("NC_000001.11:g.[1002T>C;1000A>G]", translator=None)

    # The same biological cis-phased set dedups to one row regardless of component ordering.
    assert forward.id == reverse.id


def test_identify_variation_clears_stale_block_digest():
    block = CisPhasedBlock(members=[_allele(1000, "G"), _allele(1002, "C")])
    block.digest = "STALE"

    digest = identify_variation(block)

    assert digest != "STALE"
    assert digest.startswith("ga4gh:CPB.")  # recomputed from content, not the stale cache
