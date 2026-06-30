# ruff: noqa: E402

from types import SimpleNamespace

import pytest

pytest.importorskip("ga4gh.vrs")

from ga4gh.core.models import iriReference
from ga4gh.vrs.models import (
    Allele,
    CisPhasedBlock,
    LiteralSequenceExpression,
    Range,
    ReferenceLengthExpression,
    SequenceLocation,
    SequenceReference,
)

from mavedb.lib import vrs_utils
from mavedb.lib.vrs_utils import (
    _rle_to_lse,
    identify_variation,
    normalize_and_identify,
    translate_hgvs_to_variation,
)

_SQ = "SQ." + "a" * 32

# A digest a reused ga4gh AlleleTranslator might leave on a component before it is
# re-identified — stale from the Merkle cache, or carried over from a sibling variant.
_STALE_ID = "ga4gh:VA." + "Z" * 32


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


def _stub_normalize(monkeypatch) -> None:
    """Make normalization a no-op so identification can be exercised without a seqrepo.

    Identification (the regression surface) runs for real; only the proxy-backed
    normalize step is stubbed out.
    """
    monkeypatch.setattr(vrs_utils, "normalize", lambda allele, data_proxy: allele)


def _translator() -> SimpleNamespace:
    return SimpleNamespace(data_proxy=object())


def test_single_variant_returns_a_bare_allele(monkeypatch):
    allele = _allele(1000, "G")
    _patch_component_translator(monkeypatch, {"NC_000001.11:g.1000A>G": allele})
    _stub_normalize(monkeypatch)

    result = translate_hgvs_to_variation("NC_000001.11:g.1000A>G", translator=_translator())

    assert isinstance(result, Allele)
    assert not isinstance(result, CisPhasedBlock)


def test_single_variant_is_reidentified_not_left_with_translator_digest(monkeypatch):
    # ga4gh's translate_from (do_normalize=False) stamps id via plain ga4gh_identify on
    # the reused translator; translate_hgvs_to_variation must re-identify so the persisted
    # vrs_digest reflects current content rather than that carried-over value.
    allele = _allele(1000, "G")
    allele.id = _STALE_ID
    _patch_component_translator(monkeypatch, {"NC_000001.11:g.1000A>G": allele})
    _stub_normalize(monkeypatch)

    result = translate_hgvs_to_variation("NC_000001.11:g.1000A>G", translator=_translator())

    assert result.id is not None
    assert result.id != _STALE_ID
    assert result.id.startswith("ga4gh:VA.")


def test_same_position_distinct_alts_get_distinct_digests(monkeypatch):
    # Regression: NC_000002.12:g.214809459A>C and g.214809459A>T are different MaveDB
    # variants that cannot encode the same codon, yet were merged onto one Allele row
    # (and thus one coding equivalent like NM_000465.4:c.109_111delinsCGA). The single
    # component returned by the reused translator carried a stale/shared digest, which
    # the digest-keyed get_or_create_allele then deduplicated. Re-identification must
    # give distinct content distinct digests so they never collide.
    a_c = _allele(214809458, "C")
    a_t = _allele(214809458, "T")
    a_c.id = a_t.id = _STALE_ID  # what the unfixed path would persist for both
    _patch_component_translator(
        monkeypatch,
        {
            "NC_000002.12:g.214809459A>C": a_c,
            "NC_000002.12:g.214809459A>T": a_t,
        },
    )
    _stub_normalize(monkeypatch)
    translator = _translator()

    res_c = translate_hgvs_to_variation("NC_000002.12:g.214809459A>C", translator=translator)
    res_t = translate_hgvs_to_variation("NC_000002.12:g.214809459A>T", translator=translator)

    assert res_c.id != _STALE_ID
    assert res_t.id != _STALE_ID
    assert res_c.id != res_t.id


def test_cis_phased_multivariant_returns_an_identified_block(monkeypatch):
    members = {
        "NC_000001.11:g.1000A>G": _allele(1000, "G"),
        "NC_000001.11:g.1002T>C": _allele(1002, "C"),
    }
    _patch_component_translator(monkeypatch, members)
    _stub_normalize(monkeypatch)

    result = translate_hgvs_to_variation("NC_000001.11:g.[1000A>G;1002T>C]", translator=_translator())

    assert isinstance(result, CisPhasedBlock)
    assert len(result.members) == 2
    assert result.id is not None and result.id.startswith("ga4gh:CPB.")


def test_cis_phased_block_digest_is_order_independent(monkeypatch):
    members = {
        "NC_000001.11:g.1000A>G": _allele(1000, "G"),
        "NC_000001.11:g.1002T>C": _allele(1002, "C"),
    }
    _patch_component_translator(monkeypatch, members)
    _stub_normalize(monkeypatch)

    forward = translate_hgvs_to_variation("NC_000001.11:g.[1000A>G;1002T>C]", translator=_translator())
    reverse = translate_hgvs_to_variation("NC_000001.11:g.[1002T>C;1000A>G]", translator=_translator())

    # The same biological cis-phased set dedups to one row regardless of component ordering.
    assert forward.id == reverse.id


def test_identify_variation_clears_stale_block_digest():
    block = CisPhasedBlock(members=[_allele(1000, "G"), _allele(1002, "C")])
    block.digest = "STALE"

    digest = identify_variation(block)

    assert digest != "STALE"
    assert digest.startswith("ga4gh:CPB.")  # recomputed from content, not the stale cache


def _rle_allele(start: int, *, length: int, repeat_subunit_length: int) -> Allele:
    return Allele(
        location=SequenceLocation(
            sequenceReference=SequenceReference(refgetAccession=_SQ),
            start=start,
            end=start + repeat_subunit_length,
        ),
        state=ReferenceLengthExpression(length=length, repeatSubunitLength=repeat_subunit_length),
    )


def test_rle_to_lse_tiles_repeat_subunit():
    # repeatSubunitLength=2 reads 2 bases from the proxy; length=4 tiles them out to "ACAC".
    location = SequenceLocation(sequenceReference=SequenceReference(refgetAccession=_SQ), start=10, end=12)
    rle = ReferenceLengthExpression(length=4, repeatSubunitLength=2)
    data_proxy = SimpleNamespace(get_sequence=lambda identifier, start, end: "AC")

    result = _rle_to_lse(rle, location, data_proxy)

    assert isinstance(result, LiteralSequenceExpression)
    assert result.sequence.root == "ACAC"


def test_normalize_and_identify_coerces_rle_to_lse(monkeypatch):
    # Regression: normalization can yield an RLE for an indel, but dcd_mapping stores LSE; the
    # finalize step must coerce so the same variant doesn't hash to two digests (and duplicate).
    rle = _rle_allele(10, length=2, repeat_subunit_length=2)
    monkeypatch.setattr(vrs_utils, "normalize", lambda allele, data_proxy: rle)
    data_proxy = SimpleNamespace(get_sequence=lambda identifier, start, end: "AC")

    result = normalize_and_identify(rle, data_proxy=data_proxy)

    assert isinstance(result.state, LiteralSequenceExpression)
    assert result.id is not None and result.id.startswith("ga4gh:VA.")


# The unions below (location.sequenceReference, location.start, rle.length) carry IRI-reference
# and Range variants that a fully-resolved indel allele never has. _rle_to_lse and the RLE branch
# of normalize_and_identify guard the inlined-and-integer contract with asserts; these tests pin
# that the guards fire rather than letting an IRI/Range slip into the digest computation as a
# silent AttributeError or wrong sequence read.
_NEVER_READ = SimpleNamespace(get_sequence=lambda identifier, start, end: pytest.fail("proxy read despite bad input"))


def test_rle_to_lse_rejects_iri_sequence_reference():
    location = SequenceLocation(sequenceReference=iriReference("seqref:unresolved"), start=10, end=12)
    rle = ReferenceLengthExpression(length=4, repeatSubunitLength=2)

    with pytest.raises(AssertionError):
        _rle_to_lse(rle, location, _NEVER_READ)


def test_rle_to_lse_rejects_range_start():
    location = SequenceLocation(sequenceReference=SequenceReference(refgetAccession=_SQ), start=Range([10, 12]), end=14)
    rle = ReferenceLengthExpression(length=4, repeatSubunitLength=2)

    with pytest.raises(AssertionError):
        _rle_to_lse(rle, location, _NEVER_READ)


def test_rle_to_lse_rejects_range_length():
    location = SequenceLocation(sequenceReference=SequenceReference(refgetAccession=_SQ), start=10, end=12)
    rle = ReferenceLengthExpression(length=Range([2, 4]), repeatSubunitLength=2)

    with pytest.raises(AssertionError):
        _rle_to_lse(rle, location, _NEVER_READ)


def test_normalize_and_identify_rejects_iri_location_for_rle(monkeypatch):
    # If normalization ever returned an RLE allele whose location is an unresolved IRI reference,
    # _rle_to_lse could not read a sequence from it; the call-site assert must catch this before
    # the digest is (mis)computed rather than crashing deeper with an AttributeError.
    bad = Allele(
        location=iriReference("loc:unresolved"),
        state=ReferenceLengthExpression(length=2, repeatSubunitLength=2),
    )
    monkeypatch.setattr(vrs_utils, "normalize", lambda allele, data_proxy: bad)

    with pytest.raises(AssertionError):
        normalize_and_identify(bad, data_proxy=_NEVER_READ)
