"""Unit tests for mavedb.lib.vep — covers response parsing for both Variant Recoder and VEP APIs.

These tests mock the underlying HTTP call (request_with_backoff) and assert that the parsing
logic correctly handles the actual Ensembl REST API response shapes.
"""

from datetime import date, timedelta
from unittest.mock import MagicMock, patch

import pytest
from sqlalchemy import select

from mavedb.lib.vep import (
    get_ensembl_release,
    get_functional_consequence,
    link_vep_consequences_to_alleles,
    run_variant_recoder,
)
from mavedb.models.allele import Allele
from mavedb.models.vep_allele_consequence import VepAlleleConsequence


def _mock_response(data) -> MagicMock:
    """Return a mock requests.Response whose .json() returns *data*."""
    mock = MagicMock()
    mock.json.return_value = data
    return mock


# ---------------------------------------------------------------------------
# Realistic Variant Recoder response payloads
#
# Ensembl Variant Recoder returns a list of single-key dicts.  The key is the
# called allele (e.g. "T"). The allele dict contains input, hgvsg, hgvsc, etc.
# ---------------------------------------------------------------------------

RECODER_RESPONSE_SINGLE = [
    {
        "T": {
            "hgvsc": [
                "NM_007294.4:c.5G>A",
            ],
            "spdi": ["NC_000017.11:43094691:C:T"],
            "input": "NP_009225.1:p.Cys2Tyr",
            "hgvsp": ["NP_009225.1:p.Cys2Tyr"],
            "hgvsg": [
                "NC_000017.11:g.43094692C>T",
                "LRG_292:g.100C>T",  # non-NC_ entry — should be filtered out
            ],
        }
    }
]

RECODER_RESPONSE_MULTIPLE = [
    {
        "A": {
            "input": "NM_007294.4:c.5G>T",
            "hgvsg": ["NC_000017.11:g.43094692C>A"],
        }
    },
    {
        "T": {
            "input": "NM_007294.4:c.5G>A",
            "hgvsg": ["NC_000017.11:g.43094692C>T"],
        }
    },
]

RECODER_RESPONSE_MULTI_ALLELE = [
    {
        # Both allele keys share the same "input" value; each has distinct NC_ hgvsg entries.
        "CAT": {
            "input": "NP_009225.1:p.Val1696His",
            "hgvsg": [
                "NC_000017.11:g.43063938_43063940delinsATG",
                "LRG_292:g.154044_154046delinsCAT",
            ],
            "hgvsc": ["NM_007294.4:c.5086_5088delinsCAT"],
        },
        "CAC": {
            "input": "NP_009225.1:p.Val1696His",
            "hgvsg": [
                "NC_000017.11:g.43063938_43063940inv",
                "LRG_292:g.154044_154046inv",
            ],
            "hgvsc": ["NM_007294.4:c.5086_5088inv"],
        },
    }
]

RECODER_RESPONSE_NO_HGVSG = [
    {
        "T": {
            "input": "NP_009225.1:p.Xxx1Yyy",
            "hgvsc": ["NM_007294.4:c.5G>A"],
            # deliberately no "hgvsg" key
        }
    }
]


# ---------------------------------------------------------------------------
# Realistic VEP /vep/human/hgvs response payloads
#
# VEP returns a list of dicts where "input" and "most_severe_consequence" are
# *top-level* keys in each element (unlike Recoder where "input" is nested).
# ---------------------------------------------------------------------------

VEP_RESPONSE_SINGLE = [
    {
        "input": "NM_007294.4:c.5G>A",
        "most_severe_consequence": "missense_variant",
        "id": "rs80357382",
        "seq_region_name": "17",
    }
]

VEP_RESPONSE_MULTIPLE = [
    {
        "input": "NM_007294.4:c.5G>A",
        "most_severe_consequence": "missense_variant",
    },
    {
        "input": "NM_007294.4:c.10C>T",
        "most_severe_consequence": "synonymous_variant",
    },
]


# ---------------------------------------------------------------------------
# run_variant_recoder
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
@pytest.mark.unit
class TestRunVariantRecoder:
    async def test_extracts_nc_genomic_hgvs_from_real_response_format(self):
        """Correctly extracts NC_ genomic HGVS from the nested allele-key response structure."""
        with patch("mavedb.lib.vep.request_with_backoff", return_value=_mock_response(RECODER_RESPONSE_SINGLE)):
            result = await run_variant_recoder(["NP_009225.1:p.Cys2Tyr"])

        assert result == {"NP_009225.1:p.Cys2Tyr": ["NC_000017.11:g.43094692C>T"]}

    async def test_filters_non_nc_genomic_hgvs(self):
        """Non-NC_ entries in hgvsg (e.g. LRG accessions) are excluded from the result."""
        with patch("mavedb.lib.vep.request_with_backoff", return_value=_mock_response(RECODER_RESPONSE_SINGLE)):
            result = await run_variant_recoder(["NP_009225.1:p.Cys2Tyr"])

        returned_hgvsg = result.get("NP_009225.1:p.Cys2Tyr", [])
        assert all(h.startswith("NC_") for h in returned_hgvsg)
        assert not any("LRG" in h for h in returned_hgvsg)

    async def test_handles_multiple_inputs(self):
        """Multiple input variants in one batch are all mapped correctly."""
        with patch("mavedb.lib.vep.request_with_backoff", return_value=_mock_response(RECODER_RESPONSE_MULTIPLE)):
            result = await run_variant_recoder(["NM_007294.4:c.5G>T", "NM_007294.4:c.5G>A"])

        assert result == {
            "NM_007294.4:c.5G>T": ["NC_000017.11:g.43094692C>A"],
            "NM_007294.4:c.5G>A": ["NC_000017.11:g.43094692C>T"],
        }

    async def test_handles_multiple_allele_keys_per_element(self):
        """A single response element with multiple allele keys (e.g. CAT, CAC) collects NC_ hgvsg from all of them.

        Variant Recoder can return multiple possible allele representations for the same input variant
        in a single element dict.  Each allele key is independent and may carry different hgvsg entries.
        """
        with patch("mavedb.lib.vep.request_with_backoff", return_value=_mock_response(RECODER_RESPONSE_MULTI_ALLELE)):
            result = await run_variant_recoder(["NP_009225.1:p.Val1696His"])

        genomic_hgvsg = result.get("NP_009225.1:p.Val1696His", [])
        assert "NC_000017.11:g.43063938_43063940delinsATG" in genomic_hgvsg
        assert "NC_000017.11:g.43063938_43063940inv" in genomic_hgvsg
        assert all(h.startswith("NC_") for h in genomic_hgvsg)

    async def test_returns_empty_for_variant_without_hgvsg(self):
        """A variant whose allele dict has no hgvsg field is not included in the result."""
        with patch("mavedb.lib.vep.request_with_backoff", return_value=_mock_response(RECODER_RESPONSE_NO_HGVSG)):
            result = await run_variant_recoder(["NP_009225.1:p.Xxx1Yyy"])

        assert result == {}

    async def test_skips_non_dict_allele_values(self):
        """If an allele value is not a dict (e.g. null in the response), it is skipped gracefully."""
        response_with_null_allele = [{"T": None}]
        with patch("mavedb.lib.vep.request_with_backoff", return_value=_mock_response(response_with_null_allele)):
            result = await run_variant_recoder(["some_hgvs"])

        assert result == {}

    async def test_input_field_at_allele_level_not_top_level(self):
        """Regression: the 'input' field is nested inside the allele key, not at the top level of each list element.

        The old buggy implementation called entry.get("input") on the outer dict,
        which always returned None and silently dropped all results.
        """
        with patch("mavedb.lib.vep.request_with_backoff", return_value=_mock_response(RECODER_RESPONSE_SINGLE)):
            result = await run_variant_recoder(["NP_009225.1:p.Cys2Tyr"])

        # If "input" were read from the wrong level, result would be empty.
        assert result != {}
        assert "NP_009225.1:p.Cys2Tyr" in result


# ---------------------------------------------------------------------------
# get_functional_consequence
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
@pytest.mark.unit
class TestGetFunctionalConsequence:
    async def test_extracts_most_severe_consequence(self):
        """Parses input and most_severe_consequence from the top-level VEP response structure."""
        with patch("mavedb.lib.vep.request_with_backoff", return_value=_mock_response(VEP_RESPONSE_SINGLE)):
            result = await get_functional_consequence(["NM_007294.4:c.5G>A"])

        assert result == {"NM_007294.4:c.5G>A": "missense_variant"}

    async def test_maps_none_when_consequence_absent(self):
        """When most_severe_consequence is missing from an entry, the HGVS maps to None."""
        response = [{"input": "NM_007294.4:c.5G>A"}]
        with patch("mavedb.lib.vep.request_with_backoff", return_value=_mock_response(response)):
            result = await get_functional_consequence(["NM_007294.4:c.5G>A"])

        assert result == {"NM_007294.4:c.5G>A": None}

    async def test_skips_entries_without_input_key(self):
        """Entries that have no 'input' key are skipped entirely."""
        response = [{"most_severe_consequence": "missense_variant"}]
        with patch("mavedb.lib.vep.request_with_backoff", return_value=_mock_response(response)):
            result = await get_functional_consequence(["NM_007294.4:c.5G>A"])

        assert result == {}

    async def test_handles_multiple_variants(self):
        """All variants in a batch response are extracted correctly."""
        with patch("mavedb.lib.vep.request_with_backoff", return_value=_mock_response(VEP_RESPONSE_MULTIPLE)):
            result = await get_functional_consequence(["NM_007294.4:c.5G>A", "NM_007294.4:c.10C>T"])

        assert result == {
            "NM_007294.4:c.5G>A": "missense_variant",
            "NM_007294.4:c.10C>T": "synonymous_variant",
        }

    async def test_raises_if_more_than_200_variants(self):
        """Passing more than 200 HGVS strings raises ValueError before any HTTP call."""
        with pytest.raises(ValueError, match="maximum of 200"):
            await get_functional_consequence(["NM_007294.4:c.1A>T"] * 201)


### Tests for get_ensembl_release function ###


@pytest.mark.asyncio
async def test_get_ensembl_release_returns_release_as_string():
    """The /info/software release integer is returned as a string for use as source_version."""
    with patch("mavedb.lib.vep.request_with_backoff", return_value=_mock_response({"release": 116})):
        assert await get_ensembl_release() == "116"


### Tests for link_vep_consequences_to_alleles function ###


def _make_allele(session, *, vrs_digest, level="genomic"):
    """Create and persist a deduplicated Allele."""
    allele = Allele(vrs_digest=vrs_digest, level=level)
    session.add(allele)
    session.commit()
    session.refresh(allele)
    return allele


def _live_rows_for(session, allele_id):
    return session.scalars(
        select(VepAlleleConsequence).where(
            VepAlleleConsequence.allele_id == allele_id,
            VepAlleleConsequence.current,
        )
    ).all()


def _all_rows_for(session, allele_id):
    return session.scalars(select(VepAlleleConsequence).where(VepAlleleConsequence.allele_id == allele_id)).all()


def test_link_vep_creates_new_consequence(session):
    """A consequence for an allele with no live row creates a single live row and is reported changed."""
    allele = _make_allele(session, vrs_digest="vrs-1")

    changed = link_vep_consequences_to_alleles(
        session, {allele.id: "missense_variant"}, source_version="116", access_date=date.today()
    )
    session.commit()

    assert changed == {allele.id}
    live = _live_rows_for(session, allele.id)
    assert len(live) == 1
    assert live[0].functional_consequence == "missense_variant"
    assert live[0].source_version == "116"
    assert live[0].access_date == date.today()


def test_link_vep_unchanged_bumps_version_and_date_in_place(session):
    """Re-confirming an unchanged consequence at a new release advances source_version and access_date
    in place — no supersede, no new valid-time boundary, and the allele is not reported changed."""
    allele = _make_allele(session, vrs_digest="vrs-1")
    session.add(
        VepAlleleConsequence(
            allele_id=allele.id,
            functional_consequence="missense_variant",
            source_version="115",
            access_date=date.today() - timedelta(days=90),
        )
    )
    session.commit()

    changed = link_vep_consequences_to_alleles(
        session, {allele.id: "missense_variant"}, source_version="116", access_date=date.today()
    )
    session.commit()

    assert changed == set()
    # One row, still live, never retired — version and access_date advanced in place.
    all_rows = _all_rows_for(session, allele.id)
    assert len(all_rows) == 1
    assert all_rows[0].valid_to is None
    assert all_rows[0].source_version == "116"
    assert all_rows[0].access_date == date.today()


def test_link_vep_changed_consequence_supersedes(session):
    """A changed consequence retires the live row and inserts the successor — exactly one live row,
    keyed on allele_id, with the old one preserved as retired history."""
    allele = _make_allele(session, vrs_digest="vrs-1")
    session.add(
        VepAlleleConsequence(
            allele_id=allele.id,
            functional_consequence="synonymous_variant",
            source_version="115",
            access_date=date.today() - timedelta(days=90),
        )
    )
    session.commit()

    changed = link_vep_consequences_to_alleles(
        session, {allele.id: "missense_variant"}, source_version="116", access_date=date.today()
    )
    session.commit()

    assert changed == {allele.id}
    live = _live_rows_for(session, allele.id)
    assert len(live) == 1
    assert live[0].functional_consequence == "missense_variant"
    assert live[0].source_version == "116"

    all_rows = _all_rows_for(session, allele.id)
    assert len(all_rows) == 2
    assert len([r for r in all_rows if r.valid_to is not None]) == 1


def test_link_vep_none_leaves_live_row_untouched(session):
    """A transient None result must not overwrite a held consequence: the live row is left intact
    (value, version, and date) and the allele is not reported changed."""
    allele = _make_allele(session, vrs_digest="vrs-1")
    session.add(
        VepAlleleConsequence(
            allele_id=allele.id,
            functional_consequence="missense_variant",
            source_version="115",
            access_date=date.today() - timedelta(days=90),
        )
    )
    session.commit()

    changed = link_vep_consequences_to_alleles(
        session, {allele.id: None}, source_version="116", access_date=date.today()
    )
    session.commit()

    assert changed == set()
    live = _live_rows_for(session, allele.id)
    assert len(live) == 1
    assert live[0].functional_consequence == "missense_variant"
    # Not re-confirmed -> neither version nor access_date advanced.
    assert live[0].source_version == "115"
    assert live[0].access_date == date.today() - timedelta(days=90)


def test_link_vep_none_with_no_live_row_writes_nothing(session):
    """A None result for an allele with no live row writes nothing (the allele is re-queried next run),
    mirroring gnomAD's no-match handling."""
    allele = _make_allele(session, vrs_digest="vrs-1")

    changed = link_vep_consequences_to_alleles(
        session, {allele.id: None}, source_version="116", access_date=date.today()
    )
    session.commit()

    assert changed == set()
    assert len(_all_rows_for(session, allele.id)) == 0
