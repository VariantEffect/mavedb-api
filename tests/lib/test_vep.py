"""Unit tests for mavedb.lib.vep — covers response parsing for both Variant Recoder and VEP APIs.

These tests mock the underlying HTTP call (request_with_backoff) and assert that the parsing
logic correctly handles the actual Ensembl REST API response shapes.
"""

from unittest.mock import MagicMock, patch

import pytest

from mavedb.lib.vep import get_functional_consequence, run_variant_recoder


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
