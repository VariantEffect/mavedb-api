# ruff: noqa: E402

"""Tests for mavedb.lib.vrs — deserialization of stored post_mapped dicts into GA4GH VRS objects."""

from copy import deepcopy

import pytest

pytest.importorskip("psycopg2")

from mavedb.lib.vrs import vrs_object_from_mapped_variant
from tests.helpers.constants import (
    TEST_SEQUENCE_LOCATION_ACCESSION,
    TEST_VALID_POST_MAPPED_VRS_ALLELE,
    TEST_VALID_POST_MAPPED_VRS_ALLELE_LENGTH_EXPRESSION,
    TEST_VALID_POST_MAPPED_VRS_ALLELE_RLE,
)


@pytest.mark.unit
class TestVariationExtractionUnit:
    @pytest.mark.parametrize(
        "variation_version",
        [{"variation": TEST_VALID_POST_MAPPED_VRS_ALLELE}, TEST_VALID_POST_MAPPED_VRS_ALLELE],
        ids=["vrs13_wrapped_variation", "vrs2_direct_allele"],
    )
    def test_vrs_object_from_post_mapped_variation(self, variation_version):
        result = vrs_object_from_mapped_variant(variation_version).model_dump()

        assert result["location"]["id"] == TEST_SEQUENCE_LOCATION_ACCESSION
        assert result["location"]["start"] == 5
        assert result["location"]["end"] == 6

    @pytest.mark.parametrize(
        "allele_dict, expected_state_type, expected_state_fields",
        [
            (
                TEST_VALID_POST_MAPPED_VRS_ALLELE,
                "LiteralSequenceExpression",
                {"sequence": "F"},
            ),
            (
                TEST_VALID_POST_MAPPED_VRS_ALLELE_RLE,
                "ReferenceLengthExpression",
                {"length": 5, "repeatSubunitLength": 5},
            ),
            (
                TEST_VALID_POST_MAPPED_VRS_ALLELE_LENGTH_EXPRESSION,
                "LengthExpression",
                {"length": 5},
            ),
        ],
        ids=["lse_state", "rle_state", "length_expression_state"],
    )
    def test_allele_state_type_is_deserialized_correctly(self, allele_dict, expected_state_type, expected_state_fields):
        result = vrs_object_from_mapped_variant(allele_dict).model_dump()

        assert result["state"]["type"] == expected_state_type
        for k, v in expected_state_fields.items():
            assert result["state"][k] == v

    def test_unknown_allele_state_type_raises_value_error(self):
        allele_dict = {
            **TEST_VALID_POST_MAPPED_VRS_ALLELE,
            "state": {"type": "UnknownFutureExpression", "someField": "someValue"},
        }

        with pytest.raises(ValueError, match="Unsupported VRS Allele state type"):
            vrs_object_from_mapped_variant(allele_dict)

    @pytest.mark.parametrize(
        "member_dict, expected_state_type",
        [
            (TEST_VALID_POST_MAPPED_VRS_ALLELE, "LiteralSequenceExpression"),
            (TEST_VALID_POST_MAPPED_VRS_ALLELE_RLE, "ReferenceLengthExpression"),
            (TEST_VALID_POST_MAPPED_VRS_ALLELE_LENGTH_EXPRESSION, "LengthExpression"),
        ],
        ids=["lse_members", "rle_members", "length_expression_members"],
    )
    def test_cis_phased_block_member_state_types_are_deserialized_correctly(self, member_dict, expected_state_type):
        mapping_results = {"type": "CisPhasedBlock", "members": [deepcopy(member_dict), deepcopy(member_dict)]}

        result = vrs_object_from_mapped_variant(mapping_results).model_dump()

        assert result["type"] == "CisPhasedBlock"
        assert len(result["members"]) == 2
        assert result["members"][0]["state"]["type"] == expected_state_type


@pytest.mark.integration
class TestVrsIntegration:
    def test_vrs_object_from_persisted_post_mapped(self, setup_lib_db_with_mapped_variant):
        variation = vrs_object_from_mapped_variant(TEST_VALID_POST_MAPPED_VRS_ALLELE)

        assert variation is not None
        assert variation.model_dump().get("type") in {"Allele", "CisPhasedBlock"}
