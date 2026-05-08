# ruff: noqa: E402
"""Tests for ClinGen Allele Registry API functions."""

import pytest

pytest.importorskip("aiocache", reason="aiocache is required for tests of allele registry functions")

import requests

from mavedb.lib.clingen.allele_registry import (
    get_associated_clinvar_allele_id,
    get_canonical_pa_ids,
    get_matching_registered_ca_ids,
)


@pytest.mark.network
class TestGetCanonicalPaIdsNetwork:
    @pytest.mark.asyncio
    async def test_get_canonical_pa_ids_known_caid(self):
        # Using a known ClinGen Allele ID with MANE transcripts
        clingen_allele_id = "CA321211"  # Example ClinGen Allele ID
        result = await get_canonical_pa_ids(clingen_allele_id)
        assert isinstance(result, list)
        assert result == ["PA2573050890", "PA321212"]  # Expected MANE PA ID

    @pytest.mark.asyncio
    async def test_get_canonical_pa_ids_known_no_mane(self):
        # Using a ClinGen Allele ID for protein change, as this will not have mane transcripts
        clingen_allele_id = "PA102264"  # Example ClinGen Allele ID with no MANE
        result = await get_canonical_pa_ids(clingen_allele_id)
        assert result == []

    @pytest.mark.asyncio
    async def test_get_canonical_pa_ids_invalid_id(self):
        # Using an invalid ClinGen Allele ID raises 400 Bad Request (malformed input)
        # Only 404 is treated as "no data" - other errors surface to help catch bugs

        clingen_allele_id = "INVALID_ID"
        with pytest.raises(requests.exceptions.HTTPError, match="400"):
            await get_canonical_pa_ids(clingen_allele_id)


@pytest.mark.network
class TestGetMatchingRegisteredCaIdsNetwork:
    @pytest.mark.asyncio
    async def test_get_matching_registered_ca_ids_known_paid(self):
        # Using a known ClinGen PA ID with registered CA IDs
        clingen_pa_id = "PA2573050890"  # Example ClinGen PA ID
        result = await get_matching_registered_ca_ids(clingen_pa_id)
        assert isinstance(result, list)
        assert "CA321211" in result  # Expected registered CA ID

    @pytest.mark.asyncio
    async def test_get_matching_registered_ca_ids_known_no_caids(self):
        # Using a ClinGen PA ID with no registered CA IDs
        clingen_pa_id = "PA3051398879"  # Example ClinGen PA ID with no registered CA IDs
        result = await get_matching_registered_ca_ids(clingen_pa_id)
        assert result == []

    @pytest.mark.asyncio
    async def test_get_matching_registered_ca_ids_invalid_id(self):
        # Using an invalid ClinGen PA ID raises 400 Bad Request (malformed input)
        # Only 404 is treated as "no data" - other errors surface to help catch bugs
        clingen_pa_id = "INVALID_ID"
        with pytest.raises(requests.exceptions.HTTPError, match="400"):
            await get_matching_registered_ca_ids(clingen_pa_id)


@pytest.mark.network
class TestGetAssociatedClinvarAlleleIdNetwork:
    @pytest.mark.asyncio
    async def test_get_associated_clinvar_allele_id_known_caid(self):
        # Using a known ClinGen Allele ID with associated ClinVar Allele ID
        clingen_allele_id = "CA321211"  # Example ClinGen Allele ID
        result = await get_associated_clinvar_allele_id(clingen_allele_id)
        assert result == "211565"  # Expected ClinVar Allele ID

    @pytest.mark.asyncio
    async def test_get_associated_clinvar_allele_id_no_association(self):
        # Using a ClinGen Allele ID with no associated ClinVar Allele ID
        clingen_allele_id = "CA9532274"  # Example ClinGen Allele ID with no association
        result = await get_associated_clinvar_allele_id(clingen_allele_id)
        assert result == ""  # Empty string indicates no ClinVar association (cached result)

    @pytest.mark.asyncio
    async def test_get_associated_clinvar_allele_id_invalid_id(self):
        # Using an invalid ClinGen Allele ID raises 400 Bad Request (malformed input)
        # Only 404 is treated as "no data" - other errors surface to help catch bugs
        clingen_allele_id = "INVALID_ID"
        with pytest.raises(requests.exceptions.HTTPError, match="400"):
            await get_associated_clinvar_allele_id(clingen_allele_id)
