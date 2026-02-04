import pytest

from mavedb.lib.clingen.allele_registry import (
    get_associated_clinvar_allele_id,
    get_canonical_pa_ids,
    get_matching_registered_ca_ids,
)


@pytest.mark.network
class TestGetCanonicalPaIdsNetwork:
    def test_get_canonical_pa_ids_known_caid(self):
        # Using a known ClinGen Allele ID with MANE transcripts
        clingen_allele_id = "CA321211"  # Example ClinGen Allele ID
        result = get_canonical_pa_ids(clingen_allele_id)
        assert isinstance(result, list)
        assert result == ["PA2573050890", "PA321212"]  # Expected MANE PA ID

    def test_get_canonical_pa_ids_known_no_mane(self):
        # Using a ClinGen Allele ID for protein change, as this will not have mane transcripts
        clingen_allele_id = "PA102264"  # Example ClinGen Allele ID with no MANE
        result = get_canonical_pa_ids(clingen_allele_id)
        assert result == []

    def test_get_canonical_pa_ids_invalid_id(self):
        # Using an invalid ClinGen Allele ID
        clingen_allele_id = "INVALID_ID"
        result = get_canonical_pa_ids(clingen_allele_id)
        assert result == []


@pytest.mark.network
class TestGetMatchingRegisteredCaIdsNetwork:
    def test_get_matching_registered_ca_ids_known_paid(self):
        # Using a known ClinGen PA ID with registered CA IDs
        clingen_pa_id = "PA2573050890"  # Example ClinGen PA ID
        result = get_matching_registered_ca_ids(clingen_pa_id)
        assert isinstance(result, list)
        assert "CA321211" in result  # Expected registered CA ID

    def test_get_matching_registered_ca_ids_known_no_caids(self):
        # Using a ClinGen PA ID with no registered CA IDs
        clingen_pa_id = "PA3051398879"  # Example ClinGen PA ID with no registered CA IDs
        result = get_matching_registered_ca_ids(clingen_pa_id)
        assert result == []

    def test_get_matching_registered_ca_ids_invalid_id(self):
        # Using an invalid ClinGen PA ID
        clingen_pa_id = "INVALID_ID"
        result = get_matching_registered_ca_ids(clingen_pa_id)
        assert result == []


@pytest.mark.network
class TestGetAssociatedClinvarAlleleIdNetwork:
    def test_get_associated_clinvar_allele_id_known_caid(self):
        # Using a known ClinGen Allele ID with associated ClinVar Allele ID
        clingen_allele_id = "CA321211"  # Example ClinGen Allele ID
        result = get_associated_clinvar_allele_id(clingen_allele_id)
        assert result == "211565"  # Expected ClinVar Allele ID

    def test_get_associated_clinvar_allele_id_no_association(self):
        # Using a ClinGen Allele ID with no associated ClinVar Allele ID
        clingen_allele_id = "CA9532274"  # Example ClinGen Allele ID with no association
        result = get_associated_clinvar_allele_id(clingen_allele_id)
        assert result is None

    def test_get_associated_clinvar_allele_id_invalid_id(self):
        # Using an invalid ClinGen Allele ID
        clingen_allele_id = "INVALID_ID"
        result = get_associated_clinvar_allele_id(clingen_allele_id)
        assert result is None
