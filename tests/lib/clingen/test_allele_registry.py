from unittest import mock

import pytest

from mavedb.lib.clingen.allele_registry import (
    get_associated_clinvar_allele_id,
    get_canonical_pa_ids,
    get_matching_registered_ca_ids,
)


@pytest.mark.unit
@mock.patch("mavedb.lib.clingen.allele_registry.requests.get")
class TestGetCanonicalPaIds:
    def test_get_canonical_pa_ids_success(self, mock_request):
        # Mock response object
        mock_response = mock.Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "transcriptAlleles": [
                {"MANE": True, "@id": "https://reg.genome.network/allele/PA12345"},
                {"MANE": False, "@id": "https://reg.genome.network/allele/PA54321"},
                {"MANE": True, "@id": "https://reg.genome.network/allele/PA67890"},
                {"@id": "https://reg.genome.network/allele/PA00000"},  # No MANE
            ]
        }
        mock_request.return_value = mock_response

        result = get_canonical_pa_ids("CA00001")
        assert result == ["PA12345", "PA67890"]

    def test_get_canonical_pa_ids_no_transcript_alleles(self, mock_request):
        mock_response = mock.Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {}
        mock_request.return_value = mock_response

        result = get_canonical_pa_ids("CA00002")
        assert result == []

    def test_get_canonical_pa_ids_empty_transcript_alleles(self, mock_request):
        mock_response = mock.Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"transcriptAlleles": []}
        mock_request.return_value = mock_response

        result = get_canonical_pa_ids("CA00003")
        assert result == []

    def test_get_canonical_pa_ids_missing_mane_or_id(self, mock_request):
        mock_response = mock.Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "transcriptAlleles": [
                {"MANE": True},  # Missing @id
                {"@id": "https://reg.genome.network/allele/PA99999"},  # Missing MANE
                {},  # Missing both
            ]
        }
        mock_request.return_value = mock_response

        result = get_canonical_pa_ids("CA00004")
        assert result == []

    def test_get_canonical_pa_ids_api_error(self, mock_request):
        mock_response = mock.Mock()
        mock_response.status_code = 404
        mock_request.return_value = mock_response

        result = get_canonical_pa_ids("CA404")
        assert result == []


@pytest.mark.unit
@mock.patch("mavedb.lib.clingen.allele_registry.requests.get")
class TestGetMatchingRegisteredCaIds:
    def test_get_matching_registered_ca_ids_success(self, mock_request):
        mock_response = mock.Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "aminoAcidAlleles": [
                {
                    "matchingRegisteredTranscripts": [
                        {"@id": "https://reg.genome.network/allele/CA11111"},
                        {"@id": "https://reg.genome.network/allele/CA22222"},
                    ]
                },
                {
                    "matchingRegisteredTranscripts": [
                        {"@id": "https://reg.genome.network/allele/CA33333"},
                    ]
                },
                {
                    # No matchingRegisteredTranscripts
                },
            ]
        }
        mock_request.return_value = mock_response

        result = get_matching_registered_ca_ids("PA12345")
        assert result == ["CA11111", "CA22222", "CA33333"]

    def test_get_matching_registered_ca_ids_no_amino_acid_alleles(self, mock_request):
        mock_response = mock.Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {}
        mock_request.return_value = mock_response

        result = get_matching_registered_ca_ids("PA00000")
        assert result == []

    def test_get_matching_registered_ca_ids_empty_amino_acid_alleles(self, mock_request):
        mock_response = mock.Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"aminoAcidAlleles": []}
        mock_request.return_value = mock_response

        result = get_matching_registered_ca_ids("PA00001")
        assert result == []

    def test_get_matching_registered_ca_ids_missing_matching_registered_transcripts(self, mock_request):
        mock_response = mock.Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "aminoAcidAlleles": [
                {},  # No matchingRegisteredTranscripts
                {"matchingRegisteredTranscripts": []},  # Empty list
            ]
        }
        mock_request.return_value = mock_response

        result = get_matching_registered_ca_ids("PA00002")
        assert result == []

    def test_get_matching_registered_ca_ids_api_error(self, mock_request):
        mock_response = mock.Mock()
        mock_response.status_code = 500
        mock_request.return_value = mock_response

        result = get_matching_registered_ca_ids("PAERROR")
        assert result == []


@pytest.mark.unit
@mock.patch("mavedb.lib.clingen.allele_registry.requests.get")
class TestGetAssociatedClinvarAlleleId:
    def test_get_associated_clinvar_allele_id_success(self, mock_request):
        mock_response = mock.Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"externalRecords": {"ClinVarAlleles": [{"alleleId": "123456"}]}}
        mock_request.return_value = mock_response

        result = get_associated_clinvar_allele_id("CA00001")
        assert result == "123456"

    def test_get_associated_clinvar_allele_id_no_external_records(self, mock_request):
        mock_response = mock.Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {}
        mock_request.return_value = mock_response

        result = get_associated_clinvar_allele_id("CA00002")
        assert result is None

    def test_get_associated_clinvar_allele_id_no_clinvar_alleles(self, mock_request):
        mock_response = mock.Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"externalRecords": {}}
        mock_request.return_value = mock_response

        result = get_associated_clinvar_allele_id("CA00003")
        assert result is None

    def test_get_associated_clinvar_allele_id_missing_allele_id(self, mock_request):
        mock_response = mock.Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"externalRecords": {"ClinVarAlleles": [{}]}}
        mock_request.return_value = mock_response

        result = get_associated_clinvar_allele_id("CA00004")
        assert result is None

    def test_get_associated_clinvar_allele_id_api_error(self, mock_request):
        mock_response = mock.Mock()
        mock_response.status_code = 404
        mock_request.return_value = mock_response

        result = get_associated_clinvar_allele_id("CA404")
        assert result is None
