# ruff: noqa: E402

import pytest

pytest.importorskip("aiocache", reason="aiocache is required to test caching behavior of allele registry functions")

from unittest import mock

import requests

from mavedb.lib.clingen.allele_registry import (
    get_associated_clinvar_allele_id,
    get_canonical_pa_ids,
    get_clingen_allele_data,
    get_matching_registered_ca_ids,
)


@pytest.mark.unit
@mock.patch("mavedb.lib.clingen.allele_registry.requests.get")
class TestGetCanonicalPaIds:
    @pytest.mark.asyncio
    async def test_get_canonical_pa_ids_success(self, mock_request):
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

        result = await get_canonical_pa_ids("CA00001")
        assert result == ["PA12345", "PA67890"]

    @pytest.mark.asyncio
    async def test_get_canonical_pa_ids_no_transcript_alleles(self, mock_request):
        mock_response = mock.Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {}
        mock_request.return_value = mock_response

        result = await get_canonical_pa_ids("CA00002")
        assert result == []

    @pytest.mark.asyncio
    async def test_get_canonical_pa_ids_empty_transcript_alleles(self, mock_request):
        mock_response = mock.Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"transcriptAlleles": []}
        mock_request.return_value = mock_response

        result = await get_canonical_pa_ids("CA00003")
        assert result == []

    @pytest.mark.asyncio
    async def test_get_canonical_pa_ids_missing_mane_or_id(self, mock_request):
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

        result = await get_canonical_pa_ids("CA00004")
        assert result == []

    @pytest.mark.asyncio
    async def test_get_canonical_pa_ids_404_returns_empty(self, mock_request):
        """404 means allele doesn't exist - treat as 'no data' (cacheable)."""
        mock_response = mock.Mock()
        mock_response.status_code = 404
        mock_request.return_value = mock_response

        result = await get_canonical_pa_ids("CA404")
        assert result == []

    @pytest.mark.asyncio
    async def test_get_canonical_pa_ids_5xx_raises(self, mock_request):
        """5xx errors should raise exception (transient failure, can retry)."""
        mock_response = mock.Mock()
        mock_response.status_code = 500
        mock_response.raise_for_status.side_effect = requests.exceptions.HTTPError("500 Server Error")
        mock_request.return_value = mock_response

        with pytest.raises(requests.exceptions.HTTPError):
            await get_canonical_pa_ids("CA500")


@pytest.mark.unit
@mock.patch("mavedb.lib.clingen.allele_registry.requests.get")
class TestGetMatchingRegisteredCaIds:
    @pytest.mark.asyncio
    async def test_get_matching_registered_ca_ids_success(self, mock_request):
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

        result = await get_matching_registered_ca_ids("PA12345")
        assert result == ["CA11111", "CA22222", "CA33333"]

    @pytest.mark.asyncio
    async def test_get_matching_registered_ca_ids_no_amino_acid_alleles(self, mock_request):
        mock_response = mock.Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {}
        mock_request.return_value = mock_response

        result = await get_matching_registered_ca_ids("PA00000")
        assert result == []

    @pytest.mark.asyncio
    async def test_get_matching_registered_ca_ids_empty_amino_acid_alleles(self, mock_request):
        mock_response = mock.Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"aminoAcidAlleles": []}
        mock_request.return_value = mock_response

        result = await get_matching_registered_ca_ids("PA00001")
        assert result == []

    @pytest.mark.asyncio
    async def test_get_matching_registered_ca_ids_missing_matching_registered_transcripts(self, mock_request):
        mock_response = mock.Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "aminoAcidAlleles": [
                {},  # No matchingRegisteredTranscripts
                {"matchingRegisteredTranscripts": []},  # Empty list
            ]
        }
        mock_request.return_value = mock_response

        result = await get_matching_registered_ca_ids("PA00002")
        assert result == []

    @pytest.mark.asyncio
    async def test_get_matching_registered_ca_ids_404_returns_empty(self, mock_request):
        """404 means allele doesn't exist - treat as 'no data' (cacheable)."""
        mock_response = mock.Mock()
        mock_response.status_code = 404
        mock_request.return_value = mock_response

        result = await get_matching_registered_ca_ids("PA404")
        assert result == []

    @pytest.mark.asyncio
    async def test_get_matching_registered_ca_ids_5xx_raises(self, mock_request):
        """5xx errors should raise exception (transient failure, can retry)."""
        mock_response = mock.Mock()
        mock_response.status_code = 500
        mock_response.raise_for_status.side_effect = requests.exceptions.HTTPError("500 Server Error")
        mock_request.return_value = mock_response

        with pytest.raises(requests.exceptions.HTTPError):
            await get_matching_registered_ca_ids("PAERROR")


@pytest.mark.unit
@mock.patch("mavedb.lib.clingen.allele_registry.requests.get")
class TestGetAssociatedClinvarAlleleId:
    @pytest.mark.asyncio
    async def test_get_associated_clinvar_allele_id_success(self, mock_request):
        mock_response = mock.Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"externalRecords": {"ClinVarAlleles": [{"alleleId": "123456"}]}}
        mock_request.return_value = mock_response

        result = await get_associated_clinvar_allele_id("CA_CLINVAR_SUCCESS")
        assert result == "123456"

    @pytest.mark.asyncio
    async def test_get_associated_clinvar_allele_id_no_external_records(self, mock_request):
        mock_response = mock.Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {}
        mock_request.return_value = mock_response

        result = await get_associated_clinvar_allele_id("CA_CLINVAR_NO_RECORDS")

        # For "no data found" cases we intentionally return an empty string (not None)
        # to allow caching of these results. This is the modal case - most ClinGen alleles don't have ClinVar associations.
        assert result == ""

    @pytest.mark.asyncio
    async def test_get_associated_clinvar_allele_id_no_clinvar_alleles(self, mock_request):
        mock_response = mock.Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"externalRecords": {}}
        mock_request.return_value = mock_response

        result = await get_associated_clinvar_allele_id("CA_CLINVAR_NO_ALLELES")
        assert result == ""

    @pytest.mark.asyncio
    async def test_get_associated_clinvar_allele_id_missing_allele_id(self, mock_request):
        mock_response = mock.Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"externalRecords": {"ClinVarAlleles": [{}]}}
        mock_request.return_value = mock_response

        result = await get_associated_clinvar_allele_id("CA_CLINVAR_MISSING_ID")
        assert result == ""

    @pytest.mark.asyncio
    async def test_get_associated_clinvar_allele_id_404_returns_empty(self, mock_request):
        """404 means allele doesn't exist - treat as 'no data' (cacheable)."""
        mock_response = mock.Mock()
        mock_response.status_code = 404
        mock_request.return_value = mock_response

        result = await get_associated_clinvar_allele_id("CA_CLINVAR_404")
        assert result == ""

    @pytest.mark.asyncio
    async def test_get_associated_clinvar_allele_id_5xx_raises(self, mock_request):
        """5xx errors should raise exception (transient failure, can retry)."""
        mock_response = mock.Mock()
        mock_response.status_code = 500
        mock_response.raise_for_status.side_effect = requests.exceptions.HTTPError("500 Server Error")
        mock_request.return_value = mock_response

        with pytest.raises(requests.exceptions.HTTPError):
            await get_associated_clinvar_allele_id("CA_CLINVAR_500")


@pytest.mark.unit
@mock.patch("mavedb.lib.clingen.allele_registry.requests.get")
class TestCachingBehavior:
    """Test caching behavior of allele registry functions.

    These tests verify that the @cached decorator works correctly with the
    API functions, including cache hits, misses, and edge cases.
    Uses in-memory cache (configured in conftest.py) to avoid requiring Redis.
    """

    @pytest.mark.asyncio
    async def test_cache_hit_reduces_api_calls(self, mock_request, clear_cache):
        """Verify first call is cache miss, second call is cache hit (no API call)."""
        mock_response = mock.Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"externalRecords": {"ClinVarAlleles": [{"alleleId": "999999"}]}}
        mock_request.return_value = mock_response

        # First call - should hit the API (cache miss)
        result1 = await get_associated_clinvar_allele_id("CA_CACHE_TEST_1")
        assert result1 == "999999"
        assert mock_request.call_count == 1

        # Second call with same ID - should hit cache (no new API call)
        result2 = await get_associated_clinvar_allele_id("CA_CACHE_TEST_1")
        assert result2 == "999999"
        assert mock_request.call_count == 1  # Still 1, not 2

    @pytest.mark.asyncio
    async def test_empty_string_results_are_cached(self, mock_request, clear_cache):
        """Verify that empty string results (no ClinVar association) are cached.

        This is the modal case - most ClinGen alleles don't have ClinVar associations.
        We return empty string (not None) for successful API calls with no association,
        so aiocache will cache these results and avoid repeated API calls.
        """
        mock_response = mock.Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {}  # No ClinVar association
        mock_request.return_value = mock_response

        # First call - should hit the API
        result1 = await get_associated_clinvar_allele_id("CA_NO_CLINVAR")
        assert result1 == ""
        assert mock_request.call_count == 1

        # Second call - should hit cache (no new API call)
        result2 = await get_associated_clinvar_allele_id("CA_NO_CLINVAR")
        assert result2 == ""
        assert mock_request.call_count == 1  # Still 1, not 2

    @pytest.mark.asyncio
    async def test_different_allele_ids_cached_separately(self, mock_request, clear_cache):
        """Verify different allele IDs have separate cache entries."""
        # Mock responses for different allele IDs
        mock_response1 = mock.Mock()
        mock_response1.status_code = 200
        mock_response1.json.return_value = {"externalRecords": {"ClinVarAlleles": [{"alleleId": "111111"}]}}

        mock_response2 = mock.Mock()
        mock_response2.status_code = 200
        mock_response2.json.return_value = {"externalRecords": {"ClinVarAlleles": [{"alleleId": "222222"}]}}

        mock_request.side_effect = [mock_response1, mock_response2]

        # Call with two different allele IDs
        result1 = await get_associated_clinvar_allele_id("CA_SEPARATE_1")
        result2 = await get_associated_clinvar_allele_id("CA_SEPARATE_2")

        # Both should have made API calls (different cache keys)
        assert result1 == "111111"
        assert result2 == "222222"
        assert mock_request.call_count == 2

        # Reset side_effect for subsequent calls
        mock_request.side_effect = None

        # Calling again with same IDs should hit cache (no new calls)
        result1_cached = await get_associated_clinvar_allele_id("CA_SEPARATE_1")
        result2_cached = await get_associated_clinvar_allele_id("CA_SEPARATE_2")

        assert result1_cached == "111111"
        assert result2_cached == "222222"
        assert mock_request.call_count == 2  # Still 2, no new calls

    @pytest.mark.asyncio
    async def test_api_errors_not_cached(self, mock_request, clear_cache):
        """Verify that API error responses are NOT cached.

        This is important - if we cache errors, a temporary API failure
        would prevent successful retries. Now that we raise exceptions,
        the exception prevents caching and allows retries.
        """
        # First call returns error
        mock_error_response = mock.Mock()
        mock_error_response.status_code = 500
        mock_error_response.raise_for_status.side_effect = requests.exceptions.HTTPError("500 Server Error")
        mock_request.return_value = mock_error_response

        # First call - API error raises exception
        with pytest.raises(requests.exceptions.HTTPError):
            await get_associated_clinvar_allele_id("CA_ERROR_TEST")
        assert mock_request.call_count == 1

        # Mock successful response for retry
        mock_success_response = mock.Mock()
        mock_success_response.status_code = 200
        mock_success_response.raise_for_status.return_value = None  # No exception on success
        mock_success_response.json.return_value = {"externalRecords": {"ClinVarAlleles": [{"alleleId": "777777"}]}}
        mock_request.return_value = mock_success_response

        # Second call - should retry API (error was not cached)
        result2 = await get_associated_clinvar_allele_id("CA_ERROR_TEST")
        assert result2 == "777777"
        assert mock_request.call_count == 2  # New API call was made

    @pytest.mark.asyncio
    async def test_rate_limit_errors_not_cached(self, mock_request, clear_cache):
        """Verify that 429 rate limit errors are NOT cached.

        Rate limiting is a transient condition - we should retry after
        the rate limit window expires, not cache the failure.
        """
        # First call returns rate limit error
        mock_error_response = mock.Mock()
        mock_error_response.status_code = 429
        mock_error_response.raise_for_status.side_effect = requests.exceptions.HTTPError("429 Too Many Requests")
        mock_request.return_value = mock_error_response

        # First call - rate limit error raises exception
        with pytest.raises(requests.exceptions.HTTPError):
            await get_associated_clinvar_allele_id("CA_RATE_LIMIT_TEST")
        assert mock_request.call_count == 1

        # Mock successful response for retry (after rate limit window)
        mock_success_response = mock.Mock()
        mock_success_response.status_code = 200
        mock_success_response.raise_for_status.return_value = None
        mock_success_response.json.return_value = {"externalRecords": {"ClinVarAlleles": [{"alleleId": "429429"}]}}
        mock_request.return_value = mock_success_response

        # Second call - should retry API (rate limit error was not cached)
        result2 = await get_associated_clinvar_allele_id("CA_RATE_LIMIT_TEST")
        assert result2 == "429429"
        assert mock_request.call_count == 2  # New API call was made

    @pytest.mark.asyncio
    async def test_service_unavailable_errors_not_cached(self, mock_request, clear_cache):
        """Verify that 503 service unavailable errors are NOT cached.

        Service unavailability is a transient condition - the service
        may recover, so we should allow retries rather than caching the failure.
        """
        # First call returns service unavailable error
        mock_error_response = mock.Mock()
        mock_error_response.status_code = 503
        mock_error_response.raise_for_status.side_effect = requests.exceptions.HTTPError("503 Service Unavailable")
        mock_request.return_value = mock_error_response

        # First call - service unavailable error raises exception
        with pytest.raises(requests.exceptions.HTTPError):
            await get_associated_clinvar_allele_id("CA_SERVICE_UNAVAILABLE_TEST")
        assert mock_request.call_count == 1

        # Mock successful response for retry (after service recovers)
        mock_success_response = mock.Mock()
        mock_success_response.status_code = 200
        mock_success_response.raise_for_status.return_value = None
        mock_success_response.json.return_value = {"externalRecords": {"ClinVarAlleles": [{"alleleId": "503503"}]}}
        mock_request.return_value = mock_success_response

        # Second call - should retry API (service unavailable error was not cached)
        result2 = await get_associated_clinvar_allele_id("CA_SERVICE_UNAVAILABLE_TEST")
        assert result2 == "503503"
        assert mock_request.call_count == 2  # New API call was made

    @pytest.mark.asyncio
    async def test_different_functions_share_raw_data_cache(self, mock_request, clear_cache):
        """Verify different API functions share the underlying allele data cache.

        Since all functions delegate to get_clingen_allele_data, calling one function
        caches the raw response, and subsequent calls for the same allele ID reuse it
        without making additional API calls.
        """
        mock_response = mock.Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "transcriptAlleles": [
                {"MANE": True, "@id": "https://reg.genome.network/allele/PA99999"},
            ],
            "externalRecords": {"ClinVarAlleles": [{"alleleId": "888888"}]},
        }
        mock_request.return_value = mock_response

        # First call fetches from API
        result1 = await get_canonical_pa_ids("CA_SHARED_CACHE_TEST")
        # Second call reuses cached raw data — no new API call
        result2 = await get_associated_clinvar_allele_id("CA_SHARED_CACHE_TEST")

        assert result1 == ["PA99999"]
        assert result2 == "888888"
        assert mock_request.call_count == 1  # Only one API call for both functions


@pytest.mark.unit
@mock.patch("mavedb.lib.clingen.allele_registry.requests.get")
class TestCacheBackendFailure:
    """Verify cache backend failures are bypassed rather than surfaced as errors.

    aiocache's cached decorator wraps cache reads and writes in try/except internally,
    returning None on read failure (treated as a cache miss) and silently logging write
    failures. These tests document that contract so a future aiocache upgrade or backend
    change doesn't silently break it.
    """

    @pytest.mark.asyncio
    async def test_cache_read_failure_falls_through_to_api(self, mock_request, clear_cache):
        """A failing cache read is treated as a cache miss — the real API is called."""
        mock_response = mock.Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"externalRecords": {"ClinVarAlleles": [{"alleleId": "111111"}]}}
        mock_request.return_value = mock_response

        with mock.patch.object(get_clingen_allele_data.cache, "get", side_effect=Exception("Redis unavailable")):  # type: ignore
            result = await get_associated_clinvar_allele_id("CA_CACHE_READ_FAIL")

        assert result == "111111"
        assert mock_request.call_count == 1

    @pytest.mark.asyncio
    async def test_cache_write_failure_still_returns_result(self, mock_request, clear_cache):
        """A failing cache write does not propagate — the API result is still returned."""
        mock_response = mock.Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"externalRecords": {"ClinVarAlleles": [{"alleleId": "222222"}]}}
        mock_request.return_value = mock_response

        with mock.patch.object(get_clingen_allele_data.cache, "set", side_effect=Exception("Redis unavailable")):  # type: ignore
            result = await get_associated_clinvar_allele_id("CA_CACHE_WRITE_FAIL")

        assert result == "222222"
        assert mock_request.call_count == 1

    @pytest.mark.asyncio
    async def test_cache_fully_down_calls_api_every_time(self, mock_request, clear_cache):
        """When both reads and writes fail, every call goes to the API (no caching)."""
        mock_response = mock.Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"externalRecords": {"ClinVarAlleles": [{"alleleId": "333333"}]}}
        mock_request.return_value = mock_response

        with (
            mock.patch.object(get_clingen_allele_data.cache, "get", side_effect=Exception("Redis unavailable")),  # type: ignore
            mock.patch.object(get_clingen_allele_data.cache, "set", side_effect=Exception("Redis unavailable")),  # type: ignore
        ):
            result1 = await get_associated_clinvar_allele_id("CA_CACHE_DOWN")
            result2 = await get_associated_clinvar_allele_id("CA_CACHE_DOWN")

        assert result1 == "333333"
        assert result2 == "333333"
        assert mock_request.call_count == 2  # No caching — both calls hit the API
