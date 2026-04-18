import asyncio
import csv
import gzip
import io
from datetime import datetime

import pytest
import requests

from mavedb.lib.clinvar.utils import (
    fetch_clinvar_variant_summary_tsv,
    parse_clinvar_variant_summary,
    validate_clinvar_variant_summary_date,
)


def _mock_session(mock_get):
    """Create a mock requests.Session whose .get delegates to mock_get."""

    class _Session:
        headers = {}

        def update(self, _):
            pass

        def get(self, url, **kwargs):
            return mock_get(url, **kwargs)

    # Give headers a real .update so session.headers.update(...) works
    session = _Session()
    session.headers = {}  # plain dict is fine
    return session


@pytest.mark.unit
class TestValidateClinvarVariantSummaryDate:
    def test_valid_past_date(self):
        # Should not raise for a valid past date
        validate_clinvar_variant_summary_date(2, 2015)

    def test_valid_current_month_and_year(self):
        now = datetime.now()
        # Should not raise for current month and year
        validate_clinvar_variant_summary_date(now.month, now.year)

    def test_invalid_month_low(self):
        with pytest.raises(ValueError, match="Month must be an integer between 1 and 12."):
            validate_clinvar_variant_summary_date(0, 2020)

    def test_invalid_month_high(self):
        with pytest.raises(ValueError, match="Month must be an integer between 1 and 12."):
            validate_clinvar_variant_summary_date(13, 2020)

    def test_year_before_2015(self):
        with pytest.raises(ValueError, match="ClinVar archived data is only available from February 2015 onwards."):
            validate_clinvar_variant_summary_date(6, 2014)

    def test_year_2015_before_february(self):
        with pytest.raises(ValueError, match="ClinVar archived data is only available from February 2015 onwards."):
            validate_clinvar_variant_summary_date(1, 2015)

    def test_year_in_future(self):
        future_year = datetime.now().year + 1
        with pytest.raises(ValueError, match="Cannot fetch ClinVar data for future years."):
            validate_clinvar_variant_summary_date(6, future_year)

    def test_month_in_future_for_current_year(self):
        now = datetime.now()
        if now.month == 12:
            pytest.skip("December, no future month in current year")
            return  # December, no future month in current year

        future_month = now.month + 1 if now.month < 12 else 12
        with pytest.raises(ValueError, match="Cannot fetch ClinVar data for future months."):
            validate_clinvar_variant_summary_date(future_month, now.year)


@pytest.mark.unit
class TestFetchClinvarVariantSummaryTSV:
    class MockResponse:
        def __init__(self, content, status_code=200, raise_exc=None):
            self.content = content
            self.status_code = status_code
            self._raise_exc = raise_exc

        def raise_for_status(self):
            if self._raise_exc:
                raise self._raise_exc

    @pytest.mark.asyncio
    async def test_fetch_clinvar_variant_summary_tsv_top_level_success(self, monkeypatch, tmp_path):
        # Use temporary directory for cache
        monkeypatch.setattr("mavedb.lib.clinvar.utils.CLINVAR_CACHE_DIR", tmp_path)

        # Simulate successful fetch from top-level URL
        mock_content = b"mock gzipped content"

        def mock_get(url, **kwargs):
            return self.MockResponse(mock_content)

        monkeypatch.setattr("mavedb.lib.clinvar.utils._ncbi_session", lambda: _mock_session(mock_get))
        result = await fetch_clinvar_variant_summary_tsv(1, 2016)
        assert result == mock_content

    @pytest.mark.asyncio
    async def test_fetch_clinvar_variant_summary_tsv_archive_success(self, monkeypatch, tmp_path):
        # Use temporary directory for cache
        monkeypatch.setattr("mavedb.lib.clinvar.utils.CLINVAR_CACHE_DIR", tmp_path)

        # Simulate top-level fails with HTTPError, archive succeeds
        mock_content = b"archive gzipped content"
        call_count = {"count": 0}

        def mock_get(url, **kwargs):
            call_count["count"] += 1
            if call_count["count"] == 1:
                # First call (top-level URL) should fail
                return self.MockResponse(b"", status_code=404, raise_exc=requests.exceptions.HTTPError("404 Not Found"))
            else:
                # Second call (archive URL) should succeed
                return self.MockResponse(mock_content)

        monkeypatch.setattr("mavedb.lib.clinvar.utils._ncbi_session", lambda: _mock_session(mock_get))
        result = await fetch_clinvar_variant_summary_tsv(2, 2017)
        assert result == mock_content
        assert call_count["count"] == 2  # Verify both URLs were tried

    @pytest.mark.asyncio
    async def test_fetch_clinvar_variant_summary_tsv_both_fail(self, monkeypatch, tmp_path):
        # Use temporary directory for cache
        monkeypatch.setattr("mavedb.lib.clinvar.utils.CLINVAR_CACHE_DIR", tmp_path)

        # Simulate both URLs failing
        def mock_get(url, **kwargs):
            raise requests.RequestException("Not found")

        monkeypatch.setattr("mavedb.lib.clinvar.utils._ncbi_session", lambda: _mock_session(mock_get))
        with pytest.raises(requests.RequestException, match="Not found"):
            await fetch_clinvar_variant_summary_tsv(3, 2018)

    @pytest.mark.asyncio
    async def test_fetch_clinvar_variant_summary_tsv_invalid_date(self, monkeypatch, tmp_path):
        # Use temporary directory for cache
        monkeypatch.setattr("mavedb.lib.clinvar.utils.CLINVAR_CACHE_DIR", tmp_path)

        # Should raise ValueError before any network call
        with pytest.raises(ValueError, match="Month must be an integer between 1 and 12."):
            await fetch_clinvar_variant_summary_tsv(0, 2020)

    @pytest.mark.asyncio
    async def test_fetch_clinvar_variant_summary_tsv_cache_hit(self, monkeypatch, tmp_path):
        # Use temporary directory for cache
        monkeypatch.setattr("mavedb.lib.clinvar.utils.CLINVAR_CACHE_DIR", tmp_path)

        # First call - should fetch from network and cache
        mock_content = b"cached content"
        call_count = {"count": 0}

        def mock_get(url, **kwargs):
            call_count["count"] += 1
            return self.MockResponse(mock_content)

        monkeypatch.setattr("mavedb.lib.clinvar.utils._ncbi_session", lambda: _mock_session(mock_get))

        result1 = await fetch_clinvar_variant_summary_tsv(5, 2020)
        assert result1 == mock_content
        assert call_count["count"] == 1

        # Second call - should use cached file (no network call)
        result2 = await fetch_clinvar_variant_summary_tsv(5, 2020)
        assert result2 == mock_content
        assert call_count["count"] == 1  # Should still be 1, no new network call

    @pytest.mark.asyncio
    async def test_fetch_clinvar_variant_summary_tsv_cache_expiration(self, monkeypatch, tmp_path):
        # Use temporary directory for cache
        monkeypatch.setattr("mavedb.lib.clinvar.utils.CLINVAR_CACHE_DIR", tmp_path)

        # Mock short TTL for testing
        monkeypatch.setattr("mavedb.lib.clinvar.utils.CLINVAR_TSV_CACHE_TTL", 0.1)  # 0.1 second TTL for test

        # First call - should fetch from network and cache
        mock_content_1 = b"first fetch"
        mock_content_2 = b"second fetch after expiry"
        call_count = {"count": 0}

        def mock_get(url, **kwargs):
            call_count["count"] += 1
            if call_count["count"] == 1:
                return self.MockResponse(mock_content_1)
            else:
                return self.MockResponse(mock_content_2)

        monkeypatch.setattr("mavedb.lib.clinvar.utils._ncbi_session", lambda: _mock_session(mock_get))

        result1 = await fetch_clinvar_variant_summary_tsv(6, 2021)
        assert result1 == mock_content_1
        assert call_count["count"] == 1

        # Wait for cache to expire
        await asyncio.sleep(0.2)  # Wait slightly longer than TTL

        # Second call - should re-fetch from network due to expiration
        result2 = await fetch_clinvar_variant_summary_tsv(6, 2021)
        assert result2 == mock_content_2
        assert call_count["count"] == 2  # Should be 2, cache was expired


class TestParseClinvarVariantSummary:
    def make_gzipped_tsv(self, text: str) -> bytes:
        buf = io.BytesIO()
        with gzip.GzipFile(fileobj=buf, mode="wb") as gz:
            gz.write(text.encode("utf-8"))
        return buf.getvalue()

    def test_parse_clinvar_variant_summary_basic(self):
        tsv = "#AlleleID\tGeneSymbol\tClinicalSignificance\n" "123\tBRCA1\tPathogenic\n" "456\tTP53\tBenign\n"
        gzipped = self.make_gzipped_tsv(tsv)
        result = parse_clinvar_variant_summary(gzipped)
        assert "123" in result
        assert "456" in result
        assert result["123"]["GeneSymbol"] == "BRCA1"
        assert result["456"]["ClinicalSignificance"] == "Benign"

    def test_parse_clinvar_variant_summary_missing_alleleid_column(self):
        tsv = "GeneSymbol\tClinicalSignificance\n" "BRCA1\tPathogenic\n"
        gzipped = self.make_gzipped_tsv(tsv)
        with pytest.raises(KeyError):
            parse_clinvar_variant_summary(gzipped)

    def test_parse_clinvar_variant_summary_empty_content(self):
        gzipped = self.make_gzipped_tsv("")
        parse_clinvar_variant_summary(gzipped)

    def test_parse_clinvar_variant_summary_large_field(self):
        large_field = "A" * (csv.field_size_limit() + 100)
        tsv = f"#AlleleID\tGeneSymbol\n999\t{large_field}\n"
        gzipped = self.make_gzipped_tsv(tsv)
        result = parse_clinvar_variant_summary(gzipped)
        assert result["999"]["GeneSymbol"] == large_field

    def test_parse_clinvar_variant_summary_does_not_alter_field_size_limit(self):
        default_limit = csv.field_size_limit()
        tsv = "#AlleleID\tGeneSymbol\n1\tBRCA1\n"
        gzipped = self.make_gzipped_tsv(tsv)
        parse_clinvar_variant_summary(gzipped)
        assert csv.field_size_limit() == default_limit
