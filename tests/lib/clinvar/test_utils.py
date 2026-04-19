import csv
import gzip
import io
from datetime import datetime

import pytest
import requests

from mavedb.lib.clinvar.constants import CLINVAR_FIELDS_TO_KEEP
from mavedb.lib.clinvar.utils import (
    fetch_clinvar_variant_data,
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

    session = _Session()
    session.headers = {}
    return session


def _make_gzipped_tsv(text: str) -> bytes:
    buf = io.BytesIO()
    with gzip.GzipFile(fileobj=buf, mode="wb") as gz:
        gz.write(text.encode("utf-8"))
    return buf.getvalue()


# Minimal valid TSV content with the required columns for parsing
MOCK_TSV_CONTENT = _make_gzipped_tsv(
    "#AlleleID\tGeneSymbol\tClinicalSignificance\tReviewStatus\n"
    "123\tBRCA1\tPathogenic\treviewed by expert panel\n"
    "456\tTP53\tBenign\tcriteria provided, single submitter\n"
)


@pytest.mark.unit
class TestValidateClinvarVariantSummaryDate:
    def test_valid_past_date(self):
        validate_clinvar_variant_summary_date(2, 2015)

    def test_valid_current_month_and_year(self):
        now = datetime.now()
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
            return

        future_month = now.month + 1
        with pytest.raises(ValueError, match="Cannot fetch ClinVar data for future months."):
            validate_clinvar_variant_summary_date(future_month, now.year)


class MockResponse:
    def __init__(self, content, status_code=200, raise_exc=None):
        self.content = content
        self.status_code = status_code
        self._raise_exc = raise_exc

    def raise_for_status(self):
        if self._raise_exc:
            raise self._raise_exc


@pytest.mark.unit
class TestFetchClinvarVariantData:
    @pytest.mark.asyncio
    async def test_top_level_url_success(self, monkeypatch, tmp_path):
        monkeypatch.setattr("mavedb.lib.clinvar.utils.CLINVAR_CACHE_DIR", tmp_path)

        def mock_get(url, **kwargs):
            return MockResponse(MOCK_TSV_CONTENT)

        monkeypatch.setattr("mavedb.lib.clinvar.utils._ncbi_session", lambda: _mock_session(mock_get))
        result = await fetch_clinvar_variant_data(1, 2016)

        assert "123" in result
        assert "456" in result
        assert result["123"]["GeneSymbol"] == "BRCA1"
        assert result["456"]["ClinicalSignificance"] == "Benign"

    @pytest.mark.asyncio
    async def test_falls_back_to_archive_url(self, monkeypatch, tmp_path):
        monkeypatch.setattr("mavedb.lib.clinvar.utils.CLINVAR_CACHE_DIR", tmp_path)

        call_count = {"count": 0}

        def mock_get(url, **kwargs):
            call_count["count"] += 1
            if call_count["count"] == 1:
                return MockResponse(b"", status_code=404, raise_exc=requests.exceptions.HTTPError("404"))
            return MockResponse(MOCK_TSV_CONTENT)

        monkeypatch.setattr("mavedb.lib.clinvar.utils._ncbi_session", lambda: _mock_session(mock_get))
        result = await fetch_clinvar_variant_data(2, 2017)

        assert "123" in result
        assert call_count["count"] == 2

    @pytest.mark.asyncio
    async def test_both_urls_fail_raises(self, monkeypatch, tmp_path):
        monkeypatch.setattr("mavedb.lib.clinvar.utils.CLINVAR_CACHE_DIR", tmp_path)

        def mock_get(url, **kwargs):
            raise requests.RequestException("Not found")

        monkeypatch.setattr("mavedb.lib.clinvar.utils._ncbi_session", lambda: _mock_session(mock_get))
        with pytest.raises(requests.RequestException, match="Not found"):
            await fetch_clinvar_variant_data(3, 2018)

    @pytest.mark.asyncio
    async def test_invalid_date_raises(self, monkeypatch, tmp_path):
        monkeypatch.setattr("mavedb.lib.clinvar.utils.CLINVAR_CACHE_DIR", tmp_path)

        with pytest.raises(ValueError, match="Month must be an integer between 1 and 12."):
            await fetch_clinvar_variant_data(0, 2020)

    @pytest.mark.asyncio
    async def test_cache_hit_skips_network(self, monkeypatch, tmp_path):
        monkeypatch.setattr("mavedb.lib.clinvar.utils.CLINVAR_CACHE_DIR", tmp_path)

        call_count = {"count": 0}

        def mock_get(url, **kwargs):
            call_count["count"] += 1
            return MockResponse(MOCK_TSV_CONTENT)

        monkeypatch.setattr("mavedb.lib.clinvar.utils._ncbi_session", lambda: _mock_session(mock_get))

        result1 = await fetch_clinvar_variant_data(5, 2020)
        assert call_count["count"] == 1

        result2 = await fetch_clinvar_variant_data(5, 2020)
        assert call_count["count"] == 1  # No new network call
        assert result1 == result2

    @pytest.mark.asyncio
    async def test_only_keeps_configured_fields(self, monkeypatch, tmp_path):
        monkeypatch.setattr("mavedb.lib.clinvar.utils.CLINVAR_CACHE_DIR", tmp_path)

        tsv_with_extra_cols = _make_gzipped_tsv(
            "#AlleleID\tGeneSymbol\tClinicalSignificance\tReviewStatus\tExtraCol\n"
            "789\tBRCA2\tLikely pathogenic\tno assertion\tignored\n"
        )

        def mock_get(url, **kwargs):
            return MockResponse(tsv_with_extra_cols)

        monkeypatch.setattr("mavedb.lib.clinvar.utils._ncbi_session", lambda: _mock_session(mock_get))
        result = await fetch_clinvar_variant_data(7, 2022)

        assert set(result["789"].keys()) == set(CLINVAR_FIELDS_TO_KEEP)
        assert "ExtraCol" not in result["789"]

    @pytest.mark.asyncio
    async def test_handles_large_csv_fields(self, monkeypatch, tmp_path):
        monkeypatch.setattr("mavedb.lib.clinvar.utils.CLINVAR_CACHE_DIR", tmp_path)

        large_field = "A" * (csv.field_size_limit() + 100)
        tsv = _make_gzipped_tsv(
            f"#AlleleID\tGeneSymbol\tClinicalSignificance\tReviewStatus\n999\t{large_field}\tBenign\tok\n"
        )

        def mock_get(url, **kwargs):
            return MockResponse(tsv)

        monkeypatch.setattr("mavedb.lib.clinvar.utils._ncbi_session", lambda: _mock_session(mock_get))
        result = await fetch_clinvar_variant_data(8, 2023)

        assert result["999"]["GeneSymbol"] == large_field

    @pytest.mark.asyncio
    async def test_does_not_alter_csv_field_size_limit(self, monkeypatch, tmp_path):
        monkeypatch.setattr("mavedb.lib.clinvar.utils.CLINVAR_CACHE_DIR", tmp_path)

        default_limit = csv.field_size_limit()

        def mock_get(url, **kwargs):
            return MockResponse(MOCK_TSV_CONTENT)

        monkeypatch.setattr("mavedb.lib.clinvar.utils._ncbi_session", lambda: _mock_session(mock_get))
        await fetch_clinvar_variant_data(9, 2023)

        assert csv.field_size_limit() == default_limit

    @pytest.mark.asyncio
    async def test_stale_cache_removed_on_fields_change(self, monkeypatch, tmp_path):
        """When CLINVAR_FIELDS_TO_KEEP changes (different hash), the old pickle is deleted."""
        monkeypatch.setattr("mavedb.lib.clinvar.utils.CLINVAR_CACHE_DIR", tmp_path)

        # Create a fake stale cache file with a different hash
        stale_file = tmp_path / "variant_summary_2020-10.parsed.deadbeef.pkl"
        stale_file.write_bytes(b"stale")

        def mock_get(url, **kwargs):
            return MockResponse(MOCK_TSV_CONTENT)

        monkeypatch.setattr("mavedb.lib.clinvar.utils._ncbi_session", lambda: _mock_session(mock_get))
        await fetch_clinvar_variant_data(10, 2020)

        assert not stale_file.exists()
        pkl_files = list(tmp_path.glob("variant_summary_2020-10.parsed.*.pkl"))
        assert len(pkl_files) == 1
