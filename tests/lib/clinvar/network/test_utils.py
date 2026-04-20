from datetime import datetime

import pytest

from mavedb.lib.clinvar.utils import fetch_clinvar_variant_data


@pytest.mark.network
@pytest.mark.slow
class TestFetchClinvarVariantDataIntegration:
    @pytest.mark.asyncio
    async def test_fetch_recent_variant_data(self, monkeypatch, tmp_path):
        # Use temporary directory for cache
        monkeypatch.setattr("mavedb.lib.clinvar.utils.CLINVAR_CACHE_DIR", tmp_path)

        now = datetime.now()
        # Attempt to fetch the most recent available month (previous month)
        month = now.month - 1 if now.month > 1 else 12
        year = now.year if now.month > 1 else now.year - 1

        content = await fetch_clinvar_variant_data(month, year)
        assert content

    @pytest.mark.asyncio
    async def test_fetch_older_variant_data(self, monkeypatch, tmp_path):
        # Use temporary directory for cache
        monkeypatch.setattr("mavedb.lib.clinvar.utils.CLINVAR_CACHE_DIR", tmp_path)

        # Fetch an older known date
        content = await fetch_clinvar_variant_data(2, 2015)
        assert content
