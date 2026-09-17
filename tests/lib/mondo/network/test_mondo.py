# ruff: noqa: E402
"""Network tests for the MONDO/OLS integration. Require a live connection to www.ebi.ac.uk (OLS4).

The unit tests in ``tests/lib/mondo/test_mondo.py`` mock OLS, so these are the only exposure to real upstream
behavior — a canary for changes to OLS4's endpoints, response shape, or term identity. Sockets are
blocked for unmarked tests, so these run only under the ``network`` marker and are not part of the
default suite.
"""

import pytest

# starlette is required for logging context functionality pulled in by mavedb.lib.mondo.
pytest.importorskip("starlette")

from mavedb.lib.mondo import MONDO_GENERIC_CODE, fetch_mondo_term, search_mondo

# Brugada syndrome: a stable, long-established MONDO disease term used as a known-good fixture.
BRUGADA_CODE = "MONDO:0015263"


@pytest.mark.network
@pytest.mark.asyncio
class TestMondoOlsNetwork:
    """Canary tests exercising the real OLS4 service."""

    async def test_search_returns_known_term_with_expected_shape(self):
        # /select endpoint: a known query surfaces the expected code, in the {code, label, iri} shape.
        results = await search_mondo("brugada syndrome")
        assert results, "OLS returned no results for a known disease query."
        assert BRUGADA_CODE in {result["code"] for result in results}
        first = results[0]
        assert first["code"].startswith("MONDO:")
        assert first["label"]
        assert first["iri"].startswith("http")

    async def test_search_broad_prefix_returns_results(self):
        # A two-letter prefix must still return via /select — /search timed out on these (the #754 switch).
        assert await search_mondo("ca")

    async def test_fetch_known_code_returns_canonical_term(self):
        # /search exact-obo_id lookup: a known code resolves to its canonical label.
        resolved = await fetch_mondo_term(BRUGADA_CODE)
        assert resolved is not None
        assert resolved["code"] == BRUGADA_CODE
        assert "brugada" in resolved["label"].lower()

    async def test_fetch_generic_root_resolves(self):
        resolved = await fetch_mondo_term(MONDO_GENERIC_CODE)
        assert resolved is not None
        assert resolved["code"] == MONDO_GENERIC_CODE

    async def test_fetch_unknown_code_returns_none(self):
        # The not-found path must stay a clean None so validation can reject unknown codes.
        assert await fetch_mondo_term("MONDO:9999999") is None
