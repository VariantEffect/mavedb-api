"""Tests for mavedb.lib.mondo: MONDO term resolution, search, and MappableConcept serialization."""

# ruff: noqa: E402

import pytest

pytest.importorskip("psycopg2")

import httpx

from mavedb.lib.exceptions import MondoServiceError
from mavedb.lib.mondo import (
    MONDO_GENERIC_CODE,
    MONDO_GENERIC_LABEL,
    MONDO_SYSTEM,
    fetch_mondo_term,
    find_or_create_mondo_term,
    generic_disease_mappable_concept,
    get_generic_disease_term,
    mondo_iri,
    mondo_suggestion_to_mappable_concept,
    mondo_term_to_mappable_concept,
    search_mondo,
)
from mavedb.lib.validation.exceptions import ValidationError
from mavedb.models.mondo_term import MondoTerm


class _FakeResponse:
    def __init__(self, payload):
        self._payload = payload

    def raise_for_status(self):
        pass

    def json(self):
        return self._payload


class _FakeAsyncClient:
    """Minimal stand-in for httpx.AsyncClient returning a canned OLS payload."""

    def __init__(self, payload):
        self._payload = payload

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False

    async def get(self, url, params=None):
        return _FakeResponse(self._payload)


class _FailingAsyncClient:
    """Stand-in for httpx.AsyncClient whose request fails, as when OLS is unreachable."""

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False

    async def get(self, url, params=None):
        raise httpx.ConnectError("ols down")


def _ols_payload(docs):
    return {"response": {"docs": docs}}


@pytest.mark.unit
class TestMondoSerialization:
    def test_mondo_iri_replaces_curie_colon(self):
        assert mondo_iri("MONDO:0015263") == "https://purl.obolibrary.org/obo/MONDO_0015263"

    def test_term_to_mappable_concept(self):
        term = MondoTerm(
            code="MONDO:0015263", system=MONDO_SYSTEM, system_version="2026-01-01", label="Brugada syndrome"
        )
        concept = mondo_term_to_mappable_concept(term)
        assert concept.conceptType == "Disease"
        assert concept.name == "Brugada syndrome"
        assert concept.primaryCoding.code.root == "MONDO:0015263"
        assert concept.primaryCoding.system == MONDO_SYSTEM
        assert concept.primaryCoding.iris[0].root == "https://purl.obolibrary.org/obo/MONDO_0015263"

    def test_generic_concept_is_the_mondo_root(self):
        concept = generic_disease_mappable_concept()
        assert concept.primaryCoding.code.root == MONDO_GENERIC_CODE
        assert concept.name == MONDO_GENERIC_LABEL

    def test_suggestion_to_mappable_concept(self):
        concept = mondo_suggestion_to_mappable_concept(
            {"code": "MONDO:0015263", "label": "Brugada syndrome", "iri": "https://example.org/MONDO_0015263"}
        )
        assert concept.primaryCoding.code.root == "MONDO:0015263"
        assert concept.primaryCoding.iris[0].root == "https://example.org/MONDO_0015263"


@pytest.mark.asyncio
class TestMondoOlsSearch:
    async def test_search_mondo_parses_ols_docs(self, monkeypatch):
        payload = _ols_payload(
            [
                {
                    "obo_id": "MONDO:0015263",
                    "label": "Brugada syndrome",
                    "iri": "https://purl.obolibrary.org/obo/MONDO_0015263",
                },
                {"obo_id": "MONDO:0000001", "label": "disease or disorder"},  # missing iri → derived
            ]
        )
        monkeypatch.setattr(httpx, "AsyncClient", lambda *a, **k: _FakeAsyncClient(payload))

        results = await search_mondo("brugada")
        assert results[0] == {
            "code": "MONDO:0015263",
            "label": "Brugada syndrome",
            "iri": "https://purl.obolibrary.org/obo/MONDO_0015263",
        }
        assert results[1]["iri"] == mondo_iri("MONDO:0000001")

    async def test_search_mondo_empty_query_skips_request(self, monkeypatch):
        # No network expected; if a request were made the fake would still be needed, so assert on the shortcut.
        assert await search_mondo("   ") == []

    async def test_fetch_mondo_term_returns_none_when_absent(self, monkeypatch):
        monkeypatch.setattr(httpx, "AsyncClient", lambda *a, **k: _FakeAsyncClient(_ols_payload([])))
        assert await fetch_mondo_term("MONDO:9999999") is None

    async def test_search_mondo_wraps_http_errors_in_service_error(self, monkeypatch):
        monkeypatch.setattr(httpx, "AsyncClient", lambda *a, **k: _FailingAsyncClient())

        with pytest.raises(MondoServiceError):
            await search_mondo("brugada")

    async def test_fetch_mondo_term_wraps_http_errors_in_service_error(self, monkeypatch):
        monkeypatch.setattr(httpx, "AsyncClient", lambda *a, **k: _FailingAsyncClient())

        with pytest.raises(MondoServiceError):
            await fetch_mondo_term("MONDO:0015263")


@pytest.mark.asyncio
class TestFindOrCreateMondoTerm:
    async def test_creates_from_ols_and_is_idempotent(self, session, monkeypatch):
        async def fake_fetch(code):
            return {"code": code, "label": "Brugada syndrome", "iri": mondo_iri(code)}

        monkeypatch.setattr("mavedb.lib.mondo.fetch_mondo_term", fake_fetch)

        term = await find_or_create_mondo_term(session, "MONDO:0015263")
        assert term.code == "MONDO:0015263"
        assert term.label == "Brugada syndrome"
        assert term.system == MONDO_SYSTEM

        # A second call reuses the row rather than creating a duplicate.
        again = await find_or_create_mondo_term(session, "MONDO:0015263")
        assert again.id == term.id
        assert session.query(MondoTerm).filter(MondoTerm.code == "MONDO:0015263").count() == 1

    async def test_generic_code_resolves_without_ols(self, session, monkeypatch):
        async def fail_fetch(code):
            raise AssertionError("OLS should not be consulted for the generic term")

        monkeypatch.setattr("mavedb.lib.mondo.fetch_mondo_term", fail_fetch)
        term = await find_or_create_mondo_term(session, MONDO_GENERIC_CODE)
        assert term.code == MONDO_GENERIC_CODE
        assert term.label == MONDO_GENERIC_LABEL

    async def test_rejects_unknown_code(self, session, monkeypatch):
        async def fake_fetch(code):
            return None

        monkeypatch.setattr("mavedb.lib.mondo.fetch_mondo_term", fake_fetch)
        with pytest.raises(ValidationError):
            await find_or_create_mondo_term(session, "MONDO:9999999")

    async def test_ols_outage_propagates_service_error(self, session, monkeypatch):
        # A controlled term is never persisted unvalidated: an OLS outage surfaces as MondoServiceError.
        async def fake_fetch(code):
            raise MondoServiceError("ols down")

        monkeypatch.setattr("mavedb.lib.mondo.fetch_mondo_term", fake_fetch)
        with pytest.raises(MondoServiceError):
            await find_or_create_mondo_term(session, "MONDO:0015263")


@pytest.mark.unit
class TestGenericDiseaseTerm:
    def test_get_generic_disease_term_is_idempotent(self, session):
        first = get_generic_disease_term(session)
        assert first.code == MONDO_GENERIC_CODE
        assert first.system == MONDO_SYSTEM
        second = get_generic_disease_term(session)
        assert second.id == first.id
        assert session.query(MondoTerm).filter(MondoTerm.code == MONDO_GENERIC_CODE).count() == 1
