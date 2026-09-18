"""Tests for the /diseases/search MONDO typeahead endpoint."""

# ruff: noqa: E402

import pytest

pytest.importorskip("arq")
pytest.importorskip("cdot")
pytest.importorskip("fastapi")

from mavedb.lib.exceptions import MondoServiceError


@pytest.fixture
def mock_mondo_search(monkeypatch):
    async def fake_search(query, limit=20):
        return [
            {
                "code": "MONDO:0015263",
                "label": "Brugada syndrome",
                "iri": "https://purl.obolibrary.org/obo/MONDO_0015263",
            }
        ]

    monkeypatch.setattr("mavedb.routers.diseases.search_mondo", fake_search)


def test_search_diseases_returns_mappable_concepts(client, mock_mondo_search):
    response = client.get("/api/v1/diseases/search", params={"q": "brugada"})

    assert response.status_code == 200
    data = response.json()
    assert len(data) == 1
    assert data[0]["conceptType"] == "Disease"
    assert data[0]["name"] == "Brugada syndrome"
    assert data[0]["primaryCoding"]["code"] == "MONDO:0015263"


def test_search_diseases_requires_a_query(client):
    assert client.get("/api/v1/diseases/search").status_code == 422


def test_search_diseases_surfaces_ontology_outage(client, monkeypatch):
    async def failing_search(query, limit=20):
        raise MondoServiceError("Disease ontology service temporarily unavailable")

    monkeypatch.setattr("mavedb.routers.diseases.search_mondo", failing_search)

    response = client.get("/api/v1/diseases/search", params={"q": "brugada"})
    assert response.status_code == 503
    assert "Disease ontology service temporarily unavailable" in response.json()["message"]
