# ruff: noqa: E402

import pytest

pytest.importorskip("arq")
pytest.importorskip("cdot")
pytest.importorskip("fastapi")


def test_search_taxonomies_unknown_to_ncbi_returns_404(client, ncbi_taxonomy_not_found):
    response = client.post("/api/v1/taxonomies/search", json={"text": "unicorn"})

    assert response.status_code == 404
    assert response.json()["detail"] == "Taxonomy with search unicorn not found in NCBI"


def test_search_taxonomies_blank_text_returns_400(client, ncbi_taxonomy_not_found):
    response = client.post("/api/v1/taxonomies/search", json={"text": "   "})

    assert response.status_code == 400
    assert response.json()["detail"] == "Search text is required"
