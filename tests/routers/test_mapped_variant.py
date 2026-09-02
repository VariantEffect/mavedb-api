# ruff: noqa: E402
"""Router tests for the retired ``/mapped-variants`` surface.

The router itself is gone (see ``tests/routers/test_variant_annotation.py``); what remains at this
prefix is a set of 301 redirects onto the routes' new home under ``/variants``, kept so external
callers hitting the old public paths get pointed at the replacement instead of a bare 404.
"""

import pytest

arq = pytest.importorskip("arq")
cdot = pytest.importorskip("cdot")
fastapi = pytest.importorskip("fastapi")

from urllib.parse import quote, quote_plus


TEST_URN = "urn:mavedb:00000001-a-1#1"


def test_redirect_mapped_variant(client):
    response = client.get(f"/api/v1/mapped-variants/{quote_plus(TEST_URN)}", follow_redirects=False)

    assert response.status_code == 301
    assert response.headers["location"] == f"/api/v1/variants/{quote_plus(TEST_URN)}"


def test_redirect_carries_rfc8594_deprecation_headers(client):
    """A client that follows the 301 silently still learns the resource is deprecated, and where it went."""
    response = client.get(f"/api/v1/mapped-variants/{quote_plus(TEST_URN)}", follow_redirects=False)

    assert response.headers["Deprecation"] == "true"
    assert 'rel="successor-version"' in response.headers["Link"]
    assert f"/api/v1/variants/{quote_plus(TEST_URN)}" in response.headers["Link"]
    assert response.headers["Warning"].startswith("299 - ")
    # Sunset stays unset until a removal date is ratified (deprecation.DEFAULT_SUNSET is None).
    assert "Sunset" not in response.headers


@pytest.mark.parametrize(
    "suffix",
    ["va/study-result", "va/functional-statement", "va/pathogenicity-statement"],
)
def test_redirect_mapped_variant_va_routes(client, suffix):
    response = client.get(f"/api/v1/mapped-variants/{quote_plus(TEST_URN)}/{suffix}", follow_redirects=False)

    assert response.status_code == 301
    assert response.headers["location"] == f"/api/v1/variants/{quote_plus(TEST_URN)}/{suffix}"


def test_redirect_mapped_variants_by_identifier(client):
    identifier = "ga4gh:VA.0123456789abcdefghijklmnopqrstuv"
    response = client.get(
        f"/api/v1/mapped-variants/vrs/{identifier}?only_current=false",
        follow_redirects=False,
    )

    assert response.status_code == 301
    assert response.headers["location"] == f"/api/v1/variants/vrs/{quote(identifier, safe='')}?only_current=false"


def test_redirect_mapped_variants_by_identifier_rejects_malformed_identifier(client):
    response = client.get("/api/v1/mapped-variants/vrs/not-a-valid-identifier", follow_redirects=False)

    assert response.status_code == 422
