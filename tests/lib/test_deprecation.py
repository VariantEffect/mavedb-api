# ruff: noqa: E402
"""Tests for the shared RFC 8594 deprecation-header builder."""

import pytest

# mavedb.lib.deprecation imports logging.context for record_deprecated_usage, which pulls in
# fastapi (an optional "server" extra) even though the functions tested here don't need it.
pytest.importorskip("fastapi")

from mavedb.lib import deprecation
from mavedb.lib.deprecation import deprecation_headers


@pytest.mark.unit
def test_minimal_headers_are_deprecation_and_docs_link():
    headers = deprecation_headers()
    assert headers["Deprecation"] == "true"
    assert headers["Link"] == f'<{deprecation.DEPRECATION_DOCS_URL}>; rel="deprecation"'
    assert "Sunset" not in headers  # unset by default: no ratified removal date
    assert "Warning" not in headers


@pytest.mark.unit
def test_successor_is_emitted_as_a_link_alongside_the_docs_link():
    headers = deprecation_headers(successor="/api/v1/variants/x", warning="moved")
    link = headers["Link"]
    assert '</api/v1/variants/x>; rel="successor-version"' in link
    assert 'rel="deprecation"' in link
    assert headers["Warning"] == '299 - "moved"'


@pytest.mark.unit
def test_sunset_is_emitted_when_supplied():
    headers = deprecation_headers(sunset="Wed, 01 Jul 2026 00:00:00 GMT")
    assert headers["Sunset"] == "Wed, 01 Jul 2026 00:00:00 GMT"


@pytest.mark.unit
def test_docs_link_can_be_omitted():
    headers = deprecation_headers(successor="/x", docs_url=None)
    assert headers["Link"] == '</x>; rel="successor-version"'


@pytest.mark.unit
def test_no_links_at_all_omits_the_link_header():
    headers = deprecation_headers(docs_url=None)
    assert "Link" not in headers
    assert headers == {"Deprecation": "true"}
