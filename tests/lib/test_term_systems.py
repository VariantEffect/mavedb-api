"""Tests for the shared (system, prefix) -> Coding helper."""

import pytest

from mavedb.lib.term_systems import coding


@pytest.mark.unit
def test_coding_builds_id_from_prefix_and_code():
    result = coding(("https://example.org/system", "ex"), "some_code")

    assert result.id == "ex:some_code"
    assert result.code.root == "some_code"
    assert result.system == "https://example.org/system"
