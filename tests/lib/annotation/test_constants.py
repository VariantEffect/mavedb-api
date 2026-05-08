"""
Tests for mavedb.lib.annotation.constants module.

This module tests constant values used throughout the annotation system.
"""

# ruff: noqa: E402

import pytest

pytest.importorskip("psycopg2")

from mavedb.lib.annotation.constants import (
    GENERIC_DISEASE_MEDGEN_CODE,
    MEDGEN_SYSTEM,
)


@pytest.mark.unit
class TestAnnotationConstants:
    """Unit tests for annotation constants."""

    def test_generic_disease_medgen_code(self):
        """Test generic disease MedGen code constant."""
        assert GENERIC_DISEASE_MEDGEN_CODE == "C0012634"
        assert isinstance(GENERIC_DISEASE_MEDGEN_CODE, str)

    def test_medgen_system(self):
        """Test MedGen system URL constant."""
        expected_url = "https://www.ncbi.nlm.nih.gov/medgen/"
        assert MEDGEN_SYSTEM == expected_url
        assert isinstance(MEDGEN_SYSTEM, str)
        assert MEDGEN_SYSTEM.startswith("https://")
        assert MEDGEN_SYSTEM.endswith("/")

    def test_constants_immutability(self):
        """Test that constants are properly defined as strings."""
        # These should be string constants, not mutable objects
        assert isinstance(GENERIC_DISEASE_MEDGEN_CODE, str)
        assert isinstance(MEDGEN_SYSTEM, str)
