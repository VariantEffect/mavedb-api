"""
Pytest fixtures for annotation testing.

This module provides specialized fixtures for testing MaveDB annotation functionality,
including mock objects with proper calibrations and configurations.
"""

import pytest

from tests.helpers.mocks.factories import (
    create_mock_mapped_variant,
    create_mock_mapped_variant_with_functional_calibration_score_set,
    create_mock_mapped_variant_with_pathogenicity_calibration_score_set,
)


@pytest.fixture
def mock_mapped_variant():
    """Override main fixture with properly configured mock for annotation tests."""
    return create_mock_mapped_variant(clingen_allele_id="CA123456")


@pytest.fixture
def mock_mapped_variant_with_functional_calibration_score_set():
    """Fixture for mock mapped variant with functional calibration score set."""
    return create_mock_mapped_variant_with_functional_calibration_score_set(clingen_allele_id="CA123456")


@pytest.fixture
def mock_mapped_variant_with_pathogenicity_calibration_score_set():
    """Fixture for mock mapped variant with pathogenicity calibration score set."""
    return create_mock_mapped_variant_with_pathogenicity_calibration_score_set(clingen_allele_id="CA123456")
