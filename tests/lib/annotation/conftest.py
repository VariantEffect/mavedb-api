"""
Pytest fixtures for annotation testing.

This module provides specialized fixtures for testing MaveDB annotation functionality,
including mock objects with proper calibrations and configurations.
"""

from unittest.mock import Mock

import pytest

from mavedb.lib.annotation.util import CALIBRATION_SCOPE_EXTENSION_NAME
from tests.helpers.constants import PRIVATE_CALIBRATION_OWNER_ID
from tests.helpers.mocks.factories import (
    create_mock_mapped_variant,
    create_mock_mapped_variant_with_functional_calibration_score_set,
    create_mock_mapped_variant_with_pathogenicity_calibration_score_set,
)

# Permission related helpers coupled to logging context.
try:
    from .conftest_optional import *  # noqa: F403
except ImportError:
    pass


def scope_of(annotation) -> str:
    """The disclosed principal of an annotation, which every emitted object must carry."""
    scopes = [
        extension.value
        for extension in (annotation.extensions or [])
        if extension.name == CALIBRATION_SCOPE_EXTENSION_NAME
    ]
    assert len(scopes) == 1, f"expected exactly one calibration scope extension, found {scopes}"
    return scopes[0]


def make_private(mapped_variant, *, owner_id: int = PRIVATE_CALIBRATION_OWNER_ID):
    """Mark every calibration on a mapped variant's score set private, owned by ``owner_id``.

    The real permission check reads ``created_by_id`` and the owning score set's contributor list, neither
    of which the annotation mocks populate.
    """
    for calibration in mapped_variant.variant.score_set.score_calibrations:
        calibration.private = True
        calibration.created_by_id = owner_id
        calibration.score_set = Mock(contributors=[], created_by_id=owner_id, modified_by_id=owner_id)
    return mapped_variant


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
