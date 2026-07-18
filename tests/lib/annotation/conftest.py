"""
Pytest fixtures for annotation testing.

This module provides specialized fixtures for testing MaveDB annotation functionality,
including mock objects with proper calibrations and configurations.
"""

from types import SimpleNamespace

import pytest

from mavedb.lib.annotation.context import VariantAnnotationContext
from mavedb.lib.vrs import vrs_object_from_mapped_variant
from tests.helpers.mocks.factories import (
    create_mock_mapped_variant,
    create_mock_mapped_variant_with_functional_calibration_score_set,
    create_mock_mapped_variant_with_pathogenicity_calibration_score_set,
)


def annotation_context_for(mapped_variant, subject_variant=None) -> VariantAnnotationContext:
    """Build a DB-free ``VariantAnnotationContext`` around a mock mapped variant.

    The annotation builders read a context's ``variant`` (scores/calibrations), ``record`` (VRS-mapper
    provenance), ``measured_allele`` (study-result focus + ClinGen IRI), and ``subject_variant`` (the VA
    proposition subject). We synthesize the record/allele from the mock's mapping fields; the subject
    defaults to the concrete measured variation, matching a non-projected variant. Mutating
    ``context.variant`` mutates the underlying mock variant, since it is the same object.
    """
    record = SimpleNamespace(
        mapping_api_version=mapped_variant.mapping_api_version,
        mapped_date=mapped_variant.mapped_date,
    )
    measured_allele = SimpleNamespace(
        post_mapped=mapped_variant.post_mapped,
        clingen_allele_id=mapped_variant.clingen_allele_id,
    )
    return VariantAnnotationContext(
        variant=mapped_variant.variant,
        record=record,
        measured_allele=measured_allele,
        subject_variant=subject_variant or vrs_object_from_mapped_variant(mapped_variant.post_mapped),
        as_of=None,
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


@pytest.fixture
def mock_annotation_context(mock_mapped_variant) -> VariantAnnotationContext:
    """A DB-free annotation context for an uncalibrated variant."""
    return annotation_context_for(mock_mapped_variant)


@pytest.fixture
def mock_annotation_context_with_functional_calibration_score_set(
    mock_mapped_variant_with_functional_calibration_score_set,
) -> VariantAnnotationContext:
    """A DB-free annotation context whose variant carries a functional calibration."""
    return annotation_context_for(mock_mapped_variant_with_functional_calibration_score_set)


@pytest.fixture
def mock_annotation_context_with_pathogenicity_calibration_score_set(
    mock_mapped_variant_with_pathogenicity_calibration_score_set,
) -> VariantAnnotationContext:
    """A DB-free annotation context whose variant carries a pathogenicity calibration."""
    return annotation_context_for(mock_mapped_variant_with_pathogenicity_calibration_score_set)
