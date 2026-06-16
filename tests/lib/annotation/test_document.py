# ruff: noqa: E402

"""
Tests for mavedb.lib.annotation.document module.

This module tests document creation functions for experiments, score sets, variants,
and mapped variants, ensuring proper IRI generation and document metadata.
"""

import urllib.parse

import pytest

pytest.importorskip("psycopg2")


from ga4gh.va_spec.base.core import Document

from mavedb.lib.annotation.document import (
    experiment_as_iri,
    experiment_to_document,
    mapped_variant_as_iri,
    mapped_variant_to_document,
    score_calibration_as_document,
    score_set_as_iri,
    score_set_to_document,
    variant_as_iri,
    variant_to_document,
)
from tests.helpers.mocks.factories import create_mock_score_calibration

BASE_URL = "https://mavedb.org"


@pytest.mark.unit
class TestExperimentDocumentFunctions:
    """Unit tests for experiment document creation functions."""

    def test_experiment_as_iri(self, mock_experiment):
        """Test IRI generation for experiments."""
        expected_iri_root = f"{BASE_URL}/experiments/{mock_experiment.urn}"
        result = experiment_as_iri(mock_experiment)

        assert result.root == expected_iri_root

    def test_experiment_to_document(self, mock_experiment):
        """Test document creation for experiments."""
        document = experiment_to_document(mock_experiment)

        assert document.id == mock_experiment.urn
        assert document.name == "MaveDB Experiment"
        assert document.title == mock_experiment.title
        assert document.description == mock_experiment.short_description
        assert document.documentType == "experiment"
        assert len(document.urls) > 0
        assert experiment_as_iri(mock_experiment).root in document.urls


@pytest.mark.unit
class TestScoreSetDocumentFunctions:
    """Unit tests for score set document creation functions."""

    def test_score_set_as_iri(self, mock_score_set):
        """Test IRI generation for score sets."""
        expected_iri_root = f"{BASE_URL}/score-sets/{mock_score_set.urn}"
        result = score_set_as_iri(mock_score_set)

        assert result.root == expected_iri_root

    def test_score_set_to_document(self, mock_score_set):
        """Test document creation for score sets."""
        document = score_set_to_document(mock_score_set)

        assert document.id == mock_score_set.urn
        assert document.name == "MaveDB Score Set"
        assert document.title == mock_score_set.title
        assert document.description == mock_score_set.short_description
        assert document.documentType == "score set"
        assert len(document.urls) > 0
        assert score_set_as_iri(mock_score_set).root in document.urls


@pytest.mark.unit
class TestMappedVariantDocumentFunctions:
    """Unit tests for mapped variant document creation functions."""

    def test_mapped_variant_as_iri(self, mock_mapped_variant):
        """Test IRI generation for mapped variants with ClinGen allele ID."""
        expected_iri_root = (
            f"https://mavedb.org/variant/{urllib.parse.quote_plus(mock_mapped_variant.clingen_allele_id)}"
        )
        result = mapped_variant_as_iri(mock_mapped_variant)

        assert result.root == expected_iri_root

    def test_mapped_variant_as_iri_no_caid(self, mock_mapped_variant):
        """Test IRI generation for mapped variants without ClinGen allele ID returns None."""
        mock_mapped_variant.clingen_allele_id = None
        result = mapped_variant_as_iri(mock_mapped_variant)

        assert result is None

    def test_mapped_variant_to_document(self, mock_mapped_variant):
        """Test document creation for mapped variants with ClinGen allele ID."""
        document = mapped_variant_to_document(mock_mapped_variant)

        assert document.id == mock_mapped_variant.variant.urn
        assert document.name == "MaveDB Mapped Variant"
        assert document.documentType == "mapped genomic variant description"
        assert len(document.urls) > 0
        assert mapped_variant_as_iri(mock_mapped_variant).root in document.urls

    def test_mapped_variant_to_document_no_caid(self, mock_mapped_variant):
        """Test document creation for mapped variants without ClinGen allele ID returns None."""
        mock_mapped_variant.clingen_allele_id = None
        document = mapped_variant_to_document(mock_mapped_variant)

        assert document is None


@pytest.mark.unit
class TestVariantDocumentFunctions:
    """Unit tests for variant document creation functions."""

    def test_variant_as_iri(self, mock_variant):
        """Test IRI generation for variants."""
        expected_iri_root = f"https://mavedb.org/score-sets/{mock_variant.score_set.urn}?variant={urllib.parse.quote_plus(mock_variant.urn)}"
        result = variant_as_iri(mock_variant)

        assert result.root == expected_iri_root

    def test_variant_to_document(self, mock_variant):
        """Test document creation for variants."""
        document = variant_to_document(mock_variant)

        assert document.id == mock_variant.urn
        assert document.name == "MaveDB Variant"
        assert document.documentType == "genomic variant description"
        assert len(document.urls) > 0
        assert variant_as_iri(mock_variant).root in document.urls


@pytest.mark.unit
class TestScoreCalibrationDocumentFunctions:
    """Unit tests for score calibration document creation."""

    def test_includes_baseline_score_extension_when_present(self, mock_score_set):
        calibration = create_mock_score_calibration(score_set=mock_score_set, baseline_score=1.5)
        document = score_calibration_as_document(calibration)

        baseline = [ext for ext in document.extensions if ext.name == "Baseline score"]
        assert len(baseline) == 1
        assert baseline[0].value == 1.5

    def test_omits_baseline_score_extension_when_none_and_round_trips(self, mock_score_set):
        # Extension.value is required, so a None-valued extension is dropped by exclude_none and
        # would no longer re-parse. The builder must omit the extension entirely instead.
        calibration = create_mock_score_calibration(score_set=mock_score_set, baseline_score=None)
        document = score_calibration_as_document(calibration)

        assert all(ext.name != "Baseline score" for ext in document.extensions)
        # Regression guard: the exclude_none serialization must round-trip back through the model.
        Document(**document.model_dump(exclude_none=True))
