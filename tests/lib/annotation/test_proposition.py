# ruff: noqa: E402

"""
Tests for mavedb.lib.annotation.proposition module.

This module tests proposition creation functions for experimental variant
clinical and functional impact propositions.
"""

from types import SimpleNamespace
from unittest.mock import patch

import pytest

pytest.importorskip("psycopg2")

from ga4gh.cat_vrs.models import CategoricalVariant
from ga4gh.va_spec.base.core import (
    ExperimentalVariantFunctionalImpactProposition,
    VariantPathogenicityProposition,
)
from ga4gh.vrs.models import MolecularVariation

from mavedb.lib.annotation.exceptions import MappingDataDoesntExistException
from mavedb.lib.annotation.proposition import (
    sequence_feature_for_variant,
    variant_functional_impact_proposition,
    variant_pathogenicity_proposition,
)
from mavedb.lib.cat_vrs import build_categorical_variant
from mavedb.models.allele import Allele
from mavedb.models.mapping_record_allele import MappingRecordAllele
from tests.helpers.constants import TEST_VALID_POST_MAPPED_VRS_ALLELE
from tests.lib.annotation.conftest import annotation_context_for


def _protein_categorical_variant() -> CategoricalVariant:
    """A Cat-VRS ``CategoricalVariant`` for a protein-measured variant (defining protein + `encodes` nt).

    Built over transient links (no DB) so the proposition tests can assert the builders accept a
    categorical subject — the shape a protein assay produces under Slice 5.1.
    """
    links = [
        MappingRecordAllele(
            is_authoritative=True,
            allele=Allele(level="protein", vrs_digest="prot", post_mapped=TEST_VALID_POST_MAPPED_VRS_ALLELE),
        ),
        MappingRecordAllele(
            is_authoritative=False,
            allele=Allele(level="cdna", vrs_digest="cdna", post_mapped=TEST_VALID_POST_MAPPED_VRS_ALLELE),
        ),
    ]
    transit = build_categorical_variant(links, name="urn:mavedb:test#1")
    assert transit is not None
    return transit.categorical_variant


@pytest.mark.unit
class TestExperimentalVariantClinicalImpactProposition:
    """Unit tests for experimental variant clinical impact proposition creation."""

    def test_variant_pathogenicity_proposition(self, mock_annotation_context):
        """Test creation of pathogenicity proposition from a variant annotation context."""
        result = variant_pathogenicity_proposition(mock_annotation_context)

        assert isinstance(result, VariantPathogenicityProposition)
        assert result.description == f"Variant pathogenicity proposition for {mock_annotation_context.variant.urn}."
        assert isinstance(result.subjectVariant, MolecularVariation)
        assert result.predicate == "isCausalFor"
        assert result.objectCondition.root.conceptType == "Disease"
        assert result.objectCondition.root.primaryCoding.code.root == "C0012634"
        assert result.objectCondition.root.primaryCoding.system == "https://www.ncbi.nlm.nih.gov/medgen/"

    def test_clinical_impact_proposition_carries_a_categorical_subject(self, mock_mapped_variant):
        """A protein assay's categorical subject (Slice 5.1) is carried onto the proposition unchanged."""
        context = annotation_context_for(mock_mapped_variant, subject_variant=_protein_categorical_variant())
        result = variant_pathogenicity_proposition(context)

        assert isinstance(result.subjectVariant, CategoricalVariant)


@pytest.mark.unit
class TestExperimentalVariantFunctionalImpactProposition:
    """Unit tests for experimental variant functional impact proposition creation."""

    def test_variant_functional_impact_proposition(self, mock_annotation_context):
        """Test creation of functional impact proposition from a variant annotation context."""
        result = variant_functional_impact_proposition(mock_annotation_context)

        assert isinstance(result, ExperimentalVariantFunctionalImpactProposition)
        assert result.description == f"Variant functional impact proposition for {mock_annotation_context.variant.urn}."
        assert isinstance(result.subjectVariant, MolecularVariation)
        assert result.predicate == "impactsFunctionOf"
        assert result.objectSequenceFeature.primaryCoding.code.root == "BRCA1"
        assert result.objectSequenceFeature.primaryCoding.system == "https://www.genenames.org/"
        assert result.experimentalContextQualifier is not None

    def test_functional_impact_proposition_carries_a_categorical_subject(self, mock_mapped_variant):
        """A protein assay's categorical subject (Slice 5.1) is carried onto the proposition unchanged."""
        context = annotation_context_for(mock_mapped_variant, subject_variant=_protein_categorical_variant())
        result = variant_functional_impact_proposition(context)

        assert isinstance(result.subjectVariant, CategoricalVariant)


@pytest.mark.unit
class TestSequenceFeatureForVariantUnit:
    """sequence_feature_for_variant — co-located with the propositions that consume it."""

    def test_sequence_feature_raises_when_target_is_missing(self, mock_mapped_variant):
        with patch("mavedb.lib.annotation.proposition.target_for_variant", return_value=None):
            with pytest.raises(MappingDataDoesntExistException):
                sequence_feature_for_variant(mock_mapped_variant.variant)

    def test_sequence_feature_returns_ensembl_identifier(self, mock_mapped_variant):
        target = SimpleNamespace(mapped_hgnc_name=None, post_mapped_metadata={"x": "y"}, name="BRCA1")

        with patch("mavedb.lib.annotation.proposition.target_for_variant", return_value=target):
            with patch(
                "mavedb.lib.annotation.proposition.extract_ids_from_post_mapped_metadata",
                return_value=["ENST00000357654"],
            ):
                feature, system = sequence_feature_for_variant(mock_mapped_variant.variant)

        assert feature == "ENST00000357654"
        assert system == "https://www.ensembl.org/index.html"

    def test_sequence_feature_returns_refseq_identifier(self, mock_mapped_variant):
        target = SimpleNamespace(mapped_hgnc_name=None, post_mapped_metadata={"x": "y"}, name="BRCA1")

        with patch("mavedb.lib.annotation.proposition.target_for_variant", return_value=target):
            with patch(
                "mavedb.lib.annotation.proposition.extract_ids_from_post_mapped_metadata",
                return_value=["NM_000546.6"],
            ):
                feature, system = sequence_feature_for_variant(mock_mapped_variant.variant)

        assert feature == "NM_000546.6"
        assert system == "https://www.ncbi.nlm.nih.gov/refseq/"

    def test_sequence_feature_returns_unknown_identifier_source(self, mock_mapped_variant):
        target = SimpleNamespace(mapped_hgnc_name=None, post_mapped_metadata={"x": "y"}, name="BRCA1")

        with patch("mavedb.lib.annotation.proposition.target_for_variant", return_value=target):
            with patch(
                "mavedb.lib.annotation.proposition.extract_ids_from_post_mapped_metadata",
                return_value=["CUSTOM_ID_1"],
            ):
                feature, system = sequence_feature_for_variant(mock_mapped_variant.variant)

        assert feature == "CUSTOM_ID_1"
        assert system == "transcript or gene identifier of unknown source"

    def test_sequence_feature_falls_back_to_target_name(self, mock_mapped_variant):
        target = SimpleNamespace(mapped_hgnc_name=None, post_mapped_metadata={}, name="TP53")

        with patch("mavedb.lib.annotation.proposition.target_for_variant", return_value=target):
            with patch("mavedb.lib.annotation.proposition.extract_ids_from_post_mapped_metadata", return_value=[]):
                feature, system = sequence_feature_for_variant(mock_mapped_variant.variant)

        assert feature == "TP53"
        assert system == "https://www.mavedb.org/"

    def test_sequence_feature_raises_when_target_has_no_name_or_ids(self, mock_mapped_variant):
        target = SimpleNamespace(mapped_hgnc_name=None, post_mapped_metadata={}, name=None)

        with patch("mavedb.lib.annotation.proposition.target_for_variant", return_value=target):
            with patch("mavedb.lib.annotation.proposition.extract_ids_from_post_mapped_metadata", return_value=[]):
                with pytest.raises(MappingDataDoesntExistException):
                    sequence_feature_for_variant(mock_mapped_variant.variant)
