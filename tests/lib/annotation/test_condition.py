"""
Tests for mavedb.lib.annotation.condition module.

This module tests functions for creating GA4GH Condition objects and IRIs,
specifically for generic disease conditions used in variant annotations.
"""

import pytest
from ga4gh.core.models import Coding, MappableConcept
from ga4gh.core.models import iriReference as IRI
from ga4gh.va_spec.base.domain_entities import Condition

from mavedb.lib.annotation.condition import generic_disease_condition, generic_disease_condition_iri
from mavedb.lib.annotation.constants import GENERIC_DISEASE_MEDGEN_CODE, MEDGEN_SYSTEM


@pytest.mark.unit
class TestGenericDiseaseConditionIriUnit:
    """Unit tests for generic_disease_condition_iri function."""

    def test_returns_correct_iri_structure(self):
        """Test that function returns proper IRI object with expected root."""
        iri = generic_disease_condition_iri()

        assert isinstance(iri, IRI)
        expected_root = f"http://identifiers.org/medgen/{GENERIC_DISEASE_MEDGEN_CODE}"
        assert iri.root == expected_root

    def test_uses_correct_medgen_code(self):
        """Test that function uses the correct MedGen code from constants."""
        iri = generic_disease_condition_iri()

        assert GENERIC_DISEASE_MEDGEN_CODE in iri.root
        assert iri.root.endswith(GENERIC_DISEASE_MEDGEN_CODE)

    def test_uses_correct_iri_format(self):
        """Test that IRI follows the expected identifiers.org format."""
        iri = generic_disease_condition_iri()

        assert iri.root.startswith("http://identifiers.org/medgen/")
        assert "identifiers.org" in iri.root

    def test_iri_consistency(self):
        """Test that multiple calls return consistent IRI values."""
        iri1 = generic_disease_condition_iri()
        iri2 = generic_disease_condition_iri()

        assert iri1.root == iri2.root


@pytest.mark.unit
class TestGenericDiseaseConditionUnit:
    """Unit tests for generic_disease_condition function."""

    def test_returns_correct_condition_structure(self):
        """Test that function returns proper Condition object."""
        condition = generic_disease_condition()

        assert isinstance(condition, Condition)
        assert hasattr(condition, "root")
        assert isinstance(condition.root, MappableConcept)

    def test_concept_type_is_disease(self):
        """Test that condition has correct conceptType."""
        condition = generic_disease_condition()

        assert condition.root.conceptType == "Disease"

    def test_primary_coding_structure(self):
        """Test that primary coding has correct structure and values."""
        condition = generic_disease_condition()
        coding = condition.root.primaryCoding

        assert isinstance(coding, Coding)
        assert coding.code.root == GENERIC_DISEASE_MEDGEN_CODE
        assert coding.system == MEDGEN_SYSTEM

    def test_coding_iris_structure(self):
        """Test that coding includes correct IRI list."""
        condition = generic_disease_condition()
        coding = condition.root.primaryCoding

        assert isinstance(coding.iris, list)
        assert len(coding.iris) == 1

        iri = coding.iris[0]
        assert isinstance(iri, IRI)

    def test_coding_iri_matches_helper_function(self):
        """Test that coding IRI matches the output of helper function."""
        condition = generic_disease_condition()
        coding = condition.root.primaryCoding
        expected_iri = generic_disease_condition_iri()

        assert len(coding.iris) == 1
        assert coding.iris[0].root == expected_iri.root

    def test_uses_correct_constants(self):
        """Test that condition uses the correct constant values."""
        condition = generic_disease_condition()
        coding = condition.root.primaryCoding

        assert coding.code.root == GENERIC_DISEASE_MEDGEN_CODE
        assert coding.system == MEDGEN_SYSTEM

    def test_iri_contains_medgen_code(self):
        """Test that IRI contains the correct MedGen code."""
        condition = generic_disease_condition()
        coding = condition.root.primaryCoding
        iri_root = coding.iris[0].root

        expected_iri_content = f"http://identifiers.org/medgen/{GENERIC_DISEASE_MEDGEN_CODE}"
        assert iri_root == expected_iri_content

    def test_condition_consistency(self):
        """Test that multiple calls return consistent condition structures."""
        condition1 = generic_disease_condition()
        condition2 = generic_disease_condition()

        assert condition1.root.conceptType == condition2.root.conceptType
        assert condition1.root.primaryCoding.code == condition2.root.primaryCoding.code
        assert condition1.root.primaryCoding.system == condition2.root.primaryCoding.system
        assert condition1.root.primaryCoding.iris[0].root == condition2.root.primaryCoding.iris[0].root


@pytest.mark.unit
class TestConditionConsistencyUnit:
    """Unit tests for cross-function consistency in condition helpers."""

    def test_iri_function_integration_with_condition(self):
        """Test that standalone IRI function produces same result as condition's IRI."""
        standalone_iri = generic_disease_condition_iri()
        condition = generic_disease_condition()
        condition_iri = condition.root.primaryCoding.iris[0]

        assert standalone_iri.root == condition_iri.root

    def test_complete_condition_structure_integration(self):
        """Test the complete condition structure matches expected GA4GH format."""
        condition = generic_disease_condition()

        # Verify complete structure
        assert condition.root.conceptType == "Disease"
        assert condition.root.primaryCoding.code.root == GENERIC_DISEASE_MEDGEN_CODE
        assert condition.root.primaryCoding.system == MEDGEN_SYSTEM
        assert len(condition.root.primaryCoding.iris) == 1

        # Verify IRI structure
        iri = condition.root.primaryCoding.iris[0]
        expected_iri_root = f"http://identifiers.org/medgen/{GENERIC_DISEASE_MEDGEN_CODE}"
        assert iri.root == expected_iri_root
