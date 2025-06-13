import pytest  # noqa: F401
from ga4gh.vrs.models import MolecularVariation

# filepath: src/mavedb/lib/annotation/test_proposition.py
from mavedb.lib.annotation.proposition import (
    mapped_variant_to_experimental_variant_clinical_impact_proposition,
    mapped_variant_to_experimental_variant_functional_impact_proposition,
)
from ga4gh.va_spec.base import (
    VariantPathogenicityProposition,
    ExperimentalVariantFunctionalImpactProposition,
)


def test_mapped_variant_to_experimental_variant_clinical_impact_proposition(mock_mapped_variant):
    result = mapped_variant_to_experimental_variant_clinical_impact_proposition(mock_mapped_variant)

    assert isinstance(result, VariantPathogenicityProposition)
    assert result.description == f"Variant pathogenicity proposition for {mock_mapped_variant.variant.urn}."
    assert isinstance(result.subjectVariant, MolecularVariation)
    assert result.predicate == "hasAssayVariantEffectFor"
    assert result.objectCondition.root.conceptType == "Absent"
    assert result.objectCondition.root.primaryCoding.code.root == "Absent"
    assert result.objectCondition.root.primaryCoding.system == "Absent"


def test_mapped_variant_to_experimental_variant_functional_impact_proposition(mock_mapped_variant):
    result = mapped_variant_to_experimental_variant_functional_impact_proposition(mock_mapped_variant)

    assert isinstance(result, ExperimentalVariantFunctionalImpactProposition)
    assert result.description == f"Variant functional impact proposition for {mock_mapped_variant.variant.urn}."
    assert isinstance(result.subjectVariant, MolecularVariation)
    assert result.predicate == "impactsFunctionOf"
    assert result.objectSequenceFeature.root == "placeholder"
    assert result.experimentalContextQualifier is not None
