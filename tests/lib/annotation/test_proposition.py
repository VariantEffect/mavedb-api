import pytest  # noqa: F401
from unittest.mock import MagicMock
from ga4gh.vrs.models import Allele

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
    assert result.description == "Variant pathogenicity proposition for variant123."
    assert isinstance(result.subjectVariant, Allele)
    assert result.predicate == "hasAssayVariantEffectFor"
    assert result.objectCondition.conceptType == "Absent"
    assert result.objectCondition.primaryCoding.code == "Absent"
    assert result.objectCondition.primaryCoding.system == "Absent"


def test_mapped_variant_to_experimental_variant_functional_impact_proposition():
    mock_mapped_variant = MagicMock()
    mock_mapped_variant.variant.urn = "variant123"
    mock_mapped_variant.variant.score_set.experiment = MagicMock()
    mock_mapped_variant.post_mapped = {
        "variation": {
            "location": {
                "sequence_id": "seq123",
                "interval": {
                    "start": {"value": 100},
                    "end": {"value": 200},
                },
            }
        }
    }

    result = mapped_variant_to_experimental_variant_functional_impact_proposition(mock_mapped_variant)

    assert isinstance(result, ExperimentalVariantFunctionalImpactProposition)
    assert result.description == "Variant functional impact proposition for variant123."
    assert isinstance(result.subjectVariant, Allele)
    assert result.predicate == "impactsFunctionOf"
    assert result.objectSequenceFeature.root == "placeholder"
    assert result.experimentalContextQualifier is not None
