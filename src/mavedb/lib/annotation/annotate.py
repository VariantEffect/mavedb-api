"""
This module supports the construction of three main VA-Spec data structures based on the MaveDB MappedVariant object:
- StudyResult
    See: https://va-ga4gh.readthedocs.io/en/latest/modeling-foundations/data-structures.html#study-result-structure
- Statement
    See: https://va-ga4gh.readthedocs.io/en/latest/modeling-foundations/data-structures.html#statement-structure
- EvidenceLine
    See: https://va-ga4gh.readthedocs.io/en/latest/modeling-foundations/data-structures.html#evidence-line-structure
"""

from typing import Optional

from ga4gh.va_spec.acmg_2015 import VariantPathogenicityEvidenceLine
from ga4gh.va_spec.base.core import ExperimentalVariantFunctionalImpactStudyResult, Statement

from mavedb.lib.annotation.evidence_line import acmg_evidence_line, functional_evidence_line
from mavedb.lib.annotation.proposition import (
    mapped_variant_to_experimental_variant_clinical_impact_proposition,
    mapped_variant_to_experimental_variant_functional_impact_proposition,
)
from mavedb.lib.annotation.statement import mapped_variant_to_functional_statement
from mavedb.lib.annotation.study_result import mapped_variant_to_experimental_variant_impact_study_result
from mavedb.lib.annotation.util import (
    can_annotate_variant_for_pathogenicity_evidence,
    can_annotate_variant_for_functional_statement,
)
from mavedb.models.mapped_variant import MappedVariant


def variant_study_result(mapped_variant: MappedVariant) -> ExperimentalVariantFunctionalImpactStudyResult:
    return mapped_variant_to_experimental_variant_impact_study_result(mapped_variant)


def variant_functional_impact_statement(mapped_variant: MappedVariant) -> Optional[Statement]:
    if not can_annotate_variant_for_functional_statement(mapped_variant):
        return None

    # TODO#494: Add support for multiple functional evidence lines. If a score set has multiple ranges
    #           associated with it, we should create one evidence line for each range.
    study_result = mapped_variant_to_experimental_variant_impact_study_result(mapped_variant)
    functional_evidence = functional_evidence_line(mapped_variant, [study_result])
    functional_proposition = mapped_variant_to_experimental_variant_functional_impact_proposition(mapped_variant)

    return mapped_variant_to_functional_statement(mapped_variant, functional_proposition, [functional_evidence])


def variant_pathogenicity_evidence(
    mapped_variant: MappedVariant,
) -> Optional[VariantPathogenicityEvidenceLine]:
    if not can_annotate_variant_for_pathogenicity_evidence(mapped_variant):
        return None

    study_result = mapped_variant_to_experimental_variant_impact_study_result(mapped_variant)
    functional_impact = variant_functional_impact_statement(mapped_variant)

    supporting_evidence = functional_impact if functional_impact else study_result

    # TODO#494: Add support for multiple clinical evidence lines. If a score set has multiple calibrations
    #           associated with it, we should create one evidence line for each calibration.
    clinical_proposition = mapped_variant_to_experimental_variant_clinical_impact_proposition(mapped_variant)
    clinical_evidence = acmg_evidence_line(mapped_variant, clinical_proposition, [supporting_evidence.model_dump()])

    return clinical_evidence
