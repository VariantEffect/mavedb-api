"""
This module supports the construction of three main VA-Spec data structures based on the MaveDB MappedVariant object:
- StudyResult
    See: https://va-ga4gh.readthedocs.io/en/latest/modeling-foundations/data-structures.html#study-result-structure
- Statement
    See: https://va-ga4gh.readthedocs.io/en/latest/modeling-foundations/data-structures.html#statement-structure
- VariantPathogenicityStatement
    See: https://va-spec.ga4gh.org/en/latest/va-standard-profiles/community-profiles/acmg-2015-profiles.html#variant-pathogenicity-statement-acmg-2015
"""

from typing import Optional

from ga4gh.va_spec.acmg_2015 import VariantPathogenicityStatement
from ga4gh.va_spec.base.core import ExperimentalVariantFunctionalImpactStudyResult, Statement

from mavedb.lib.annotation.classification import functional_classification_of_variant
from mavedb.lib.annotation.evidence_line import acmg_evidence_line, functional_evidence_line
from mavedb.lib.annotation.proposition import (
    mapped_variant_to_experimental_variant_clinical_impact_proposition,
    mapped_variant_to_experimental_variant_functional_impact_proposition,
)
from mavedb.lib.annotation.statement import (
    mapped_variant_to_functional_statement,
    mapped_variant_to_pathogenicity_statement,
)
from mavedb.lib.annotation.study_result import mapped_variant_to_experimental_variant_impact_study_result
from mavedb.lib.annotation.util import (
    can_annotate_variant_for_functional_statement,
    can_annotate_variant_for_pathogenicity_evidence,
    score_calibration_may_be_used_for_annotation,
    select_strongest_functional_calibration,
    select_strongest_pathogenicity_calibration,
)
from mavedb.models.mapped_variant import MappedVariant


def variant_study_result(mapped_variant: MappedVariant) -> ExperimentalVariantFunctionalImpactStudyResult:
    return mapped_variant_to_experimental_variant_impact_study_result(mapped_variant)


def variant_functional_impact_statement(
    mapped_variant: MappedVariant, allow_research_use_only_calibrations: bool = False
) -> Optional[Statement]:
    if not can_annotate_variant_for_functional_statement(
        mapped_variant, allow_research_use_only_calibrations=allow_research_use_only_calibrations
    ):
        return None

    study_result = mapped_variant_to_experimental_variant_impact_study_result(mapped_variant)
    functional_proposition = mapped_variant_to_experimental_variant_functional_impact_proposition(mapped_variant)

    # Collect eligible calibrations
    eligible_calibrations = []
    for score_calibration in mapped_variant.variant.score_set.score_calibrations:
        if score_calibration_may_be_used_for_annotation(
            score_calibration,
            annotation_type="functional",
            allow_research_use_only_calibrations=allow_research_use_only_calibrations,
        ):
            eligible_calibrations.append(score_calibration)

    # Select the calibration with the strongest evidence
    strongest_calibration, strongest_range = select_strongest_functional_calibration(
        mapped_variant, eligible_calibrations
    )

    if not strongest_calibration:
        return None

    # Get the classification from the strongest range
    # If strongest_range is None, the variant is not in any range, so classification will be INDETERMINATE
    _, classification = functional_classification_of_variant(mapped_variant, strongest_calibration)

    # Build evidence lines for all eligible calibrations
    functional_evidence = []
    for score_calibration in eligible_calibrations:
        functional_evidence.append(functional_evidence_line(mapped_variant, score_calibration, [study_result]))

    return mapped_variant_to_functional_statement(
        mapped_variant, functional_proposition, functional_evidence, strongest_calibration, classification
    )


def variant_pathogenicity_statement(
    mapped_variant: MappedVariant, allow_research_use_only_calibrations: bool = False
) -> Optional[VariantPathogenicityStatement]:
    if not can_annotate_variant_for_pathogenicity_evidence(
        mapped_variant, allow_research_use_only_calibrations=allow_research_use_only_calibrations
    ):
        return None

    study_result = mapped_variant_to_experimental_variant_impact_study_result(mapped_variant)
    functional_proposition = mapped_variant_to_experimental_variant_functional_impact_proposition(mapped_variant)
    clinical_proposition = mapped_variant_to_experimental_variant_clinical_impact_proposition(mapped_variant)

    # Collect eligible calibrations
    eligible_calibrations = []
    for score_calibration in mapped_variant.variant.score_set.score_calibrations:
        if score_calibration_may_be_used_for_annotation(
            score_calibration,
            annotation_type="pathogenicity",
            allow_research_use_only_calibrations=allow_research_use_only_calibrations,
        ):
            eligible_calibrations.append(score_calibration)

    # Select the calibration with the strongest evidence
    strongest_calibration, strongest_range = select_strongest_pathogenicity_calibration(
        mapped_variant, eligible_calibrations
    )

    if not strongest_calibration:
        return None

    # Get the classification from the strongest range (used for the functional statement within clinical evidence)
    # If strongest_range is None, the variant is not in any range, so classification will be INDETERMINATE
    _, classification = functional_classification_of_variant(mapped_variant, strongest_calibration)

    # Note: strongest_range is used in the pathogenicity statement for ACMG classification
    # If None, the statement will use UNCERTAIN_SIGNIFICANCE

    # Build evidence lines for all eligible calibrations
    clinical_evidence = []
    for score_calibration in eligible_calibrations:
        functional_evidence = functional_evidence_line(mapped_variant, score_calibration, [study_result])
        functional_statement = mapped_variant_to_functional_statement(
            mapped_variant, functional_proposition, [functional_evidence], score_calibration, classification
        )
        clinical_evidence.append(
            acmg_evidence_line(mapped_variant, score_calibration, clinical_proposition, [functional_statement])
        )

    return mapped_variant_to_pathogenicity_statement(
        mapped_variant, clinical_proposition, clinical_evidence, strongest_calibration, strongest_range
    )
