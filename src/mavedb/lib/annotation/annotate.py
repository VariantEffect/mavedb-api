"""
This module supports the construction of three main VA-Spec data structures based on the MaveDB MappedVariant object:
- StudyResult
    See: https://va-ga4gh.readthedocs.io/en/latest/modeling-foundations/data-structures.html#study-result-structure
- Statement
    See: https://va-ga4gh.readthedocs.io/en/latest/modeling-foundations/data-structures.html#statement-structure
- VariantPathogenicityStatement
    See: https://va-spec.ga4gh.org/en/latest/va-standard-profiles/community-profiles/acmg-2015-profiles.html#variant-pathogenicity-statement-acmg-2015
"""

from typing import Optional, Sequence, TypeVar, Union

from ga4gh.va_spec.acmg_2015 import VariantPathogenicityStatement
from ga4gh.va_spec.base.core import ExperimentalVariantFunctionalImpactStudyResult, Statement

from mavedb.lib.annotation.classification import functional_classification_of_variant
from mavedb.lib.annotation.evidence_line import acmg_evidence_line, functional_evidence_line
from mavedb.lib.annotation.exceptions import MappingDataDoesntExistException
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
    calibration_scope_extension,
    calibrations_available_for_annotation,
    can_annotate_variant_for_functional_statement,
    can_annotate_variant_for_pathogenicity_evidence,
    select_strongest_functional_calibration,
    select_strongest_pathogenicity_calibration,
)
from mavedb.lib.permissions.principal import Principal
from mavedb.models.mapped_variant import MappedVariant
from mavedb.models.score_calibration import ScoreCalibration

Annotation = TypeVar(
    "Annotation", ExperimentalVariantFunctionalImpactStudyResult, Statement, VariantPathogenicityStatement
)


def _disclosing_calibration_scope(annotation: Annotation, calibrations: Sequence[ScoreCalibration]) -> Annotation:
    """Record on the annotation which principal it was built for.

    Applied at the top-level entry points only. Nested study results and statements built as components of
    an evidence line inherit the scope of the object that contains them.
    """
    # model_copy rather than assigning to `.extensions`: mypy resolves the field's element type to a
    # `ga4gh.va_spec.base.core.Extension` that does not exist at runtime (the ga4gh namespace packages
    # confuse its import resolution), so a direct assignment is a false positive.
    return annotation.model_copy(
        update={"extensions": [*(annotation.extensions or []), calibration_scope_extension(calibrations)]}
    )


def variant_study_result(mapped_variant: MappedVariant) -> ExperimentalVariantFunctionalImpactStudyResult:
    # A study result reports the measured score and carries no calibration-derived evidence, so its scope
    # is public regardless of viewer. Disclosed anyway, so that a missing scope never has to be read as
    # "public" or "generated before disclosure existed".
    return _disclosing_calibration_scope(mapped_variant_to_experimental_variant_impact_study_result(mapped_variant), [])


def variant_functional_impact_statement(
    mapped_variant: MappedVariant,
    allow_research_use_only_calibrations: bool = False,
    principal: Optional[Principal] = None,
) -> Optional[Statement]:
    if not can_annotate_variant_for_functional_statement(
        mapped_variant, allow_research_use_only_calibrations=allow_research_use_only_calibrations, principal=principal
    ):
        return None

    study_result = mapped_variant_to_experimental_variant_impact_study_result(mapped_variant)
    functional_proposition = mapped_variant_to_experimental_variant_functional_impact_proposition(mapped_variant)

    eligible_calibrations = calibrations_available_for_annotation(
        mapped_variant,
        "functional",
        allow_research_use_only_calibrations=allow_research_use_only_calibrations,
        principal=principal,
    )

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

    return _disclosing_calibration_scope(
        mapped_variant_to_functional_statement(
            mapped_variant, functional_proposition, functional_evidence, strongest_calibration, classification
        ),
        eligible_calibrations,
    )


def variant_pathogenicity_statement(
    mapped_variant: MappedVariant,
    allow_research_use_only_calibrations: bool = False,
    principal: Optional[Principal] = None,
) -> Optional[VariantPathogenicityStatement]:
    if not can_annotate_variant_for_pathogenicity_evidence(
        mapped_variant, allow_research_use_only_calibrations=allow_research_use_only_calibrations, principal=principal
    ):
        return None

    study_result = mapped_variant_to_experimental_variant_impact_study_result(mapped_variant)
    functional_proposition = mapped_variant_to_experimental_variant_functional_impact_proposition(mapped_variant)
    clinical_proposition = mapped_variant_to_experimental_variant_clinical_impact_proposition(mapped_variant)

    eligible_calibrations = calibrations_available_for_annotation(
        mapped_variant,
        "pathogenicity",
        allow_research_use_only_calibrations=allow_research_use_only_calibrations,
        principal=principal,
    )

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

    return _disclosing_calibration_scope(
        mapped_variant_to_pathogenicity_statement(
            mapped_variant, clinical_proposition, clinical_evidence, strongest_calibration, strongest_range
        ),
        eligible_calibrations,
    )


def variant_highest_level_annotation(
    mapped_variant: MappedVariant,
    principal: Optional[Principal] = None,
) -> Optional[Union[ExperimentalVariantFunctionalImpactStudyResult, Statement, VariantPathogenicityStatement]]:
    """
    Build the single highest-materialized VA-Spec layer for a mapped variant.

    Layer ladder (highest to lowest): pathogenicity statement -> functional impact statement -> study result.
    Returns None when the variant has no post-mapped allele and therefore cannot be annotated.

    The viewer decides which layer is reachable as well as what the layer contains: a variant whose only
    calibration is invisible to this principal degrades to a study result rather than yielding a statement
    with nothing in it.
    """
    try:
        if can_annotate_variant_for_pathogenicity_evidence(mapped_variant, principal=principal):
            return variant_pathogenicity_statement(mapped_variant, principal=principal)
        if can_annotate_variant_for_functional_statement(mapped_variant, principal=principal):
            return variant_functional_impact_statement(mapped_variant, principal=principal)
        return variant_study_result(mapped_variant)
    except MappingDataDoesntExistException:
        return None
