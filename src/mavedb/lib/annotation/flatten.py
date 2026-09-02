"""Flatten a variant's VA-Spec clinical interpretation into scalar values.

The rest of this package builds nested VA-Spec structures, which are faithful to the standard but not
consumable by a spreadsheet. This projects the same classification onto flat scalars.
"""

from dataclasses import dataclass
from typing import Optional

from ga4gh.va_spec.acmg_2015 import AcmgClassification

from mavedb.lib.acmg import acmg_evidence_outcome_code
from mavedb.lib.annotation.classification import (
    functional_classification_of_variant,
    pathogenicity_classification_of_variant,
)
from mavedb.models.score_calibration import ScoreCalibration
from mavedb.models.variant import Variant


@dataclass(frozen=True)
class FlatAnnotation:
    """A single variant's clinical interpretation under one calibration, flattened to scalars.

    A field is ``None`` when the calibration cannot support it; exporters render that as NA.
    """

    functional_classification: Optional[str] = None
    """"normal", "abnormal", or "indeterminate"."""

    acmg_criterion: Optional[str] = None
    """The ACMG 2015 criterion evaluated, e.g. "PS3" or "BS3"."""

    acmg_evidence_strength: Optional[str] = None
    """Strength of evidence, e.g. "MODERATE" or "MODERATE_PLUS"; None when the criterion was not met.

    MaveDB's own enumeration, finer-grained than VA-Spec's: an M+ range reports MODERATE_PLUS here while
    its VA-Spec annotation must report moderate.
    """

    acmg_evidence_outcome_code: Optional[str] = None
    """ACMG evidence outcome code, e.g. "PS3_moderate", "PS3" (strong), or "BS3_not_met"."""

    pathogenicity_classification: Optional[str] = None
    """"PATHOGENIC", "BENIGN", or "UNCERTAIN_SIGNIFICANCE"."""

    calibration_urn: Optional[str] = None
    calibration_title: Optional[str] = None

    research_use_only: Optional[bool] = None
    """Whether the calibration is marked research use only.

    Carried so a consumer holding only exported rows can tell that a criterion came from thresholds never
    validated for clinical use.
    """


def flatten_annotation(
    variant: Variant,
    score_calibration: Optional[ScoreCalibration],
    containing_classification_ids: Optional[set[int]] = None,
) -> FlatAnnotation:
    """Flatten a variant's clinical interpretation under *score_calibration* into scalar values.

    A calibration with ranges but no ACMG classifications yields a functional classification only, matching
    the annotation layer. Evidence strength uses MaveDB's own enumeration, so MODERATE_PLUS is preserved.

    Args:
        containing_classification_ids: forwarded to the classifiers. Supply it when flattening many
            variants; the fallback loads every variant of every score range.

    Returns:
        An all-``None`` annotation when *score_calibration* is None. A calibration that exists but defines
        no ranges reports its identity and standing with no interpretation.
    """
    # No calibration means nothing to say under this namespace (e.g. it belongs to another score set).
    if score_calibration is None:
        return FlatAnnotation()

    # Rangeless: reporting identity distinguishes "defines no ranges" from "no calibration applies here".
    if not score_calibration.functional_classifications:
        return FlatAnnotation(
            calibration_urn=score_calibration.urn,
            calibration_title=score_calibration.title,
            research_use_only=bool(score_calibration.research_use_only),
        )

    _, functional_classification = functional_classification_of_variant(
        variant, score_calibration, containing_classification_ids
    )

    functional_only = FlatAnnotation(
        functional_classification=functional_classification.value,
        calibration_urn=score_calibration.urn,
        calibration_title=score_calibration.title,
        research_use_only=bool(score_calibration.research_use_only),
    )

    # No ACMG classification on any range: stop at the functional classification rather than reporting a
    # not-met PS3 the curator never asserted.
    if all(fc.acmg_classification is None for fc in score_calibration.functional_classifications):
        return functional_only

    # VA-Spec strength deliberately unused: it has already collapsed MODERATE_PLUS to MODERATE.
    containing_range, criterion, _va_spec_evidence_strength = pathogenicity_classification_of_variant(
        variant, score_calibration, containing_classification_ids
    )

    # Read the strength off the containing range, which keeps MODERATE_PLUS. Reachable in practice:
    # `points_evidence_strength_equivalent` assigns M+ to +/-3 point ranges (e.g. the Excalibr
    # calibrations).
    #
    # TODO(#XXX): move the lossy MODERATE_PLUS -> MODERATE conversion to the VA-Spec boundary instead of
    # `classification.py`, upstream of every consumer; this special case then disappears.
    native_evidence_strength = (
        containing_range.acmg_classification.evidence_strength
        if containing_range is not None and containing_range.acmg_classification is not None
        else None
    )
    evidence_strength_name = native_evidence_strength.name if native_evidence_strength is not None else None

    # `pathogenicity_classification_of_variant` returns PS3 even for variants in no range, so the range,
    # not the criterion, tells us whether evidence exists. No strength means evaluated and not met.
    if containing_range is None or evidence_strength_name is None:
        pathogenicity_classification = AcmgClassification.UNCERTAIN_SIGNIFICANCE
    elif criterion.name.startswith("B"):
        pathogenicity_classification = AcmgClassification.BENIGN
    else:
        pathogenicity_classification = AcmgClassification.PATHOGENIC

    return FlatAnnotation(
        functional_classification=functional_only.functional_classification,
        acmg_criterion=criterion.value,
        acmg_evidence_strength=evidence_strength_name,
        acmg_evidence_outcome_code=acmg_evidence_outcome_code(criterion.value, evidence_strength_name),
        pathogenicity_classification=pathogenicity_classification.name,
        calibration_urn=functional_only.calibration_urn,
        calibration_title=functional_only.calibration_title,
        research_use_only=functional_only.research_use_only,
    )
