from ga4gh.va_spec.base.domain_entities import Condition

from mavedb.lib.mondo import mondo_term_to_mappable_concept
from mavedb.models.score_calibration import ScoreCalibration


def calibration_disease_condition(score_calibration: ScoreCalibration) -> Condition:
    """The disease/disorder a calibration applies to, as a VA-Spec ``Condition``.

    Every calibration carries a non-null MONDO disease term (often the generic "disease or
    disorder" (``MONDO:0000001``)) which is serialized directly to a condition.
    """
    return Condition(root=mondo_term_to_mappable_concept(score_calibration.disease_term))
