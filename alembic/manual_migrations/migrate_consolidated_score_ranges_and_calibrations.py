"""
To be run as part of revision af87c9953d2d
"""

import sqlalchemy as sa
from sqlalchemy.orm import Session, configure_mappers

from mavedb.models import *
from sqlalchemy.orm import Session

from mavedb.models.score_set import ScoreSet
from mavedb.view_models.score_range import ScoreSetRangesCreate, InvestigatorScoreRangesCreate, ZeibergCalibrationScoreRangesCreate, ZeibergCalibrationScoreRangeCreate


from mavedb.db.session import SessionLocal

configure_mappers()


evidence_strength_to_label = {
    -8: "BS3_VERY_STRONG",
    -4: "BS3_STRONG",
    -3: "BS3_MODERATE+",
    -2: "BS3_MODERATE",
    -1: "BS3_SUPPORTING",
    1: "PS3_SUPPORTING",
    2: "PS3_MODERATE",
    3: "PS3_MODERATE+",
    4: "PS3_STRONG",
    8: "PS3_VERY_STRONG",
}


def do_migration(db: Session):
    score_sets_with_ranges_or_calibrations = db.scalars(sa.select(ScoreSet).where(ScoreSet.score_ranges.isnot(None) | ScoreSet.score_calibrations.isnot(None))).all()

    for score_set in score_sets_with_ranges_or_calibrations:
        if score_set.score_ranges is not None:
            investigator_ranges = InvestigatorScoreRangesCreate(
                **score_set.score_ranges
            )
            for score_range in investigator_ranges.ranges:
                score_range.inclusive_lower_bound = False if score_range.range[0] is None else True
                score_range.inclusive_upper_bound = False

        else:
            investigator_ranges = None

        if score_set.score_calibrations is not None:
            thresholds = score_set.score_calibrations.get("zeiberg_calibration", {}).get("thresholds", [])
            evidence_strengths = score_set.score_calibrations.get("zeiberg_calibration", {}).get("evidence_strengths", [])
            positive_likelihood_ratios = score_set.score_calibrations.get("zeiberg_calibration", {}).get("positive_likelihood_ratios", [])
            prior_probability_pathogenicity = score_set.score_calibrations.get("zeiberg_calibration", {}).get("prior_probability_pathogenicity", None)
            parameter_sets = score_set.score_calibrations.get("zeiberg_calibration", {}).get("parameter_sets", [])

            ranges = []
            boundary_direction = -1  # Start with a negative sign to indicate the first range has the lower boundary appearing prior to the threshold
            for idx, vals in enumerate(zip(thresholds, evidence_strengths, positive_likelihood_ratios)):
                threshold, evidence_strength, positive_likelihood_ratio = vals

                if idx == 0:
                    calculated_range = (None, threshold)
                    ranges.append(ZeibergCalibrationScoreRangeCreate(
                        range=(None, threshold),
                        classification="normal" if evidence_strength < 0 else "abnormal",
                        label=str(evidence_strength),
                        evidence_strength=evidence_strength,
                        positive_likelihood_ratio=positive_likelihood_ratio,
                        inclusive_lower_bound=False,
                        inclusive_upper_bound=False,
                    ))
                elif idx == len(thresholds) - 1:
                    calculated_range = (threshold, None)
                    ranges.append(ZeibergCalibrationScoreRangeCreate(
                        range=(threshold, None),
                        classification="normal" if evidence_strength < 0 else "abnormal",
                        evidence_strength=evidence_strength,
                        label=str(evidence_strength),
                        positive_likelihood_ratio=positive_likelihood_ratio,
                        inclusive_lower_bound=True,
                        inclusive_upper_bound=False,
                    ))
                else:
                    if boundary_direction < 0:
                        calculated_range = (thresholds[idx - 1], threshold)
                    else:
                        calculated_range = (threshold, thresholds[idx + 1])

                    ranges.append(ZeibergCalibrationScoreRangeCreate(
                        range=calculated_range,
                        classification="normal" if evidence_strength < 0 else "abnormal",
                        label=str(evidence_strength),
                        evidence_strength=evidence_strength,
                        positive_likelihood_ratio=positive_likelihood_ratio,
                        inclusive_lower_bound=True,
                        inclusive_upper_bound=False,
                    ))

                # Set boundary_direction if the sign of evidence_strength flips compared to the next one
                if idx != len(evidence_strengths) - 1 and (evidence_strengths[idx + 1] * evidence_strength < 0):
                    boundary_direction = -boundary_direction

                zeiberg_calibration_ranges = ZeibergCalibrationScoreRangesCreate(
                    prior_probability_pathogenicity=prior_probability_pathogenicity,
                    parameter_sets=parameter_sets,
                    ranges=ranges,
                )
        else:
            zeiberg_calibration_ranges = None

        score_set.score_ranges = ScoreSetRangesCreate(
            investigator_provided=investigator_ranges if investigator_ranges else None,
            zeiberg_calibration=zeiberg_calibration_ranges if zeiberg_calibration_ranges else None,
        ).model_dump()
        db.add(score_set)


if __name__ == "__main__":
    db = SessionLocal()
    db.current_user = None  # type: ignore

    do_migration(db)

    db.commit()
    db.close()
