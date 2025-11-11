
from typing import Union

import sqlalchemy as sa
from sqlalchemy.orm import Session

# SQLAlchemy needs access to all models to properly map relationships.
from mavedb.models import *

from mavedb.db.session import SessionLocal
from mavedb.models.score_set import ScoreSet
from mavedb.models.score_calibration import ScoreCalibration as ScoreCalibrationDBModel
from mavedb.models.publication_identifier import PublicationIdentifier
from mavedb.view_models.score_range import (
    ScoreRangeCreate,
    ScoreSetRangesAdminCreate,
    ZeibergCalibrationScoreRangesAdminCreate,
    ScottScoreRangesAdminCreate,
    InvestigatorScoreRangesAdminCreate,
    IGVFCodingVariantFocusGroupControlScoreRangesAdminCreate,
    IGVFCodingVariantFocusGroupMissenseScoreRangesAdminCreate,
)

score_range_kinds: dict[
    str,
    Union[
        type[ZeibergCalibrationScoreRangesAdminCreate],
        type[ScottScoreRangesAdminCreate],
        type[InvestigatorScoreRangesAdminCreate],
        type[IGVFCodingVariantFocusGroupControlScoreRangesAdminCreate],
        type[IGVFCodingVariantFocusGroupMissenseScoreRangesAdminCreate],
    ],
] = {
    "zeiberg_calibration": ZeibergCalibrationScoreRangesAdminCreate,
    "scott_calibration": ScottScoreRangesAdminCreate,
    "investigator_provided": InvestigatorScoreRangesAdminCreate,
    "cvfg_all_variants": IGVFCodingVariantFocusGroupControlScoreRangesAdminCreate,
    "cvfg_missense_variants": IGVFCodingVariantFocusGroupMissenseScoreRangesAdminCreate,
}

EVIDENCE_STRENGTH_FROM_POINTS = {
    8: "Very Strong",
    4: "Strong",
    3: "Moderate+",
    2: "Moderate",
    1: "Supporting",
}


def do_migration(session: Session) -> None:
    score_sets_with_ranges = (
        session.execute(sa.select(ScoreSet).where(ScoreSet.score_ranges.isnot(None))).scalars().all()
    )

    for score_set in score_sets_with_ranges:
        if not score_set.score_ranges:
            continue    

        score_set_ranges = ScoreSetRangesAdminCreate.model_validate(score_set.score_ranges)

        for field in score_set_ranges.model_fields_set:
            if field == "record_type":
                continue

            ranges = getattr(score_set_ranges, field)
            if not ranges:
                continue

            range_model = score_range_kinds.get(field)
            inferred_ranges = range_model.model_validate(ranges)

            model_thresholds = []
            for range in inferred_ranges.ranges:
                model_thresholds.append(ScoreRangeCreate.model_validate(range.__dict__).model_dump())

            # We should migrate the zeiberg evidence classifications to be explicitly part of the calibration ranges.
            if field == "zeiberg_calibration":
                for inferred_range, model_range in zip(
                    inferred_ranges.ranges,
                    model_thresholds,
                ):
                    model_range["label"] = f"PS3 {EVIDENCE_STRENGTH_FROM_POINTS.get(inferred_range.evidence_strength, 'Unknown')}" if inferred_range.evidence_strength > 0 else f"BS3 {EVIDENCE_STRENGTH_FROM_POINTS.get(abs(inferred_range.evidence_strength), 'Unknown')}"
                    model_range["acmg_classification"] = {"points": inferred_range.evidence_strength}

            # Reliant on existing behavior that these sources have been created already.
            # If not present, no sources will be associated.
            if "odds_path_source" in inferred_ranges.model_fields_set and inferred_ranges.odds_path_source:
                oddspaths_sources = (
                    session.execute(
                        sa.select(PublicationIdentifier).where(
                            PublicationIdentifier.identifier.in_(
                                [src.identifier for src in (inferred_ranges.odds_path_source or [])]
                            )
                        )
                    )
                    .scalars()
                    .all()
                )
            else:
                oddspaths_sources = []

            if "source" in inferred_ranges.model_fields_set and inferred_ranges.source:
                range_sources = (
                    session.execute(
                        sa.select(PublicationIdentifier).where(
                            PublicationIdentifier.identifier.in_(
                                [src.identifier for src in (inferred_ranges.source or [])]
                            )
                        )
                    )
                    .scalars()
                    .all()
                )
            else:
                range_sources = []

            sources = set()
            for publication in oddspaths_sources:
                setattr(publication, "relation", "method")
                sources.add(publication)
            for publication in range_sources:
                setattr(publication, "relation", "threshold")
                sources.add(publication)
                
            score_calibration = ScoreCalibrationDBModel(
                score_set_id=score_set.id,
                title=inferred_ranges.title,
                research_use_only=inferred_ranges.research_use_only,
                primary=inferred_ranges.primary,
                private=False, # All migrated calibrations are public.
                investigator_provided=True if field == "investigator_provided" else False,
                baseline_score=inferred_ranges.baseline_score
                if "baseline_score" in inferred_ranges.model_fields_set
                else None,
                baseline_score_description=inferred_ranges.baseline_score_description
                if "baseline_score_description" in inferred_ranges.model_fields_set
                else None,
                functional_ranges=None if not model_thresholds else model_thresholds,
                calibration_metadata=None,
                publication_identifiers=sources,
                # If investigator_provided, set to creator of score set, else set to default system user (1).
                created_by_id=score_set.created_by_id if field == "investigator_provided" else 1,
                modified_by_id=score_set.created_by_id if field == "investigator_provided" else 1,
            )
            session.add(score_calibration)


if __name__ == "__main__":
    db = SessionLocal()
    db.current_user = None  # type: ignore

    do_migration(db)

    db.commit()
    db.close()