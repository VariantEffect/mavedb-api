"""The database reads behind a CSV export's calibration interpretations, which ``sources`` turns into cells."""

from typing import Optional, Sequence

from sqlalchemy import select
from sqlalchemy.orm import Session, selectinload

from mavedb.lib.csv.entries import calibration_viewer
from mavedb.lib.permissions.score_calibration import ScoreCalibrationViewer
from mavedb.models.score_calibration import ScoreCalibration
from mavedb.models.score_calibration_functional_classification import ScoreCalibrationFunctionalClassification
from mavedb.models.score_calibration_functional_classification_variant_association import (
    score_calibration_functional_classification_variants_association_table,
)


def calibrations_for_namespaces(
    db: Session,
    calibration_namespaces: dict[str, str],
    viewer: Optional[ScoreCalibrationViewer] = None,
) -> dict[str, ScoreCalibration]:
    """Load the calibrations named by the requested namespaces, keyed by namespace.

    Looked up by the URN the caller named, not by what a score set offers: the namespace *is* the request.
    Which means this, not discovery, is the gate — naming a private calibration's URN directly must not
    serve its interpretation, so the viewer is applied here too.
    """
    if not calibration_namespaces:
        return {}

    calibrations = db.scalars(
        select(ScoreCalibration)
        .where(ScoreCalibration.urn.in_(list(calibration_namespaces.values())))
        .options(
            selectinload(ScoreCalibration.functional_classifications).selectinload(
                ScoreCalibrationFunctionalClassification.acmg_classification
            ),
            selectinload(ScoreCalibration.superseding_calibration),  # read by the superseded flag
        )
    ).all()

    by_urn = {str(calibration.urn): calibration for calibration in calibration_viewer(viewer).visible(calibrations)}
    return {namespace: by_urn[urn] for namespace, urn in calibration_namespaces.items() if urn in by_urn}


def containing_classification_ids(db: Session, variant_ids: Sequence[int]) -> dict[int, set[int]]:
    """Map each variant to the score-classification ids whose range contains it.

    One query over the association table, replacing the ORM membership check in
    ``mavedb.lib.annotation.classification`` that loads every variant of every range once per range per
    row — the dominant cost of these exports at score-set scale.
    """
    if not variant_ids:
        return {}

    membership: dict[int, set[int]] = {variant_id: set() for variant_id in variant_ids}
    rows = db.execute(
        select(
            score_calibration_functional_classification_variants_association_table.c.variant_id,
            score_calibration_functional_classification_variants_association_table.c.functional_classification_id,
        ).where(score_calibration_functional_classification_variants_association_table.c.variant_id.in_(variant_ids))
    ).all()
    for variant_id, classification_id in rows:
        membership[variant_id].add(classification_id)

    return membership
