"""Resolving each row's calibration interpretations for a CSV export.

Shared by both exports, and kept out of ``columns`` because filling these cells needs the database.
"""

from typing import Callable, Optional, Sequence

from sqlalchemy import select
from sqlalchemy.orm import Session, selectinload

from mavedb.lib.annotation.flatten import FlatAnnotation, flatten_annotation
from mavedb.lib.csv.entries import visible_calibrations
from mavedb.models.mapped_variant import MappedVariant
from mavedb.models.score_calibration import ScoreCalibration
from mavedb.models.score_calibration_functional_classification import ScoreCalibrationFunctionalClassification
from mavedb.models.score_calibration_functional_classification_variant_association import (
    score_calibration_functional_classification_variants_association_table,
)
from mavedb.models.variant import Variant


def calibrations_for_namespaces(
    db: Session,
    calibration_namespaces: dict[str, str],
    may_read_calibration: Optional[Callable[[ScoreCalibration], bool]] = None,
) -> dict[str, ScoreCalibration]:
    """Load the calibrations named by the requested namespaces, keyed by namespace.

    Looked up by the URN the caller named, not by what a score set offers: the namespace *is* the request.
    Which means this, not discovery, is the gate — naming a private calibration's URN directly must not
    serve its interpretation, so *may_read_calibration* is applied here too.
    """
    if not calibration_namespaces:
        return {}

    calibrations = db.scalars(
        select(ScoreCalibration)
        .where(ScoreCalibration.urn.in_(list(calibration_namespaces.values())))
        .options(
            selectinload(ScoreCalibration.functional_classifications).selectinload(
                ScoreCalibrationFunctionalClassification.acmg_classification
            )
        )
    ).all()

    by_urn = {
        str(calibration.urn): calibration for calibration in visible_calibrations(calibrations, may_read_calibration)
    }
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


def annotations_for_rows(
    db: Session,
    variants: Sequence[Variant],
    mappings: Sequence[Optional[MappedVariant]],
    calibration_namespaces: dict[str, str],
    may_read_calibration: Optional[Callable[[ScoreCalibration], bool]] = None,
) -> Optional[list[dict[str, Optional[FlatAnnotation]]]]:
    """Flatten every row's interpretation under each requested calibration namespace.

    A calibration from a different score set than the row leaves that namespace empty: a score from one
    assay carries no meaning under another's thresholds. So does one the caller may not read.

    Returns:
        None when no calibration namespace was requested, so the caller can skip the work entirely.
    """
    if not calibration_namespaces:
        return None

    calibrations_by_ns = calibrations_for_namespaces(db, calibration_namespaces, may_read_calibration)
    # TODO(#372): non-null id fields
    membership = containing_classification_ids(db, [variant.id for variant in variants])  # type: ignore

    rows: list[dict[str, Optional[FlatAnnotation]]] = []
    for variant, mapping in zip(variants, mappings):
        # TODO(#372): non-null id fields
        contained = membership.get(variant.id, set())  # type: ignore
        annotations: dict[str, Optional[FlatAnnotation]] = {}
        for namespace in calibration_namespaces:
            calibration = calibrations_by_ns.get(namespace)
            if mapping is None or calibration is None or calibration.score_set_id != variant.score_set_id:
                annotations[namespace] = None
            else:
                annotations[namespace] = flatten_annotation(mapping, calibration, contained)
        rows.append(annotations)

    return rows
