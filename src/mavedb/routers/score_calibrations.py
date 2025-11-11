import logging

from fastapi import APIRouter, Depends, HTTPException, Query
from typing import Optional
from sqlalchemy.orm import Session

from mavedb import deps
from mavedb.lib.logging import LoggedRoute
from mavedb.lib.logging.context import (
    logging_context,
    save_to_logging_context,
)
from mavedb.lib.authentication import get_current_user, UserData
from mavedb.lib.authorization import require_current_user
from mavedb.lib.permissions import Action, assert_permission, has_permission
from mavedb.lib.score_calibrations import (
    create_score_calibration_in_score_set,
    modify_score_calibration,
    delete_score_calibration,
    demote_score_calibration_from_primary,
    promote_score_calibration_to_primary,
    publish_score_calibration,
)
from mavedb.models.score_calibration import ScoreCalibration
from mavedb.routers.score_sets import fetch_score_set_by_urn
from mavedb.view_models import score_calibration


logger = logging.getLogger(__name__)

router = APIRouter(
    prefix="/api/v1/score-calibrations",
    tags=["score-calibrations"],
    responses={404: {"description": "Not found"}},
    route_class=LoggedRoute,
)


@router.get(
    "/{urn}",
    response_model=score_calibration.ScoreCalibrationWithScoreSetUrn,
    responses={404: {}},
)
def get_score_calibration(
    *,
    urn: str,
    db: Session = Depends(deps.get_db),
    user_data: Optional[UserData] = Depends(get_current_user),
) -> ScoreCalibration:
    """
    Retrieve a score calibration by its URN.
    """
    save_to_logging_context({"requested_resource": urn})

    item = db.query(ScoreCalibration).where(ScoreCalibration.urn == urn).one_or_none()
    if not item:
        logger.debug("The requested score calibration does not exist", extra=logging_context())
        raise HTTPException(status_code=404, detail="The requested score calibration does not exist")

    assert_permission(user_data, item, Action.READ)
    return item


@router.get(
    "/score-set/{score_set_urn}",
    response_model=list[score_calibration.ScoreCalibrationWithScoreSetUrn],
    responses={404: {}},
)
async def get_score_calibrations_for_score_set(
    *,
    score_set_urn: str,
    db: Session = Depends(deps.get_db),
    user_data: Optional[UserData] = Depends(get_current_user),
) -> list[ScoreCalibration]:
    """
    Retrieve all score calibrations for a given score set URN.
    """
    save_to_logging_context({"requested_resource": score_set_urn, "resource_property": "calibrations"})
    score_set = await fetch_score_set_by_urn(db, score_set_urn, user_data, None, False)

    permitted_calibrations = [
        calibration
        for calibration in score_set.score_calibrations
        if has_permission(user_data, calibration, Action.READ).permitted
    ]
    if not permitted_calibrations:
        logger.debug("No score calibrations found for the requested score set", extra=logging_context())
        raise HTTPException(status_code=404, detail="No score calibrations found for the requested score set")

    return permitted_calibrations


@router.get(
    "/score-set/{score_set_urn}/primary",
    response_model=score_calibration.ScoreCalibrationWithScoreSetUrn,
    responses={404: {}},
)
async def get_primary_score_calibrations_for_score_set(
    *,
    score_set_urn: str,
    db: Session = Depends(deps.get_db),
    user_data: Optional[UserData] = Depends(get_current_user),
) -> ScoreCalibration:
    """
    Retrieve the primary score calibration for a given score set URN.
    """
    save_to_logging_context({"requested_resource": score_set_urn, "resource_property": "calibrations"})
    score_set = await fetch_score_set_by_urn(db, score_set_urn, user_data, None, False)

    permitted_calibrations = [
        calibration
        for calibration in score_set.score_calibrations
        if has_permission(user_data, calibration, Action.READ)
    ]
    if not permitted_calibrations:
        logger.debug("No score calibrations found for the requested score set", extra=logging_context())
        raise HTTPException(status_code=404, detail="No primary score calibrations found for the requested score set")

    primary_calibrations = [c for c in permitted_calibrations if c.primary]
    if not primary_calibrations:
        logger.debug("No primary score calibrations found for the requested score set", extra=logging_context())
        raise HTTPException(status_code=404, detail="No primary score calibrations found for the requested score set")
    elif len(primary_calibrations) > 1:
        logger.error(
            "Multiple primary score calibrations found for the requested score set",
            extra={**logging_context(), "num_primary_calibrations": len(primary_calibrations)},
        )
        raise HTTPException(
            status_code=500,
            detail="Multiple primary score calibrations found for the requested score set",
        )

    return primary_calibrations[0]


@router.post(
    "/",
    response_model=score_calibration.ScoreCalibrationWithScoreSetUrn,
    responses={404: {}},
)
async def create_score_calibration_route(
    *,
    calibration: score_calibration.ScoreCalibrationCreate,
    db: Session = Depends(deps.get_db),
    user_data: UserData = Depends(require_current_user),
) -> ScoreCalibration:
    """
    Create a new score calibration.

    The score set URN must be provided to associate the calibration with an existing score set.
    The user must have write permission on the associated score set.
    """
    if not calibration.score_set_urn:
        raise HTTPException(status_code=422, detail="score_set_urn must be provided to create a score calibration.")

    save_to_logging_context({"requested_resource": calibration.score_set_urn, "resource_property": "calibrations"})

    score_set = await fetch_score_set_by_urn(db, calibration.score_set_urn, user_data, None, False)
    # TODO#539: Allow any authenticated user to upload a score calibration for a score set, not just those with
    #           permission to update the score set itself.
    assert_permission(user_data, score_set, Action.UPDATE)

    created_calibration = await create_score_calibration_in_score_set(db, calibration, user_data.user)

    db.commit()
    db.refresh(created_calibration)

    return created_calibration


@router.put(
    "/{urn}",
    response_model=score_calibration.ScoreCalibrationWithScoreSetUrn,
    responses={404: {}},
)
async def modify_score_calibration_route(
    *,
    urn: str,
    calibration_update: score_calibration.ScoreCalibrationModify,
    db: Session = Depends(deps.get_db),
    user_data: UserData = Depends(require_current_user),
) -> ScoreCalibration:
    """
    Modify an existing score calibration by its URN.
    """
    save_to_logging_context({"requested_resource": urn})

    # If the user supplies a new score_set_urn, validate it exists and the user has permission to use it.
    if calibration_update.score_set_urn is not None:
        score_set = await fetch_score_set_by_urn(db, calibration_update.score_set_urn, user_data, None, False)

        # TODO#539: Allow any authenticated user to upload a score calibration for a score set, not just those with
        #           permission to update the score set itself.
        assert_permission(user_data, score_set, Action.UPDATE)

    item = db.query(ScoreCalibration).where(ScoreCalibration.urn == urn).one_or_none()
    if not item:
        logger.debug("The requested score calibration does not exist", extra=logging_context())
        raise HTTPException(status_code=404, detail="The requested score calibration does not exist")

    assert_permission(user_data, item, Action.UPDATE)

    updated_calibration = await modify_score_calibration(db, item, calibration_update, user_data.user)

    db.commit()
    db.refresh(updated_calibration)

    return updated_calibration


@router.delete(
    "/{urn}",
    response_model=None,
    responses={404: {}},
    status_code=204,
)
async def delete_score_calibration_route(
    *,
    urn: str,
    db: Session = Depends(deps.get_db),
    user_data: UserData = Depends(require_current_user),
) -> None:
    """
    Delete an existing score calibration by its URN.
    """
    save_to_logging_context({"requested_resource": urn})

    item = db.query(ScoreCalibration).where(ScoreCalibration.urn == urn).one_or_none()
    if not item:
        logger.debug("The requested score calibration does not exist", extra=logging_context())
        raise HTTPException(status_code=404, detail="The requested score calibration does not exist")

    assert_permission(user_data, item, Action.DELETE)

    delete_score_calibration(db, item)
    db.commit()

    return None


@router.post(
    "/{urn}/promote-to-primary",
    response_model=score_calibration.ScoreCalibrationWithScoreSetUrn,
    responses={404: {}},
)
async def promote_score_calibration_to_primary_route(
    *,
    urn: str,
    demote_existing_primary: bool = Query(
        False, description="Whether to demote any existing primary calibration", alias="demoteExistingPrimary"
    ),
    db: Session = Depends(deps.get_db),
    user_data: UserData = Depends(require_current_user),
) -> ScoreCalibration:
    """
    Promote a score calibration to be the primary calibration for its associated score set.
    """
    save_to_logging_context(
        {"requested_resource": urn, "resource_property": "primary", "demote_existing_primary": demote_existing_primary}
    )

    item = db.query(ScoreCalibration).where(ScoreCalibration.urn == urn).one_or_none()
    if not item:
        logger.debug("The requested score calibration does not exist", extra=logging_context())
        raise HTTPException(status_code=404, detail="The requested score calibration does not exist")

    assert_permission(user_data, item, Action.CHANGE_RANK)

    if item.primary:
        logger.debug("The requested score calibration is already primary", extra=logging_context())
        return item

    if item.research_use_only:
        logger.debug("Research use only score calibrations cannot be promoted to primary", extra=logging_context())
        raise HTTPException(
            status_code=400, detail="Research use only score calibrations cannot be promoted to primary"
        )

    if item.private:
        logger.debug("Private score calibrations cannot be promoted to primary", extra=logging_context())
        raise HTTPException(status_code=400, detail="Private score calibrations cannot be promoted to primary")

    # We've already checked whether the item matching the calibration URN is primary, so this
    # will necessarily be a different calibration, if it exists.
    existing_primary_calibration = next((c for c in item.score_set.score_calibrations if c.primary), None)
    if existing_primary_calibration and not demote_existing_primary:
        logger.debug(
            "A primary score calibration already exists for this score set",
            extra={**logging_context(), "existing_primary_urn": existing_primary_calibration.urn},
        )
        raise HTTPException(
            status_code=400,
            detail="A primary score calibration already exists for this score set. Demote it first or pass demoteExistingPrimary=True.",
        )
    elif existing_primary_calibration and demote_existing_primary:
        assert_permission(user_data, existing_primary_calibration, Action.CHANGE_RANK)

    promoted_calibration = promote_score_calibration_to_primary(db, item, user_data.user, demote_existing_primary)
    db.commit()
    db.refresh(promoted_calibration)

    return promoted_calibration


@router.post(
    "/{urn}/demote-from-primary",
    response_model=score_calibration.ScoreCalibrationWithScoreSetUrn,
    responses={404: {}},
)
def demote_score_calibration_from_primary_route(
    *,
    urn: str,
    db: Session = Depends(deps.get_db),
    user_data: UserData = Depends(require_current_user),
) -> ScoreCalibration:
    """
    Demote a score calibration from being the primary calibration for its associated score set.
    """
    save_to_logging_context({"requested_resource": urn, "resource_property": "primary"})

    item = db.query(ScoreCalibration).where(ScoreCalibration.urn == urn).one_or_none()
    if not item:
        logger.debug("The requested score calibration does not exist", extra=logging_context())
        raise HTTPException(status_code=404, detail="The requested score calibration does not exist")

    assert_permission(user_data, item, Action.CHANGE_RANK)

    if not item.primary:
        logger.debug("The requested score calibration is not primary", extra=logging_context())
        return item

    demoted_calibration = demote_score_calibration_from_primary(db, item, user_data.user)
    db.commit()
    db.refresh(demoted_calibration)

    return demoted_calibration


@router.post(
    "/{urn}/publish",
    response_model=score_calibration.ScoreCalibrationWithScoreSetUrn,
    responses={404: {}},
)
def publish_score_calibration_route(
    *,
    urn: str,
    db: Session = Depends(deps.get_db),
    user_data: UserData = Depends(require_current_user),
) -> ScoreCalibration:
    """
    Publish a score calibration, making it publicly visible.
    """
    save_to_logging_context({"requested_resource": urn, "resource_property": "private"})

    item = db.query(ScoreCalibration).where(ScoreCalibration.urn == urn).one_or_none()
    if not item:
        logger.debug("The requested score calibration does not exist", extra=logging_context())
        raise HTTPException(status_code=404, detail="The requested score calibration does not exist")

    assert_permission(user_data, item, Action.PUBLISH)

    if not item.private:
        logger.debug("The requested score calibration is already public", extra=logging_context())
        return item

    # XXX: desired?
    # if item.score_set.private:
    #     logger.debug(
    #         "Score calibrations associated with private score sets cannot be published", extra=logging_context()
    #     )
    #     raise HTTPException(
    #         status_code=400,
    #         detail="Score calibrations associated with private score sets cannot be published. First publish the score set, then calibrations.",
    #     )

    item = publish_score_calibration(db, item, user_data.user)
    db.commit()
    db.refresh(item)

    return item
