import logging
from typing import Optional

from fastapi import APIRouter, Depends, File, HTTPException, Query, UploadFile
from sqlalchemy.orm import Session, selectinload

from mavedb import deps
from mavedb.lib.authentication import UserData, get_current_user
from mavedb.lib.authorization import require_current_user
from mavedb.lib.flexible_model_loader import json_or_form_loader
from mavedb.lib.logging import LoggedRoute
from mavedb.lib.logging.context import (
    logging_context,
    save_to_logging_context,
)
from mavedb.lib.permissions import Action, assert_permission, has_permission
from mavedb.lib.score_calibrations import (
    create_score_calibration_in_score_set,
    delete_score_calibration,
    demote_score_calibration_from_primary,
    modify_score_calibration,
    promote_score_calibration_to_primary,
    publish_score_calibration,
    variant_classification_df_to_dict,
)
from mavedb.lib.score_sets import csv_data_to_df
from mavedb.lib.validation.constants.general import calibration_class_column_name, calibration_variant_column_name
from mavedb.lib.validation.dataframe.calibration import validate_and_standardize_calibration_classes_dataframe
from mavedb.lib.validation.exceptions import ValidationError
from mavedb.models.score_calibration import ScoreCalibration
from mavedb.models.score_set import ScoreSet
from mavedb.view_models import score_calibration

logger = logging.getLogger(__name__)

router = APIRouter(
    prefix="/api/v1/score-calibrations",
    tags=["Score Calibrations"],
    responses={404: {"description": "Not found"}},
    route_class=LoggedRoute,
)

# Create dependency loaders for flexible JSON/form parsing
calibration_create_loader = json_or_form_loader(
    score_calibration.ScoreCalibrationCreate,
    field_name="calibration_json",
)

calibration_modify_loader = json_or_form_loader(
    score_calibration.ScoreCalibrationModify,
    field_name="calibration_json",
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

    item = (
        db.query(ScoreCalibration)
        .options(selectinload(ScoreCalibration.score_set).selectinload(ScoreSet.contributors))
        .where(ScoreCalibration.urn == urn)
        .one_or_none()
    )
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
    score_set = db.query(ScoreSet).filter(ScoreSet.urn == score_set_urn).one_or_none()

    if not score_set:
        logger.debug("ScoreSet not found", extra=logging_context())
        raise HTTPException(status_code=404, detail=f"score set with URN '{score_set_urn}' not found")

    assert_permission(user_data, score_set, Action.READ)

    calibrations = (
        db.query(ScoreCalibration)
        .filter(ScoreCalibration.score_set_id == score_set.id)
        .options(selectinload(ScoreCalibration.score_set).selectinload(ScoreSet.contributors))
        .all()
    )

    permitted_calibrations = [
        calibration for calibration in calibrations if has_permission(user_data, calibration, Action.READ).permitted
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

    score_set = db.query(ScoreSet).filter(ScoreSet.urn == score_set_urn).one_or_none()
    if not score_set:
        logger.debug("ScoreSet not found", extra=logging_context())
        raise HTTPException(status_code=404, detail=f"score set with URN '{score_set_urn}' not found")

    assert_permission(user_data, score_set, Action.READ)

    calibrations = (
        db.query(ScoreCalibration)
        .filter(ScoreCalibration.score_set_id == score_set.id)
        .options(selectinload(ScoreCalibration.score_set).selectinload(ScoreSet.contributors))
        .all()
    )

    permitted_calibrations = [
        calibration for calibration in calibrations if has_permission(user_data, calibration, Action.READ).permitted
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
    responses={404: {}, 422: {"description": "Validation Error"}},
    openapi_extra={
        "requestBody": {
            "content": {
                "application/json": {
                    "schema": {"$ref": "#/components/schemas/ScoreCalibrationCreate"},
                },
                "multipart/form-data": {
                    "schema": {
                        "type": "object",
                        "properties": {
                            "calibration_json": {
                                "type": "string",
                                "description": "JSON string containing the calibration data",
                                "example": '{"score_set_urn":"urn:mavedb:0000000X-X-X","title":"My Calibration","description":"Functional score calibration","baseline_score":1.0}',
                            },
                            "classes_file": {
                                "type": "string",
                                "format": "binary",
                                "description": "CSV file containing variant classifications",
                            },
                        },
                    }
                },
            },
            "description": "Score calibration data. Can be sent as JSON body or multipart form data",
        }
    },
)
async def create_score_calibration_route(
    *,
    calibration: score_calibration.ScoreCalibrationCreate = Depends(calibration_create_loader),
    classes_file: Optional[UploadFile] = File(
        None,
        description=f"CSV file containing variant classifications. This file must contain two columns: '{calibration_variant_column_name}' and '{calibration_class_column_name}'.",
    ),
    db: Session = Depends(deps.get_db),
    user_data: UserData = Depends(require_current_user),
) -> ScoreCalibration:
    """
    Create a new score calibration.

    This endpoint supports two different request formats to accommodate various client needs:

    ## Method 1: JSON Request Body (application/json)
    Send calibration data as a standard JSON request body. This method is ideal for 
    creating calibrations without file uploads.

    **Content-Type**: `application/json`

    **Example**:
    ```json
    {
        "score_set_urn": "urn:mavedb:0000000X-X-X",
        "title": "My Calibration",
        "description": "Functional score calibration",
        "baseline_score": 1.0
    }
    ```

    ## Method 2: Multipart Form Data (multipart/form-data)
    Send calibration data as JSON in a form field, optionally with file uploads.
    This method is required when uploading classification files.

    **Content-Type**: `multipart/form-data`

    **Form Fields**:
    - `calibration_json` (string, required): JSON string containing the calibration data
    - `classes_file` (file, optional): CSV file containing variant classifications

    **Example**:
    ```bash
    curl -X POST "/api/v1/score-calibrations/" \\
         -H "Authorization: Bearer your-token" \\
         -F 'calibration_json={"score_set_urn":"urn:mavedb:0000000X-X-X","title":"My Calibration","description":"Functional score calibration","baseline_score":"1.0"}' \\
         -F 'classes_file=@variant_classes.csv'
    ```

    ## Requirements
    - The score set URN must be provided to associate the calibration with an existing score set
    - User must have write permission on the associated score set
    - If uploading a classes_file, it must be a valid CSV with variant classification data

    ## File Upload Details
    The `classes_file` parameter accepts CSV files containing variant classification data.
    The file should have appropriate headers and contain columns for variant urns and class names.

    ## Response
    Returns the created score calibration with its generated URN and associated score set information.
    """
    if not calibration.score_set_urn:
        raise HTTPException(status_code=422, detail="score_set_urn must be provided to create a score calibration.")

    save_to_logging_context({"requested_resource": calibration.score_set_urn, "resource_property": "calibrations"})

    score_set = db.query(ScoreSet).filter(ScoreSet.urn == calibration.score_set_urn).one_or_none()
    if not score_set:
        logger.debug("ScoreSet not found", extra=logging_context())
        raise HTTPException(status_code=404, detail=f"score set with URN '{calibration.score_set_urn}' not found")

    # TODO#539: Allow any authenticated user to upload a score calibration for a score set, not just those with
    #           permission to update the score set itself.
    assert_permission(user_data, score_set, Action.UPDATE)

    if calibration.class_based and not classes_file:
        raise HTTPException(
            status_code=422,
            detail="A classes_file must be provided when creating a class-based calibration.",
        )

    if classes_file:
        if calibration.range_based:
            raise HTTPException(
                status_code=422,
                detail="A classes_file should not be provided when creating a range-based calibration.",
            )

        try:
            classes_df = csv_data_to_df(classes_file.file, induce_hgvs_cols=False)
        except UnicodeDecodeError as e:
            raise HTTPException(
                status_code=400, detail=f"Error decoding file: {e}. Ensure the file has correct values."
            )

        try:
            standardized_classes_df, index_column = validate_and_standardize_calibration_classes_dataframe(
                db, score_set, calibration, classes_df
            )
            variant_classes = variant_classification_df_to_dict(standardized_classes_df, index_column)
        except ValidationError as e:
            raise HTTPException(
                status_code=422,
                detail=[{"loc": [e.custom_loc or "classesFile"], "msg": str(e), "type": "value_error"}],
            )

    created_calibration = await create_score_calibration_in_score_set(
        db, calibration, user_data.user, variant_classes if classes_file else None
    )

    db.commit()
    db.refresh(created_calibration)

    return created_calibration


@router.put(
    "/{urn}",
    response_model=score_calibration.ScoreCalibrationWithScoreSetUrn,
    responses={404: {}, 422: {"description": "Validation Error"}},
    openapi_extra={
        "requestBody": {
            "content": {
                "application/json": {
                    "schema": {"$ref": "#/components/schemas/ScoreCalibrationModify"},
                },
                "multipart/form-data": {
                    "schema": {
                        "type": "object",
                        "properties": {
                            "calibration_json": {
                                "type": "string",
                                "description": "JSON string containing the calibration update data",
                                "example": '{"title":"Updated Calibration","description":"Updated description","baseline_score":2.0}',
                            },
                            "classes_file": {
                                "type": "string",
                                "format": "binary",
                                "description": "CSV file containing updated variant classifications",
                            },
                        },
                    }
                },
            },
            "description": "Score calibration update data. Can be sent as JSON body or multipart form data",
        }
    },
)
async def modify_score_calibration_route(
    *,
    urn: str,
    calibration_update: score_calibration.ScoreCalibrationModify = Depends(calibration_modify_loader),
    classes_file: Optional[UploadFile] = File(
        None,
        description=f"CSV file containing variant classifications. This file must contain two columns: '{calibration_variant_column_name}' and '{calibration_class_column_name}'.",
    ),
    db: Session = Depends(deps.get_db),
    user_data: UserData = Depends(require_current_user),
) -> ScoreCalibration:
    """
    Modify an existing score calibration by its URN.

    This endpoint supports two different request formats to accommodate various client needs:

    ## Method 1: JSON Request Body (application/json)
    Send calibration update data as a standard JSON request body. This method is ideal for 
    modifying calibrations without file uploads.

    **Content-Type**: `application/json`

    **Example**:
    ```json
    {
        "score_set_urn": "urn:mavedb:0000000X-X-X",
        "title": "Updated Calibration Title",
        "description": "Updated functional score calibration",
        "baseline_score": 1.0
    }
    ```

    ## Method 2: Multipart Form Data (multipart/form-data)
    Send calibration update data as JSON in a form field, optionally with file uploads.
    This method is required when uploading new classification files.

    **Content-Type**: `multipart/form-data`

    **Form Fields**:
    - `calibration_json` (string, required): JSON string containing the calibration update data
    - `classes_file` (file, optional): CSV file containing updated variant classifications

    **Example**:
    ```bash
    curl -X PUT "/api/v1/score-calibrations/{urn}" \\
         -H "Authorization: Bearer your-token" \\
         -F 'calibration_json={"score_set_urn":"urn:mavedb:0000000X-X-X","title":"My Calibration","description":"Functional score calibration","baseline_score":"1.0"}' \\
         -F 'classes_file=@updated_variant_classes.csv'
    ```

    ## Requirements
    - User must have update permission on the calibration
    - If changing the score_set_urn, user must have permission on the new score set
    - All fields in the update are optional - only provided fields will be modified

    ## File Upload Details
    The `classes_file` parameter accepts CSV files containing updated variant classification data.
    If provided, this will replace the existing classification data for the calibration.
    The file should have appropriate headers and follow the expected format for variant
    classifications within the associated score set.

    ## Response
    Returns the updated score calibration with all modifications applied and any new
    classification data from the uploaded file.
    """
    save_to_logging_context({"requested_resource": urn})

    # If the user supplies a new score_set_urn, validate it exists and the user has permission to use it.
    if calibration_update.score_set_urn is not None:
        score_set_update = db.query(ScoreSet).filter(ScoreSet.urn == calibration_update.score_set_urn).one_or_none()

        if not score_set_update:
            logger.debug("ScoreSet not found", extra=logging_context())
            raise HTTPException(
                status_code=404, detail=f"score set with URN '{calibration_update.score_set_urn}' not found"
            )

        # TODO#539: Allow any authenticated user to upload a score calibration for a score set, not just those with
        #           permission to update the score set itself.
        assert_permission(user_data, score_set_update, Action.UPDATE)
    else:
        score_set_update = None

    item = (
        db.query(ScoreCalibration)
        .options(selectinload(ScoreCalibration.score_set).selectinload(ScoreSet.contributors))
        .where(ScoreCalibration.urn == urn)
        .one_or_none()
    )
    if not item:
        logger.debug("The requested score calibration does not exist", extra=logging_context())
        raise HTTPException(status_code=404, detail="The requested score calibration does not exist")

    assert_permission(user_data, item, Action.UPDATE)
    score_set = score_set_update or item.score_set

    if calibration_update.class_based and not classes_file:
        raise HTTPException(
            status_code=422,
            detail="A classes_file must be provided when modifying a class-based calibration.",
        )

    if classes_file:
        if calibration_update.range_based:
            raise HTTPException(
                status_code=422,
                detail="A classes_file should not be provided when modifying a range-based calibration.",
            )

        try:
            classes_df = csv_data_to_df(classes_file.file, induce_hgvs_cols=False)
        except UnicodeDecodeError as e:
            raise HTTPException(
                status_code=400, detail=f"Error decoding file: {e}. Ensure the file has correct values."
            )

        try:
            standardized_classes_df, index_column = validate_and_standardize_calibration_classes_dataframe(
                db, score_set, calibration_update, classes_df
            )
            variant_classes = variant_classification_df_to_dict(standardized_classes_df, index_column)
        except ValidationError as e:
            raise HTTPException(
                status_code=422,
                detail=[{"loc": [e.custom_loc or "classesFile"], "msg": str(e), "type": "value_error"}],
            )

    updated_calibration = await modify_score_calibration(
        db, item, calibration_update, user_data.user, variant_classes if classes_file else None
    )

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

    item = (
        db.query(ScoreCalibration)
        .options(selectinload(ScoreCalibration.score_set).selectinload(ScoreSet.contributors))
        .where(ScoreCalibration.urn == urn)
        .one_or_none()
    )
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

    item = (
        db.query(ScoreCalibration)
        .options(selectinload(ScoreCalibration.score_set).selectinload(ScoreSet.contributors))
        .where(ScoreCalibration.urn == urn)
        .one_or_none()
    )
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

    item = (
        db.query(ScoreCalibration)
        .options(selectinload(ScoreCalibration.score_set).selectinload(ScoreSet.contributors))
        .where(ScoreCalibration.urn == urn)
        .one_or_none()
    )
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

    item = (
        db.query(ScoreCalibration)
        .options(selectinload(ScoreCalibration.score_set).selectinload(ScoreSet.contributors))
        .where(ScoreCalibration.urn == urn)
        .one_or_none()
    )
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
