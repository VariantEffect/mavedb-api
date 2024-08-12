import logging
from operator import attrgetter
from typing import Any, Optional

from fastapi import APIRouter, Depends, HTTPException
from fastapi.encoders import jsonable_encoder
import pydantic
from sqlalchemy import or_, and_
from sqlalchemy.orm import Session

from mavedb import deps
from mavedb.lib.authentication import get_current_user, UserData
from mavedb.lib.authorization import require_current_user, require_current_user_with_email
from mavedb.lib.contributors import find_or_create_contributor
from mavedb.lib.exceptions import NonexistentOrcidUserError
from mavedb.lib.experiments import search_experiments as _search_experiments
from mavedb.lib.identifiers import (
    find_or_create_doi_identifier,
    find_or_create_publication_identifier,
    find_or_create_raw_read_identifier,
)
from mavedb.lib.logging import LoggedRoute
from mavedb.lib.logging.context import dump_context, save_to_logging_context
from mavedb.lib.permissions import assert_permission, Action
from mavedb.lib.validation.exceptions import ValidationError
from mavedb.lib.validation.keywords import validate_keyword_list
from mavedb.lib.keywords import search_keyword
from mavedb.models.contributor import Contributor
from mavedb.models.experiment import Experiment
from mavedb.models.experiment_controlled_keyword import ExperimentControlledKeywordAssociation
from mavedb.models.experiment_set import ExperimentSet
from mavedb.models.score_set import ScoreSet
from mavedb.view_models import experiment, score_set
from mavedb.view_models.search import ExperimentsSearch

import requests

logger = logging.getLogger(__name__)

router = APIRouter(
    prefix="/api/v1",
    tags=["experiments"],
    responses={404: {"description": "Not found"}},
    route_class=LoggedRoute,
)


# TODO: Rewrite this function.
@router.get(
    "/experiments/",
    status_code=200,
    response_model=list[experiment.Experiment],
    response_model_exclude_none=True,
)
def list_experiments(
    *,
    editable: Optional[bool] = None,
    q: Optional[str] = None,
    db: Session = Depends(deps.get_db),
    user_data: Optional[UserData] = Depends(get_current_user),
) -> list[Experiment]:
    """
    List experiments.
    """
    query = db.query(Experiment)
    if q is not None:
        save_to_logging_context({"query_string": q})

        if user_data is None or user_data.user is None:
            logger.debug(dump_context(message="User is anonymous; Cannot list their experiments."))
            return []

        if len(q) > 0:
            logger.debug(dump_context(message="Listing experiments for the current user."))
            query = query.filter(
                Experiment.created_by_id == user_data.user.id
            )  # .filter(Experiment.published_date is None)
        # else:
        #     query = query.filter(Experiment.created_by_id == user.id).filter(Experiment.published_date is None)
    else:
        logger.debug(dump_context(message="No query string was provided; Listing all experiments."))

    items = query.order_by(Experiment.urn).all()
    return items


@router.post(
    "/experiments/search",
    status_code=200,
    response_model=list[experiment.ShortExperiment],
)
def search_experiments(search: ExperimentsSearch, db: Session = Depends(deps.get_db)) -> Any:
    """
    Search experiments.
    """
    return _search_experiments(db, None, search)


@router.post(
    "/me/experiments/search",
    status_code=200,
    response_model=list[experiment.ShortExperiment],
)
def search_my_experiments(
    search: ExperimentsSearch,
    db: Session = Depends(deps.get_db),
    user_data: UserData = Depends(require_current_user),
) -> Any:
    """
    Search experiments created by the current user..
    """
    return _search_experiments(db, user_data.user, search)


@router.get(
    "/experiments/{urn}",
    status_code=200,
    response_model=experiment.Experiment,
    responses={404: {}},
    response_model_exclude_none=True,
)
def fetch_experiment(
    *,
    urn: str,
    db: Session = Depends(deps.get_db),
    user_data: Optional[UserData] = Depends(get_current_user),
) -> Experiment:
    """
    Fetch a single experiment by URN.
    """
    # item = db.query(Experiment).filter(Experiment.urn == urn).filter(Experiment.private.is_(False)).first()
    item = db.query(Experiment).filter(Experiment.urn == urn).first()
    save_to_logging_context({"requested_resource": urn})

    if not item:
        logger.debug(dump_context(message="The requested experiment does not exist."))
        raise HTTPException(status_code=404, detail=f"Experiment with URN {urn} not found")

    assert_permission(user_data, item, Action.READ)
    return item


@router.get(
    "/experiments/{urn}/score-sets",
    status_code=200,
    response_model=list[score_set.ScoreSet],
    responses={404: {}},
    response_model_exclude_none=True,
)
def get_experiment_score_sets(
    *,
    urn: str,
    db: Session = Depends(deps.get_db),
    user_data: Optional[UserData] = Depends(get_current_user),
) -> Any:
    """
    Get all score sets belonging to an experiment.
    """
    save_to_logging_context({"requested_resource": urn, "resource_property": "score-sets"})

    experiment = db.query(Experiment).filter(Experiment.urn == urn).first()
    if not experiment:
        logger.debug(dump_context(message="The requested experiment does not exist."))
        raise HTTPException(status_code=404, detail=f"experiment with URN '{urn}' not found")

    assert_permission(user_data, experiment, Action.READ)

    # If there is a current user with score sets associated with this experiment, return all of them. Otherwise, only show
    # the public / published score sets.
    #
    # TODO(#182): A side effect of this implementation is that only the user who has created the experiment may view all the Score sets
    # associated with a given experiment. This could be solved with user impersonation for certain user roles.
    score_sets = (
        db.query(ScoreSet).filter(ScoreSet.experiment_id == experiment.id).filter(~ScoreSet.superseding_score_set.has())
    )
    if user_data is not None:
        score_set_result = score_sets.filter(
            or_(
                ScoreSet.private.is_(False),
                and_(ScoreSet.private.is_(True), ScoreSet.created_by == user_data.user),
            )
        ).all()
    else:
        score_set_result = score_sets.filter(ScoreSet.private.is_(False)).all()
        logger.debug(dump_context(message="User is anonymous; Filtering only public score sets will be shown."))

    if not score_set_result:
        save_to_logging_context({"associated_resources": []})
        logger.info(dump_context(message="No score sets are associated with the requested experiment."))

        raise HTTPException(status_code=404, detail="no associated score sets")
    else:
        score_set_result.sort(key=attrgetter("urn"))
        save_to_logging_context({"associated_resources": [item.urn for item in score_set_result]})

    return score_set_result


@router.post(
    "/experiments/",
    response_model=experiment.Experiment,
    responses={422: {}},
    response_model_exclude_none=True,
)
async def create_experiment(
    *,
    item_create: experiment.ExperimentCreate,
    db: Session = Depends(deps.get_db),
    user_data: UserData = Depends(require_current_user_with_email),
) -> Any:
    """
    Create an experiment.
    """
    logger.debug(dump_context(message="Began creation of new experiment."))

    experiment_set = None
    if item_create.experiment_set_urn is not None:
        experiment_set = (
            db.query(ExperimentSet).filter(ExperimentSet.urn == item_create.experiment_set_urn).one_or_none()
        )
        if not experiment_set:
            logger.info(
                dump_context(message="Could not create experiment; The requested experiment set does not exist.")
            )
            raise HTTPException(
                status_code=404,
                detail=f"experiment set with URN '{item_create.experiment_set_urn}' not found.",
            )

        save_to_logging_context({"experiment_set": experiment_set.urn})
        assert_permission(user_data, experiment_set, Action.ADD_EXPERIMENT)

    contributors: list[Contributor] = []
    try:
        contributors = [
            await find_or_create_contributor(db, contributor.orcid_id) for contributor in item_create.contributors or []
        ]
    except NonexistentOrcidUserError as e:
        raise pydantic.ValidationError(
            [pydantic.error_wrappers.ErrorWrapper(ValidationError(str(e)), loc="contributors")],
            model=experiment.ExperimentCreate,
        )
    logger.debug(dump_context(message="Creating experiment within existing experiment set."))

    try:
        doi_identifiers = [
            await find_or_create_doi_identifier(db, identifier.identifier)
            for identifier in item_create.doi_identifiers or []
        ]
        raw_read_identifiers = [
            await find_or_create_raw_read_identifier(db, identifier.identifier)
            for identifier in item_create.raw_read_identifiers or []
        ]
        primary_publication_identifiers = [
            await find_or_create_publication_identifier(db, identifier.identifier, identifier.db_name)
            for identifier in item_create.primary_publication_identifiers or []
        ]
        publication_identifiers = [
            await find_or_create_publication_identifier(db, identifier.identifier, identifier.db_name)
            for identifier in item_create.secondary_publication_identifiers or []
        ] + primary_publication_identifiers

    except requests.exceptions.ConnectTimeout:
        logger.error(dump_context(message="Gateway timed out while creating experiment identifiers."))
        raise HTTPException(status_code=504, detail="Gateway Timeout")

    except requests.exceptions.HTTPError:
        logger.error(dump_context(message="Encountered bad gateway while creating experiment identifiers."))
        raise HTTPException(status_code=502, detail="Bad Gateway")

    # create a temporary `primary` attribute on each of our publications that indicates
    # to our association proxy whether it is a primary publication or not
    primary_identifiers = [pub.identifier for pub in primary_publication_identifiers]
    for publication in publication_identifiers:
        setattr(publication, "primary", publication.identifier in primary_identifiers)

    # TODO: Controlled keywords currently is allowed none value.
    #  Will be changed in the future when we get the final list.
    keywords: list[ExperimentControlledKeywordAssociation] = []
    if item_create.keywords:
        all_values_none = all(k.keyword.value is None for k in item_create.keywords)
        if all_values_none is False:
            # Users may choose part of keywords from dropdown menu. Remove not chosen keywords from the list.
            filtered_keywords = list(filter(lambda k: k.keyword.value is not None, item_create.keywords))
            try:
                validate_keyword_list(filtered_keywords)
            except ValidationError as e:
                raise HTTPException(status_code=422, detail=str(e))
            for upload_keyword in filtered_keywords:
                try:
                    description = upload_keyword.description
                    controlled_keyword = search_keyword(db, upload_keyword.keyword.key, upload_keyword.keyword.value)
                    experiment_controlled_keyword = ExperimentControlledKeywordAssociation(
                        controlled_keyword=controlled_keyword,
                        description=description,
                    )
                    keywords.append(experiment_controlled_keyword)
                except ValueError as e:
                    raise HTTPException(status_code=422, detail=str(e))

    item = Experiment(
        **jsonable_encoder(
            item_create,
            by_alias=False,
            exclude={
                "contributors",
                "doi_identifiers",
                "experiment_set_urn",
                "keywords",
                "primary_publication_identifiers",
                "secondary_publication_identifiers",
                "raw_read_identifiers",
            },
        ),
        experiment_set=experiment_set,
        contributors=contributors,
        doi_identifiers=doi_identifiers,
        publication_identifiers=publication_identifiers,  # an internal association proxy representation
        raw_read_identifiers=raw_read_identifiers,
        created_by=user_data.user,
        modified_by=user_data.user,
        keyword_objs=keywords
    )  # type: ignore

    db.add(item)
    db.commit()
    db.refresh(item)

    save_to_logging_context({"created_resource": item.urn})
    return item


@router.put(
    "/experiments/{urn}",
    response_model=experiment.Experiment,
    responses={422: {}},
    response_model_exclude_none=True,
)
async def update_experiment(
    *,
    item_update: experiment.ExperimentUpdate,
    urn: str,
    db: Session = Depends(deps.get_db),
    user_data: UserData = Depends(require_current_user_with_email),
) -> Any:
    """
    Update an experiment.
    """
    save_to_logging_context({"requested_resource": urn})
    logger.debug(dump_context(message="Began experiment update."))

    # item = db.query(Experiment).filter(Experiment.urn == urn).filter(Experiment.private.is_(False)).one_or_none()
    item = db.query(Experiment).filter(Experiment.urn == urn).one_or_none()
    if item is None:
        logger.info(dump_context(message="Failed to update experiment; The requested experiment does not exist."))
        raise HTTPException(status_code=404, detail=f"experiment with URN {urn} not found")

    assert_permission(user_data, item, Action.UPDATE)

    pairs = {
        k: v
        for k, v in vars(item_update).items()
        if k
        not in [
            "contributors",
            "doi_identifiers",
            "keywords",
            "secondary_publication_identifiers",
            "primary_publication_identifiers",
            "raw_read_identifiers",
        ]
    }
    for var, value in pairs.items():  # vars(item_update).items():
        setattr(item, var, value) if value else None

    try:
        item.contributors = [
            await find_or_create_contributor(db, contributor.orcid_id) for contributor in item_update.contributors or []
        ]
    except NonexistentOrcidUserError as e:
        raise pydantic.ValidationError(
            [pydantic.error_wrappers.ErrorWrapper(ValidationError(str(e)), loc="contributors")],
            model=experiment.ExperimentUpdate,
        )

    doi_identifiers = [
        await find_or_create_doi_identifier(db, identifier.identifier)
        for identifier in item_update.doi_identifiers or []
    ]
    raw_read_identifiers = [
        await find_or_create_raw_read_identifier(db, identifier.identifier)
        for identifier in item_update.raw_read_identifiers or []
    ]

    primary_publication_identifiers = [
        await find_or_create_publication_identifier(db, identifier.identifier, identifier.db_name)
        for identifier in item_update.primary_publication_identifiers or []
    ]
    publication_identifiers = [
        await find_or_create_publication_identifier(db, identifier.identifier, identifier.db_name)
        for identifier in item_update.secondary_publication_identifiers or []
    ] + primary_publication_identifiers

    # create a temporary `primary` attribute on each of our publications that indicates
    # to our association proxy whether it is a primary publication or not
    primary_identifiers = [pub.identifier for pub in primary_publication_identifiers]
    for publication in publication_identifiers:
        setattr(publication, "primary", publication.identifier in primary_identifiers)

    item.doi_identifiers = doi_identifiers
    item.publication_identifiers = publication_identifiers
    item.raw_read_identifiers = raw_read_identifiers

    if item_update.keywords:
        all_values_none = all(k.keyword.value is None for k in item_update.keywords)
        if all_values_none is False:
            # Users may choose part of keywords from dropdown menu. Remove not chosen keywords from the list.
            filtered_keywords = list(filter(lambda k: k.keyword.value is not None, item_update.keywords))
            try:
                validate_keyword_list(filtered_keywords)
            except ValidationError as e:
                raise HTTPException(status_code=422, detail=str(e))
            try:
                await item.set_keywords(db, filtered_keywords)
            except Exception as e:
                raise HTTPException(status_code=500, detail=f"Invalid keywords: {str(e)}")

    item.modified_by = user_data.user

    db.add(item)
    db.commit()
    db.refresh(item)

    save_to_logging_context({"updated_resource": item.urn})
    return item


@router.delete("/experiments/{urn}", response_model=None, responses={422: {}})
async def delete_experiment(
    *,
    urn: str,
    db: Session = Depends(deps.get_db),
    user_data: UserData = Depends(require_current_user),
) -> None:
    """
    Delete a experiment .

    Raises

    Returns
    _______
    Does not return anything
    string : HTTP code 200 successful but returning content
    or
    communitcate to client whether the operation succeeded
    204 if successful but not returning content - likely going with this
    """
    save_to_logging_context({"requested_resource": urn})

    item = db.query(Experiment).filter(Experiment.urn == urn).one_or_none()
    if not item:
        logger.info(
            dump_context(message="Could not delete the requested experiment; The requested experiment does not exist.")
        )
        raise HTTPException(status_code=404, detail=f"experiment with URN '{urn}' not found.")

    assert_permission(user_data, item, Action.DELETE)

    db.delete(item)
    db.commit()
