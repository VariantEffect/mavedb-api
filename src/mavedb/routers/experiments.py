import logging
from operator import attrgetter
from typing import Any, Optional

import requests
from fastapi import APIRouter, Depends, HTTPException
from fastapi.encoders import jsonable_encoder
from sqlalchemy import or_
from sqlalchemy.orm import Session

from mavedb import deps
from mavedb.lib.authentication import get_current_user
from mavedb.lib.authorization import require_current_user, require_current_user_with_email
from mavedb.lib.contributors import find_or_create_contributor
from mavedb.lib.exceptions import NonexistentOrcidUserError
from mavedb.lib.experiments import enrich_experiment_with_num_score_sets
from mavedb.lib.experiments import search_experiments as _search_experiments
from mavedb.lib.identifiers import (
    find_or_create_doi_identifier,
    find_or_create_publication_identifier,
    find_or_create_raw_read_identifier,
)
from mavedb.lib.keywords import search_keyword
from mavedb.lib.logging import LoggedRoute
from mavedb.lib.logging.context import logging_context, save_to_logging_context
from mavedb.lib.permissions import Action, assert_permission, has_permission
from mavedb.lib.score_sets import find_superseded_score_set_tail
from mavedb.lib.types.authentication import UserData
from mavedb.lib.validation.exceptions import ValidationError
from mavedb.lib.validation.keywords import validate_keyword_list
from mavedb.models.contributor import Contributor
from mavedb.models.experiment import Experiment
from mavedb.models.experiment_controlled_keyword import ExperimentControlledKeywordAssociation
from mavedb.models.experiment_set import ExperimentSet
from mavedb.models.score_set import ScoreSet
from mavedb.routers.shared import (
    ACCESS_CONTROL_ERROR_RESPONSES,
    GATEWAY_ERROR_RESPONSES,
    PUBLIC_ERROR_RESPONSES,
    ROUTER_BASE_PREFIX,
)
from mavedb.view_models import experiment, score_set
from mavedb.view_models.search import ExperimentsSearch

TAG_NAME = "Experiments"

logger = logging.getLogger(__name__)

router = APIRouter(
    prefix=f"{ROUTER_BASE_PREFIX}",
    tags=[TAG_NAME],
    responses={**PUBLIC_ERROR_RESPONSES},
    route_class=LoggedRoute,
)

metadata = {
    "name": TAG_NAME,
    "description": "Manage and retrieve experiments and their associated data.",
    "externalDocs": {
        "description": "Experiments Documentation",
        "url": "https://mavedb.org/docs/mavedb/record_types.html#experiments",
    },
}


# None of any part calls this function. Feel free to modify it if we need it in the future.
@router.get(
    "/experiments/",
    status_code=200,
    response_model=list[experiment.Experiment],
    response_model_exclude_none=True,
    responses={**ACCESS_CONTROL_ERROR_RESPONSES},
    summary="List experiments",
)
def list_experiments(
    *,
    editable: Optional[bool] = None,
    db: Session = Depends(deps.get_db),
    user_data: Optional[UserData] = Depends(get_current_user),
) -> list[Experiment]:
    """
    List all experiments viewable by the current user.
    """
    if editable and user_data is None:
        logger.debug(msg="User is anonymous; Cannot list their experiments.", extra=logging_context())
        return []

    query = db.query(Experiment)

    if editable and user_data is not None:
        logger.debug(msg="Listing experiments for the current user.", extra=logging_context())
        query = query.filter(
            or_(
                Experiment.created_by_id == user_data.user.id,
                Experiment.contributors.any(Contributor.orcid_id == user_data.user.username),
            )
        )

    items = query.order_by(Experiment.urn).all()
    return [item for item in items if has_permission(user_data, item, Action.READ).permitted]


@router.post(
    "/experiments/search",
    status_code=200,
    response_model=list[experiment.ShortExperiment],
    summary="Search experiments",
)
def search_experiments(search: ExperimentsSearch, db: Session = Depends(deps.get_db)) -> Any:
    """
    Search experiments.
    """
    items = _search_experiments(db, None, search)
    return [enrich_experiment_with_num_score_sets(exp, None) for exp in items]


@router.post(
    "/me/experiments/search",
    status_code=200,
    response_model=list[experiment.ShortExperiment],
    responses={**ACCESS_CONTROL_ERROR_RESPONSES},
    summary="Search my experiments",
)
def search_my_experiments(
    search: ExperimentsSearch,
    db: Session = Depends(deps.get_db),
    user_data: UserData = Depends(require_current_user),
) -> Any:
    """
    Search experiments created by the current user..
    """
    items = _search_experiments(db, user_data.user, search)
    return [enrich_experiment_with_num_score_sets(exp, user_data) for exp in items]


@router.get(
    "/experiments/{urn}",
    status_code=200,
    response_model=experiment.Experiment,
    responses={**ACCESS_CONTROL_ERROR_RESPONSES},
    summary="Fetch experiment by URN",
    response_model_exclude_none=True,
)
def fetch_experiment(
    *,
    urn: str,
    db: Session = Depends(deps.get_db),
    user_data: Optional[UserData] = Depends(get_current_user),
) -> experiment.Experiment:
    """
    Fetch a single experiment by URN.
    """
    # item = db.query(Experiment).filter(Experiment.urn == urn).filter(Experiment.private.is_(False)).first()
    item = db.query(Experiment).filter(Experiment.urn == urn).first()
    save_to_logging_context({"requested_resource": urn})

    if not item:
        logger.debug(msg="The requested experiment does not exist.", extra=logging_context())
        raise HTTPException(status_code=404, detail=f"experiment with URN {urn} not found")

    assert_permission(user_data, item, Action.READ)
    return enrich_experiment_with_num_score_sets(item, user_data)


@router.get(
    "/experiments/{urn}/score-sets",
    status_code=200,
    response_model=list[score_set.ScoreSet],
    responses={**ACCESS_CONTROL_ERROR_RESPONSES},
    summary="Get score sets for an experiment",
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
        logger.debug(msg="The requested experiment does not exist.", extra=logging_context())
        raise HTTPException(status_code=404, detail=f"experiment with URN '{urn}' not found")

    assert_permission(user_data, experiment, Action.READ)

    score_set_result = (
        db.query(ScoreSet)
        .filter(ScoreSet.experiment_id == experiment.id)
        .filter(~ScoreSet.superseding_score_set.has())
        .all()
    )

    filter_superseded_score_set_tails = [
        find_superseded_score_set_tail(score_set, Action.READ, user_data) for score_set in score_set_result
    ]
    filtered_score_sets = [score_set for score_set in filter_superseded_score_set_tails if score_set is not None]
    if not filtered_score_sets:
        save_to_logging_context({"associated_resources": []})
        logger.info(msg="No score sets are associated with the requested experiment.", extra=logging_context())

        raise HTTPException(status_code=404, detail="no associated score sets")

    filtered_score_sets.sort(key=attrgetter("urn"))
    save_to_logging_context({"associated_resources": [item.urn for item in score_set_result]})
    enriched_score_sets = []
    for fs in filtered_score_sets:
        enriched_experiment = enrich_experiment_with_num_score_sets(fs.experiment, user_data)
        response_item = score_set.ScoreSet.model_validate(fs).copy(update={"experiment": enriched_experiment})
        enriched_score_sets.append(response_item)

    return enriched_score_sets


@router.post(
    "/experiments/",
    status_code=200,
    response_model=experiment.Experiment,
    responses={
        **ACCESS_CONTROL_ERROR_RESPONSES,
        **GATEWAY_ERROR_RESPONSES,
    },
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
    logger.debug(msg="Began creation of new experiment.", extra=logging_context())

    experiment_set = None
    if item_create.experiment_set_urn is not None:
        experiment_set = (
            db.query(ExperimentSet).filter(ExperimentSet.urn == item_create.experiment_set_urn).one_or_none()
        )
        if not experiment_set:
            logger.info(
                msg="Could not create experiment; The requested experiment set does not exist.", extra=logging_context()
            )
            raise HTTPException(
                status_code=404,
                detail=f"experiment set with URN '{item_create.experiment_set_urn}' not found.",
            )

        save_to_logging_context({"experiment_set": experiment_set.urn})
        assert_permission(user_data, experiment_set, Action.ADD_EXPERIMENT)

        logger.debug(msg="Creating experiment within existing experiment set.", extra=logging_context())

    contributors: list[Contributor] = []
    try:
        contributors = [
            await find_or_create_contributor(db, contributor.orcid_id) for contributor in item_create.contributors or []
        ]
    except NonexistentOrcidUserError as e:
        logger.error(msg="Could not find ORCID user with the provided user ID.", extra=logging_context())
        raise HTTPException(status_code=404, detail=str(e))

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
        logger.error(msg="Gateway timed out while creating experiment identifiers.", extra=logging_context())
        raise HTTPException(
            status_code=504,
            detail="Gateway Timeout while attempting to contact PubMed/bioRxiv/medRxiv/Crossref APIs. Please try again later.",
        )

    except requests.exceptions.HTTPError:
        logger.error(msg="Encountered bad gateway while creating experiment identifiers.", extra=logging_context())
        raise HTTPException(
            status_code=502,
            detail="Bad Gateway while attempting to contact PubMed/bioRxiv/medRxiv/Crossref APIs. Please try again later.",
        )

    # create a temporary `primary` attribute on each of our publications that indicates
    # to our association proxy whether it is a primary publication or not
    primary_identifiers = [pub.identifier for pub in primary_publication_identifiers]
    for publication in publication_identifiers:
        setattr(publication, "primary", publication.identifier in primary_identifiers)

    # TODO: Controlled keywords currently is allowed none label.
    #  Will be changed in the future when we get the final list.
    keywords: list[ExperimentControlledKeywordAssociation] = []
    if item_create.keywords:
        all_labels_none = all(k.keyword.label is None for k in item_create.keywords)
        if all_labels_none is False:
            # Users may choose part of keywords from dropdown menu. Remove not chosen keywords from the list.
            filtered_keywords = list(filter(lambda k: k.keyword.label is not None, item_create.keywords))
            try:
                validate_keyword_list(filtered_keywords)
            except ValidationError as e:
                raise HTTPException(status_code=422, detail=str(e))
            for upload_keyword in filtered_keywords:
                try:
                    description = upload_keyword.description
                    controlled_keyword = search_keyword(db, upload_keyword.keyword.key, upload_keyword.keyword.label)
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
        keyword_objs=keywords,
    )  # type: ignore

    db.add(item)
    db.commit()
    db.refresh(item)

    save_to_logging_context({"created_resource": item.urn})
    return item


@router.put(
    "/experiments/{urn}",
    status_code=200,
    response_model=experiment.Experiment,
    responses={
        **ACCESS_CONTROL_ERROR_RESPONSES,
        **GATEWAY_ERROR_RESPONSES,
    },
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
    logger.debug(msg="Began experiment update.", extra=logging_context())

    # item = db.query(Experiment).filter(Experiment.urn == urn).filter(Experiment.private.is_(False)).one_or_none()
    item = db.query(Experiment).filter(Experiment.urn == urn).one_or_none()
    if item is None:
        logger.info(
            msg="Failed to update experiment; The requested experiment does not exist.", extra=logging_context()
        )
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
        setattr(item, var, value)

    try:
        item.contributors = [
            await find_or_create_contributor(db, contributor.orcid_id) for contributor in item_update.contributors or []
        ]
    except NonexistentOrcidUserError as e:
        logger.error(msg="Could not find ORCID user with the provided user ID.", extra=logging_context())
        raise HTTPException(status_code=404, detail=str(e))

    try:
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

    except requests.exceptions.ConnectTimeout:
        logger.error(msg="Gateway timed out while creating experiment identifiers.", extra=logging_context())
        raise HTTPException(
            status_code=504,
            detail="Gateway Timeout while attempting to contact PubMed/bioRxiv/medRxiv/Crossref APIs. Please try again later.",
        )

    except requests.exceptions.HTTPError:
        logger.error(msg="Encountered bad gateway while creating experiment identifiers.", extra=logging_context())
        raise HTTPException(
            status_code=502,
            detail="Bad Gateway while attempting to contact PubMed/bioRxiv/medRxiv/Crossref APIs. Please try again later.",
        )

    # create a temporary `primary` attribute on each of our publications that indicates
    # to our association proxy whether it is a primary publication or not
    primary_identifiers = [pub.identifier for pub in primary_publication_identifiers]
    for publication in publication_identifiers:
        setattr(publication, "primary", publication.identifier in primary_identifiers)

    item.doi_identifiers = doi_identifiers
    item.publication_identifiers = publication_identifiers
    item.raw_read_identifiers = raw_read_identifiers

    if item_update.keywords:
        keywords: list[ExperimentControlledKeywordAssociation] = []
        all_labels_none = all(k.keyword.label is None for k in item_update.keywords)
        if all_labels_none is False:
            # Users may choose part of keywords from dropdown menu. Remove not chosen keywords from the list.
            filtered_keywords = list(filter(lambda k: k.keyword.label is not None, item_update.keywords))
            try:
                validate_keyword_list(filtered_keywords)
            except ValidationError as e:
                raise HTTPException(status_code=422, detail=str(e))
            for upload_keyword in filtered_keywords:
                try:
                    description = upload_keyword.description
                    controlled_keyword = search_keyword(db, upload_keyword.keyword.key, upload_keyword.keyword.label)
                    experiment_controlled_keyword = ExperimentControlledKeywordAssociation(
                        controlled_keyword=controlled_keyword,
                        description=description,
                    )
                    keywords.append(experiment_controlled_keyword)
                except ValueError as e:
                    raise HTTPException(status_code=422, detail=str(e))
        item.keyword_objs = keywords

    item.modified_by = user_data.user

    db.add(item)
    db.commit()
    db.refresh(item)

    save_to_logging_context({"updated_resource": item.urn})
    return enrich_experiment_with_num_score_sets(item, user_data)


@router.delete(
    "/experiments/{urn}",
    status_code=200,
    response_model=None,
    responses={**ACCESS_CONTROL_ERROR_RESPONSES},
    summary="Delete an experiment",
)
async def delete_experiment(
    *,
    urn: str,
    db: Session = Depends(deps.get_db),
    user_data: UserData = Depends(require_current_user),
) -> None:
    """
    Delete an experiment.
    """
    save_to_logging_context({"requested_resource": urn})

    item = db.query(Experiment).filter(Experiment.urn == urn).one_or_none()
    if not item:
        logger.info(
            msg="Could not delete the requested experiment; The requested experiment does not exist.",
            extra=logging_context(),
        )
        raise HTTPException(status_code=404, detail=f"experiment with URN '{urn}' not found.")

    assert_permission(user_data, item, Action.DELETE)

    db.delete(item)
    db.commit()
