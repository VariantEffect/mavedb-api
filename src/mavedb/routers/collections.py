import logging
from operator import attrgetter
from typing import Any, Optional

from fastapi import APIRouter, Depends, HTTPException
from fastapi.encoders import jsonable_encoder
from sqlalchemy import select
from sqlalchemy.orm import Session
from sqlalchemy.orm.exc import MultipleResultsFound, NoResultFound

from mavedb import deps
from mavedb.lib.authentication import UserData, get_current_user
from mavedb.lib.authorization import require_current_user_with_email
from mavedb.lib.logging import LoggedRoute
from mavedb.lib.logging.context import format_raised_exception_info_as_dict, logging_context, save_to_logging_context
from mavedb.lib.permissions import Action, assert_permission, has_permission
from mavedb.models.collection import Collection
from mavedb.models.collection_user_association import CollectionUserAssociation
from mavedb.models.enums.contribution_role import ContributionRole
from mavedb.models.experiment import Experiment
from mavedb.models.score_set import ScoreSet
from mavedb.models.user import User
from mavedb.view_models import collection

logger = logging.getLogger(__name__)

router = APIRouter(
    prefix="/api/v1",
    tags=["collections"],
    responses={404: {"description": "Not found"}},
    route_class=LoggedRoute,
)


@router.get(
    "/users/me/collections",
    status_code=200,
    response_model=list[collection.Collection],
    response_model_exclude_none=True,
)
def list_my_collections(
    *,
    db: Session = Depends(deps.get_db),
    user_data: Optional[UserData] = Depends(get_current_user),
) -> list[Collection]:
    """
    List my collections.
    """
    # TODO logging?

    # collections_result = db.scalars(select(Collection).where(user_data.user.id in Collection.users)).all()
    # TODO is this too inefficient to select everything? should we do permission checking in the select statement
    # rather than using the permissions module below?
    collections_result = db.execute(select(Collection)).scalars().all()

    collections_result[:] = [
        collection for collection in collections_result if has_permission(user_data, collection, Action.READ).permitted
    ]

    # TODO return http exception if no collections? or just return nothing?
    if not collections_result:
        logger.info(msg="No collections available.", extra=logging_context())

        raise HTTPException(status_code=404, detail="no collections available")
    else:
        collections_result.sort(key=attrgetter("urn"))

    return collections_result


@router.get(
    "/collections/{urn}",
    status_code=200,
    response_model=collection.Collection,
    responses={404: {}},
    response_model_exclude_none=True,
)
def fetch_collection(
    *,
    urn: str,
    db: Session = Depends(deps.get_db),
    user_data: Optional[UserData] = Depends(get_current_user),
) -> Collection:
    """
    Fetch a single collection by URN.
    """
    save_to_logging_context({"requested_resource": urn})

    # TODO scalars() with one_or_none()? or just one_or_none()?
    item = db.execute(select(Collection).where(Collection.urn == urn)).scalars().one_or_none()

    if not item:
        logger.debug(msg="The requested collection does not exist.", extra=logging_context())
        raise HTTPException(status_code=404, detail=f"Collection with URN {urn} not found")

    # TODO return admin view if user is mavedb admin? not done for score sets or experiments

    assert_permission(user_data, item, Action.READ)
    return item


@router.post(
    "/collections/",
    response_model=collection.Collection,
    responses={422: {}},
    response_model_exclude_none=True,
)
async def create_collection(
    *,
    item_create: collection.CollectionCreate,
    db: Session = Depends(deps.get_db),
    user_data: UserData = Depends(require_current_user_with_email),
) -> Any:
    """
    Create a collection.
    """
    logger.debug(msg="Began creation of new collection.", extra=logging_context())

    users = []
    user_orcid_ids = set()

    try:
        for admin in item_create.admins or []:
            admin_orcid = admin.orcid_id
            if admin_orcid not in user_orcid_ids:
                user = db.scalars(select(User).where(User.username == admin_orcid)).one()
                setattr(user, "role", ContributionRole.admin)
                users.append(user)
                user_orcid_ids.add(admin_orcid)

        for editor in item_create.editors or []:
            editor_orcid = editor.orcid_id
            if editor_orcid not in user_orcid_ids:
                user = db.scalars(select(User).where(User.username == editor_orcid)).one()
                setattr(user, "role", ContributionRole.editor)
                users.append(user)
                user_orcid_ids.add(editor_orcid)

        for viewer in item_create.viewers or []:
            viewer_orcid = viewer.orcid_id
            if viewer_orcid not in user_orcid_ids:
                user = db.scalars(select(User).where(User.username == viewer_orcid)).one()
                setattr(user, "role", ContributionRole.viewer)
                users.append(user)
                user_orcid_ids.add(viewer_orcid)

    except NoResultFound as e:
        save_to_logging_context(format_raised_exception_info_as_dict(e))
        logger.error(msg="No existing user found with the given ORCID iD", extra=logging_context())
        raise HTTPException(status_code=404, detail="No MaveDB user found with the given ORCID iD")

    except MultipleResultsFound as e:
        save_to_logging_context(format_raised_exception_info_as_dict(e))
        logger.error(msg="Multiple users found with the given ORCID iD", extra=logging_context())
        raise HTTPException(status_code=400, detail="Multiple MaveDB users found with the given ORCID iD")

    try:
        score_sets = [
            db.scalars(select(ScoreSet).where(ScoreSet.urn == score_set_urn)).one()
            for score_set_urn in item_create.score_set_urns or []
        ]

        experiments = [
            db.scalars(select(Experiment).where(Experiment.urn == experiment_urn)).one()
            for experiment_urn in item_create.experiment_urns or []
        ]

    except NoResultFound as e:
        save_to_logging_context(format_raised_exception_info_as_dict(e))
        logger.error(msg="No resource found with the given URN", extra=logging_context())
        raise HTTPException(status_code=404, detail="No resource found with the given URN")

    except MultipleResultsFound as e:
        save_to_logging_context(format_raised_exception_info_as_dict(e))
        logger.error(msg="Multiple resources found with the given URN", extra=logging_context())
        raise HTTPException(status_code=400, detail="Multiple resources found with the given URN")

    # TODO require MaveDB admin permissions to set badge name
    # in the current permissions model, the item needs to already exist to assert permission
    # so maybe we only want to allow modifying badge name, rather than setting it upon creation
    # or, we need to modify permission assertion to allow for it when an item doesn't exist
    # or even just check user data directly here.
    # or I could create the collection and then update it in the same function.
    # if item_create.badge_name:
    #     assert_permission(user_data, )

    item = Collection(
        **jsonable_encoder(
            item_create,
            by_alias=False,
            exclude={"viewers", "editors", "admins", "score_set_urns", "experiment_urns", "badge_name"},
        ),
        users=users,
        score_sets=score_sets,
        experiments=experiments,
        created_by=user_data.user,
        modified_by=user_data.user,
    )  # type: ignore

    db.add(item)
    db.commit()
    db.refresh(item)

    save_to_logging_context({"created_resource": item.urn})
    return item


@router.put(
    "/collections/{urn}",
    response_model=collection.Collection,
    responses={422: {}},
    response_model_exclude_none=True,
)
async def update_collection(
    *,
    item_update: collection.CollectionModify,
    urn: str,
    db: Session = Depends(deps.get_db),
    user_data: UserData = Depends(require_current_user_with_email),
) -> Any:
    """
    Modify a collection's metadata.
    """
    save_to_logging_context({"requested_resource": urn})
    logger.debug(msg="Began collection metadata update.", extra=logging_context())

    # TODO scalars() with one_or_none()? or just one_or_none()?
    item = db.execute(select(Collection).where(Collection.urn == urn)).scalars().one_or_none()
    # item = db.query(Collection).filter(Collection.urn == urn).one_or_none()
    if item is None:
        logger.info(
            msg="Failed to update collection; The requested collection does not exist.", extra=logging_context()
        )
        raise HTTPException(status_code=404, detail=f"experiment with URN {urn} not found")

    assert_permission(user_data, item, Action.UPDATE)

    # editors may update metadata, but not all editors can publish (which is just setting private to public)
    # TODO permissions check for making a public collection private (unpublishing)?
    if item.private and not item_update.private:
        assert_permission(user_data, item, Action.PUBLISH)

    if item_update.badge_name:
        assert_permission(user_data, item, Action.ADD_BADGE)

    pairs = {k: v for k, v in vars(item_update).items()}
    for var, value in pairs.items():  # vars(item_update).items():
        setattr(item, var, value) if value else None

    item.modified_by = user_data.user

    db.add(item)
    db.commit()
    db.refresh(item)

    save_to_logging_context({"updated_resource": item.urn})
    return item


@router.post(
    "/collections/{collection_urn}/score-sets",
    response_model=str,  # TODO this needs to be a scores set urn view model for documentation purposes
    responses={422: {}},
)
async def add_score_set_to_collection(
    *,
    score_set_urn: str,
    collection_urn: str,
    db: Session = Depends(deps.get_db),
    user_data: UserData = Depends(require_current_user_with_email),
) -> Any:
    """
    Add an existing score set to an existing collection.
    """
    save_to_logging_context(
        {"requested_resource": collection_urn}
    )  # TODO best way to label collection vs score set in logging?

    # TODO have Ben check this query
    item = db.execute(select(Collection).where(Collection.urn == collection_urn)).scalars().one_or_none()
    if not item:
        logger.info(
            msg="Failed to add score set to collection; The requested collection does not exist.",
            extra=logging_context(),
        )
        raise HTTPException(status_code=404, detail=f"collection with URN '{collection_urn}' not found")

    score_set = db.execute(select(ScoreSet).where(ScoreSet.urn == score_set_urn)).scalars().one_or_none()
    if not score_set:
        logger.info(
            msg="Failed to add score set to collection; The requested score set does not exist.",
            extra=logging_context(),
        )
        raise HTTPException(status_code=404, detail=f"score set with URN '{score_set_urn}' not found")

    assert_permission(user_data, item, Action.ADD_SCORE_SET)

    item.score_sets.append(score_set)
    item.modified_by = user_data.user

    db.add(item)
    db.commit()
    db.refresh(item)

    save_to_logging_context({"updated_resource": item.urn})
    # TODO return whole collection, or just the list of the collection's score sets?
    return item


@router.delete("/collections/{collection_urn}/score-sets/{score_set_urn}", responses={422: {}})
async def delete_score_set_from_collection(
    *,
    collection_urn: str,
    score_set_urn: str,
    db: Session = Depends(deps.get_db),
    user_data: UserData = Depends(require_current_user_with_email),
) -> Any:
    """
    Remove a score set from an existing collection. Preserves the score set in the database, only removes the association between the score set and the collection.
    """
    save_to_logging_context(
        {"requested_resource": collection_urn}
    )  # TODO best way to label collection vs score set in logging?

    # TODO have Ben check this query
    item = db.execute(select(Collection).where(Collection.urn == collection_urn)).scalars().one_or_none()
    if not item:
        logger.info(
            msg="Failed to remove score set from collection; The requested collection does not exist.",
            extra=logging_context(),
        )
        raise HTTPException(status_code=404, detail=f"collection with URN '{collection_urn}' not found")

    score_set = db.execute(select(ScoreSet).where(ScoreSet.urn == score_set_urn)).scalars().one_or_none()
    if not score_set:
        logger.info(
            msg="Failed to remove score set from collection; The requested score set does not exist.",
            extra=logging_context(),
        )
        raise HTTPException(status_code=404, detail=f"score set with URN '{score_set_urn}' not found")

    if score_set not in item.score_sets:
        logger.info(
            msg="Failed to remove score set from collection; The requested score set is not associated with the requested collection.",
            extra=logging_context(),
        )
        # TODO what kind of http error is this?
        raise HTTPException(
            status_code=404,
            detail=f"association between score set '{score_set_urn}' and collection '{collection_urn}' not found",
        )

    # add and remove permissions are the same
    assert_permission(user_data, item, Action.ADD_SCORE_SET)

    item.score_sets.remove(score_set)
    item.modified_by = user_data.user

    db.add(item)
    db.commit()
    db.refresh(item)

    save_to_logging_context({"updated_resource": item.urn})
    # TODO return whole collection, or just the list of the collection's score sets?
    return item


@router.post(
    "/collections/{collection_urn}/experiments",
    response_model=str,  # TODO this needs to be a experiment urn view model for documentation purposes
    responses={422: {}},
)
async def add_experiment_to_collection(
    *,
    experiment_urn: str,
    collection_urn: str,
    db: Session = Depends(deps.get_db),
    user_data: UserData = Depends(require_current_user_with_email),
) -> Any:
    """
    Add an existing experiment to an existing collection.
    """
    save_to_logging_context(
        {"requested_resource": collection_urn}
    )  # TODO best way to label collection vs score set in logging?

    # TODO have Ben check this query
    item = db.execute(select(Collection).where(Collection.urn == collection_urn)).scalars().one_or_none()
    if not item:
        logger.info(
            msg="Failed to add experiment to collection; The requested collection does not exist.",
            extra=logging_context(),
        )
        raise HTTPException(status_code=404, detail=f"collection with URN '{collection_urn}' not found")

    experiment = db.execute(select(Experiment).where(Experiment.urn == experiment_urn)).scalars().one_or_none()
    if not experiment:
        logger.info(
            msg="Failed to add experiment to collection; The requested experiment does not exist.",
            extra=logging_context(),
        )
        raise HTTPException(status_code=404, detail=f"experiment with URN '{experiment_urn}' not found")

    assert_permission(user_data, item, Action.ADD_EXPERIMENT)

    item.experiments.append(experiment)
    item.modified_by = user_data.user

    db.add(item)
    db.commit()
    db.refresh(item)

    save_to_logging_context({"updated_resource": item.urn})
    # TODO return whole collection, or just the list of the collection's score sets?
    return item


@router.delete("/collections/{collection_urn}/experiments/{experiment_urn}", responses={422: {}})
async def delete_experiment_from_collection(
    *,
    collection_urn: str,
    experiment_urn: str,
    db: Session = Depends(deps.get_db),
    user_data: UserData = Depends(require_current_user_with_email),
) -> Any:
    """
    Remove an experiment from an existing collection. Preserves the experiment in the database, only removes the association between the experiment and the collection.
    """
    save_to_logging_context(
        {"requested_resource": collection_urn}
    )  # TODO best way to label collection vs score set in logging?

    # TODO have Ben check this query
    item = db.execute(select(Collection).where(Collection.urn == collection_urn)).scalars().one_or_none()
    if not item:
        logger.info(
            msg="Failed to remove experiment from collection; The requested collection does not exist.",
            extra=logging_context(),
        )
        raise HTTPException(status_code=404, detail=f"collection with URN '{collection_urn}' not found")

    experiment = db.execute(select(Experiment).where(Experiment.urn == experiment_urn)).scalars().one_or_none()
    if not experiment:
        logger.info(
            msg="Failed to remove experiment from collection; The requested experiment does not exist.",
            extra=logging_context(),
        )
        raise HTTPException(status_code=404, detail=f"experiment with URN '{experiment_urn}' not found")

    if experiment not in item.experiments:
        logger.info(
            msg="Failed to remove experiment from collection; The requested experiment is not associated with the requested collection.",
            extra=logging_context(),
        )
        # TODO what kind of http error is this?
        raise HTTPException(
            status_code=404,
            detail=f"association between experiment '{experiment_urn}' and collection '{collection_urn}' not found",
        )

    # add and remove permissions are the same
    assert_permission(user_data, item, Action.ADD_EXPERIMENT)

    item.experiments.remove(experiment)
    item.modified_by = user_data.user

    db.add(item)
    db.commit()
    db.refresh(item)

    save_to_logging_context({"updated_resource": item.urn})
    # TODO return whole collection, or just the list of the collection's score sets?
    return item


@router.post(
    "/collections/{urn}/{role}",
    response_model=str,  # TODO need view model for user orcid id
    responses={422: {}},
)
async def add_user_to_collection_role(
    *,
    orcid_id: str,
    urn: str,
    role: ContributionRole,
    db: Session = Depends(deps.get_db),
    user_data: UserData = Depends(require_current_user_with_email),
) -> Any:
    """
    Add an existing user to a collection under the specified role.
    Removes the user from any other roles in this collection.
    """
    save_to_logging_context({"requested_resource": urn})

    # TODO have Ben check this query
    item = db.execute(select(Collection).where(Collection.urn == urn)).scalars().one_or_none()
    # item = db.query(Collection).filter((Collection).urn == urn).one_or_none()
    if not item:
        logger.info(
            msg="Failed to add user to collection role; The requested collection does not exist.",
            extra=logging_context(),
        )
        raise HTTPException(status_code=404, detail=f"collection with URN '{urn}' not found")

    # TODO have Ben check this query
    user = db.execute(select(User).where(User.username == orcid_id)).scalars().one_or_none()
    if not user:
        logger.info(
            msg="Failed to add user to collection role; The requested user does not exist.", extra=logging_context()
        )
        raise HTTPException(status_code=404, detail=f"user with ORCID iD '{orcid_id}' not found")

    # get current user role
    # TODO there is probably a nicer way to select this since we've already selected the user and collection?
    collection_user_association = (
        db.execute(
            select(CollectionUserAssociation).where(
                CollectionUserAssociation.collection_id == item.id and CollectionUserAssociation.user_id == User.id
            )
        )
        .scalars()
        .one_or_none()
    )

    assert_permission(user_data, item, Action.ADD_ROLE)

    # Since this is a post request, user should not already be in this role
    if collection_user_association.contribution_role == role:
        logger.info(
            msg="Failed to add user to collection role; the requested user already has the requested role for this collection.",
            extra=logging_context(),
        )
        # TODO what error code?
        raise HTTPException(
            status_code=404,
            detail=f"user with ORCID iD '{orcid_id}' is already a {role} for collection '{urn}'",
        )
    # A user can only be in one role per collection, so remove from any other roles
    # TODO I think it's easiest just to delete the user from this collection, then add them back,
    # in order to do the adding the same way whether the user already has a contribution role for this collection or not.
    elif collection_user_association.contribution_role:
        item.users.remove(User)

    setattr(user, "role", role)
    item.users.append(user)

    item.modified_by = user_data.user

    db.add(item)
    db.commit()
    db.refresh(item)

    save_to_logging_context({"updated_resource": item.urn})
    # TODO return whole collection, or just the list of the collection's users with this role?
    return item


@router.delete("/collections/{urn}/{role}/{orcid_id}", responses={422: {}})
async def remove_user_from_collection_role(
    *,
    urn: str,
    role: ContributionRole,
    orcid_id: str,
    db: Session = Depends(deps.get_db),
    user_data: UserData = Depends(require_current_user_with_email),
) -> Any:
    """
    Remove a user from a collection role.
    """
    save_to_logging_context({"requested_resource": urn})

    # TODO have Ben check this query
    item = db.execute(select(Collection).where(Collection.urn == urn)).scalars().one_or_none()
    # item = db.query(Collection).filter((Collection).urn == urn).one_or_none()
    if not item:
        logger.info(
            msg="Failed to add user to collection role; The requested collection does not exist.",
            extra=logging_context(),
        )
        raise HTTPException(status_code=404, detail=f"collection with URN '{urn}' not found")

    # TODO have Ben check this query
    user = db.execute(select(User).where(User.username == orcid_id)).scalars().one_or_none()
    if not user:
        logger.info(
            msg="Failed to add user to collection role; The requested user does not exist.", extra=logging_context()
        )
        raise HTTPException(status_code=404, detail=f"user with ORCID iD '{orcid_id}' not found")

    # get current user role
    # TODO there is probably a nicer way to select this since we've already selected the user and collection?
    collection_user_association = (
        db.execute(
            select(CollectionUserAssociation).where(
                CollectionUserAssociation.collection_id == item.id and CollectionUserAssociation.user_id == User.id
            )
        )
        .scalars()
        .one_or_none()
    )

    # TODO add and delete permissions for collection role are the same I assume?
    assert_permission(user_data, item, Action.ADD_ROLE)

    # Since this is a post request, user should not already be in this role
    if collection_user_association.contribution_role != role:
        logger.info(
            msg="Failed to remove user from collection role; the requested user does not currently hold the requested role for this collection.",
            extra=logging_context(),
        )
        # TODO what error code?
        raise HTTPException(
            status_code=404,
            detail=f"user with ORCID iD '{orcid_id}' does not currently hold the role {role} for collection '{urn}'",
        )

    item.users.remove(User)
    item.modified_by = user_data.user

    db.add(item)
    db.commit()
    db.refresh(item)

    save_to_logging_context({"updated_resource": item.urn})
    # TODO return whole collection, or just the list of the collection's users with this role?
    return item


@router.delete("/collections/{urn}", responses={422: {}})
async def delete_collection(
    *,
    urn: str,
    db: Session = Depends(deps.get_db),
    user_data: UserData = Depends(require_current_user_with_email),
) -> Any:
    """
    Delete a collection.
    """
    save_to_logging_context({"requested_resource": urn})

    # TODO have Ben check this query
    item = db.execute(select(Collection).where(Collection.urn == urn)).scalars().one_or_none()
    # item = db.query(Collection).filter((Collection).urn == urn).one_or_none()
    if not item:
        logger.info(
            msg="Failed to delete collection; The requested collection does not exist.", extra=logging_context()
        )
        raise HTTPException(status_code=404, detail=f"collection with URN '{urn}' not found")

    assert_permission(user_data, item, Action.DELETE)

    db.delete(item)
    db.commit()