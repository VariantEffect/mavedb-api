import logging
from datetime import date
from typing import Any, Dict, Optional, Sequence

from fastapi import APIRouter, Depends, HTTPException
from fastapi.encoders import jsonable_encoder
from sqlalchemy import and_, select
from sqlalchemy.orm import Session
from sqlalchemy.orm.exc import MultipleResultsFound, NoResultFound

from mavedb import deps
from mavedb.lib.authentication import get_current_user
from mavedb.lib.authorization import require_current_user, require_current_user_with_email
from mavedb.lib.logging import LoggedRoute
from mavedb.lib.logging.context import (
    format_raised_exception_info_as_dict,
    logging_context,
    save_to_logging_context,
)
from mavedb.lib.permissions import Action, assert_permission, has_permission
from mavedb.lib.types.authentication import UserData
from mavedb.models.collection import Collection
from mavedb.models.collection_experiment_association import CollectionExperimentAssociation
from mavedb.models.collection_score_set_association import CollectionScoreSetAssociation
from mavedb.models.collection_user_association import CollectionUserAssociation
from mavedb.models.enums.contribution_role import ContributionRole
from mavedb.models.experiment import Experiment
from mavedb.models.score_set import ScoreSet
from mavedb.models.user import User
from mavedb.routers.shared import (
    ACCESS_CONTROL_ERROR_RESPONSES,
    BASE_400_RESPONSE,
    BASE_409_RESPONSE,
    PUBLIC_ERROR_RESPONSES,
    ROUTER_BASE_PREFIX,
)
from mavedb.view_models import collection, collection_bundle

TAG_NAME = "Collections"

logger = logging.getLogger(__name__)

router = APIRouter(
    prefix=f"{ROUTER_BASE_PREFIX}",
    tags=[TAG_NAME],
    responses={**PUBLIC_ERROR_RESPONSES},
    route_class=LoggedRoute,
)

metadata = {
    "name": TAG_NAME,
    "description": "Manage the members and permissions of data set collections.",
}


@router.get(
    "/users/me/collections",
    status_code=200,
    response_model=collection_bundle.CollectionBundle,
    response_model_exclude_none=True,
    responses={**ACCESS_CONTROL_ERROR_RESPONSES},
    summary="List my collections",
)
def list_my_collections(
    *,
    db: Session = Depends(deps.get_db),
    user_data: UserData = Depends(require_current_user),
) -> Dict[str, Sequence[Collection]]:
    """
    List the current user's collections. These are all the collections the user either owns or
    is listed as a contributor (in any role).
    """
    collection_bundle: Dict[str, Sequence[Collection]] = {}
    for role in ContributionRole:
        collection_bundle[role.value] = (
            db.execute(
                select(Collection)
                .join(CollectionUserAssociation)
                .where(CollectionUserAssociation.user_id == user_data.user.id)
                .where(CollectionUserAssociation.contribution_role == role.value)
            )
            .scalars()
            .all()
        )

        for item in collection_bundle[role.value]:
            # filter score set and experiment associations based on user permissions
            # work with associations directly to preserve position ordering
            item.score_set_associations = [
                assoc
                for assoc in item.score_set_associations
                if has_permission(user_data, assoc.score_set, Action.READ)
            ]
            item.experiment_associations = [
                assoc
                for assoc in item.experiment_associations
                if has_permission(user_data, assoc.experiment, Action.READ)
            ]
            # unless user is admin of this collection, filter users to only admins
            # the rationale is that all collection contributors should be able to see admins
            # to know who to contact, but only collection admins should be able to see viewers and editors
            if role in (ContributionRole.viewer, ContributionRole.editor):
                admins = []
                for user_assoc in item.user_associations:
                    if user_assoc.contribution_role == ContributionRole.admin:
                        admin = user_assoc.user
                        # role must be set in order to assign users to collection
                        setattr(admin, "role", ContributionRole.admin)
                        admins.append(admin)
                item.users = admins

    return collection_bundle


@router.get(
    "/collections/{urn}",
    status_code=200,
    response_model=collection.Collection,
    responses={**ACCESS_CONTROL_ERROR_RESPONSES},
    response_model_exclude_none=True,
    summary="Fetch a collection by URN",
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

    item = db.execute(select(Collection).where(Collection.urn == urn)).scalars().one_or_none()
    if not item:
        logger.debug(msg="The requested collection does not exist.", extra=logging_context())
        raise HTTPException(status_code=404, detail=f"Collection with URN {urn} not found")

    assert_permission(user_data, item, Action.READ)
    # filter score set and experiment associations based on user permissions
    # work with associations directly to preserve position ordering
    item.score_set_associations = [
        assoc for assoc in item.score_set_associations if has_permission(user_data, assoc.score_set, Action.READ)
    ]
    item.experiment_associations = [
        assoc for assoc in item.experiment_associations if has_permission(user_data, assoc.experiment, Action.READ)
    ]

    # Only collection admins can see all user roles for the collection. Other users can only see the list of admins.
    # We could create a new permission action for this. But for now, assume that any user who has the ADD_ROLE
    # permission is a collection admin and should be able to see all user roles for the collection.
    if not has_permission(user_data, item, Action.ADD_ROLE):
        admins = []
        for user_assoc in item.user_associations:
            if user_assoc.contribution_role == ContributionRole.admin:
                admin = user_assoc.user
                # role must be set in order to assign users to collection
                setattr(admin, "role", ContributionRole.admin)
                admins.append(admin)
        item.users = admins

    return item


@router.post(
    "/collections/",
    response_model=collection.Collection,
    responses={**BASE_400_RESPONSE, **ACCESS_CONTROL_ERROR_RESPONSES},
    response_model_exclude_none=True,
    summary="Create a collection",
)
async def create_collection(
    *,
    item_create: collection.CollectionCreate,
    db: Session = Depends(deps.get_db),
    user_data: UserData = Depends(require_current_user_with_email),
) -> Any:
    """
    Create a new collection owned by the current user.

    The order of URNs in score_set_urns and experiment_urns determines the display
    order in the collection. This order is preserved and can be modified later using
    the PATCH endpoint.
    """
    logger.debug(msg="Began creation of new collection.", extra=logging_context())

    users = []
    user_orcid_ids = set()

    try:
        # always assign creator as admin, as collections permissions do not distinguish between owner/creator and admin
        creator_user = user_data.user
        setattr(creator_user, "role", ContributionRole.admin)
        users.append(creator_user)
        user_orcid_ids.add(creator_user.username)

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
        logger.error(
            msg="No existing user found with the given ORCID iD",
            extra=logging_context(),
        )
        raise HTTPException(status_code=404, detail="No MaveDB user found with the given ORCID iD")

    except MultipleResultsFound as e:
        save_to_logging_context(format_raised_exception_info_as_dict(e))
        logger.error(msg="Multiple users found with the given ORCID iD", extra=logging_context())
        raise HTTPException(
            status_code=500,
            detail="Multiple MaveDB users found with the given ORCID iD",
        )

    try:
        score_set_associations = []
        for position, score_set_urn in enumerate(item_create.score_set_urns or []):
            score_set = db.scalars(select(ScoreSet).where(ScoreSet.urn == score_set_urn)).one()
            score_set_associations.append(CollectionScoreSetAssociation(score_set=score_set, position=position))

        experiment_associations = []
        for position, experiment_urn in enumerate(item_create.experiment_urns or []):
            experiment = db.scalars(select(Experiment).where(Experiment.urn == experiment_urn)).one()
            experiment_associations.append(CollectionExperimentAssociation(experiment=experiment, position=position))

    except NoResultFound as e:
        save_to_logging_context(format_raised_exception_info_as_dict(e))
        logger.error(msg="No resource found with the given URN", extra=logging_context())
        raise HTTPException(status_code=404, detail="No resource found with the given URN")

    except MultipleResultsFound as e:
        save_to_logging_context(format_raised_exception_info_as_dict(e))
        logger.error(msg="Multiple resources found with the given URN", extra=logging_context())
        raise HTTPException(status_code=500, detail="Multiple resources found with the given URN")

    item = Collection(
        **jsonable_encoder(
            item_create,
            by_alias=False,
            exclude={
                "viewers",
                "editors",
                "admins",
                "score_set_urns",
                "experiment_urns",
                "badge_name",
            },
        ),
        users=users,
        score_set_associations=score_set_associations,
        experiment_associations=experiment_associations,
        created_by=user_data.user,
        modified_by=user_data.user,
    )  # type: ignore

    db.add(item)
    db.commit()
    db.refresh(item)

    save_to_logging_context({"created_resource": item.urn})
    return item


@router.patch(
    "/collections/{urn}",
    response_model=collection.Collection,
    responses={**BASE_400_RESPONSE, **ACCESS_CONTROL_ERROR_RESPONSES},
    response_model_exclude_none=True,
    summary="Update a collection",
)
async def update_collection(
    *,
    item_update: collection.CollectionModify,
    urn: str,
    db: Session = Depends(deps.get_db),
    user_data: UserData = Depends(require_current_user_with_email),
) -> Any:
    """
    Modify a collection's metadata. Also supports reordering and modifying collection membership
    via score_set_urns and experiment_urns fields (replace-all with implicit add/remove).

    When score_set_urns or experiment_urns are provided, the order of URNs in the array determines
    the display order in the collection. The provided list replaces the entire set of associations:
    URNs not in the list are removed, new URNs are added, and the order is updated to match.
    """
    save_to_logging_context({"requested_resource": urn})
    logger.debug(msg="Began collection metadata update.", extra=logging_context())

    item = db.execute(select(Collection).where(Collection.urn == urn)).scalars().one_or_none()
    if item is None:
        logger.info(
            msg="Failed to update collection; The requested collection does not exist.",
            extra=logging_context(),
        )
        raise HTTPException(status_code=404, detail=f"collection with URN {urn} not found")

    assert_permission(user_data, item, Action.UPDATE)

    # Ensure users have permission to make the specific changes they are attempting to make.
    if item_update.private is not None and item.private != item_update.private:
        assert_permission(user_data, item, Action.PUBLISH)

    if item_update.badge_name is not None and item.badge_name != item_update.badge_name:
        assert_permission(user_data, item, Action.ADD_BADGE)

    # Handle score_set_urns: replace-all with implicit add/remove
    if "score_set_urns" in item_update.model_fields_set and item_update.score_set_urns is not None:
        # Check if this implies membership changes (additions or removals)
        current_urns = {assoc.score_set.urn for assoc in item.score_set_associations}
        new_urns = set(item_update.score_set_urns)
        if current_urns != new_urns:
            # Membership is changing, require ADD_SCORE_SET permission
            assert_permission(user_data, item, Action.ADD_SCORE_SET)

        try:
            # Clear existing associations
            item.score_set_associations.clear()
            db.flush()

            # Build new ordered associations
            for position, score_set_urn in enumerate(item_update.score_set_urns):
                score_set = db.scalars(select(ScoreSet).where(ScoreSet.urn == score_set_urn)).one()
                item.score_set_associations.append(
                    CollectionScoreSetAssociation(score_set=score_set, position=position)
                )
        except NoResultFound:
            logger.error(msg="No score set found with the given URN", extra=logging_context())
            raise HTTPException(status_code=404, detail="No score set found with the given URN")
        except MultipleResultsFound:
            logger.error(msg="Multiple score sets found with the given URN", extra=logging_context())
            raise HTTPException(status_code=500, detail="Multiple score sets found with the given URN")

    # Handle experiment_urns: replace-all with implicit add/remove
    if "experiment_urns" in item_update.model_fields_set and item_update.experiment_urns is not None:
        # Check if this implies membership changes
        current_urns = {assoc.experiment.urn for assoc in item.experiment_associations}
        new_urns = set(item_update.experiment_urns)
        if current_urns != new_urns:
            # Membership is changing, require ADD_EXPERIMENT permission
            assert_permission(user_data, item, Action.ADD_EXPERIMENT)

        try:
            # Clear existing associations
            item.experiment_associations.clear()
            db.flush()

            # Build new ordered associations
            for position, experiment_urn in enumerate(item_update.experiment_urns):
                experiment = db.scalars(select(Experiment).where(Experiment.urn == experiment_urn)).one()
                item.experiment_associations.append(
                    CollectionExperimentAssociation(experiment=experiment, position=position)
                )
        except NoResultFound:
            logger.error(msg="No experiment found with the given URN", extra=logging_context())
            raise HTTPException(status_code=404, detail="No experiment found with the given URN")
        except MultipleResultsFound:
            logger.error(msg="Multiple experiments found with the given URN", extra=logging_context())
            raise HTTPException(status_code=500, detail="Multiple experiments found with the given URN")

    # Only access non-URN fields for generic setattr
    pairs = {
        k: v
        for k, v in vars(item_update).items()
        if k in item_update.model_fields_set and k not in {"score_set_urns", "experiment_urns"}
    }
    for var, value in pairs.items():
        setattr(item, var, value)

    item.modified_by = user_data.user

    db.add(item)
    db.commit()
    db.refresh(item)

    save_to_logging_context({"updated_resource": item.urn})
    # filter score set and experiment associations based on user permissions
    # note that this filtering occurs after saving changes to db; the filtering is only for the returned view model
    # work with associations directly to preserve position ordering
    item.score_set_associations = [
        assoc for assoc in item.score_set_associations if has_permission(user_data, assoc.score_set, Action.READ)
    ]
    item.experiment_associations = [
        assoc for assoc in item.experiment_associations if has_permission(user_data, assoc.experiment, Action.READ)
    ]

    # Only collection admins can see all user roles for the collection. Other users can only see the list of admins.
    # We could create a new permission action for this. But for now, assume that any user who has the ADD_ROLE
    # permission is a collection admin and should be able to see all user roles for the collection.
    if not has_permission(user_data, item, Action.ADD_ROLE):
        admins = []
        for user_assoc in item.user_associations:
            if user_assoc.contribution_role == ContributionRole.admin:
                admin = user_assoc.user
                # role must be set in order to assign users to collection
                setattr(admin, "role", ContributionRole.admin)
                admins.append(admin)
        item.users = admins

    return item


@router.post(
    "/collections/{collection_urn}/score-sets",
    response_model=collection.Collection,
    responses={
        401: {"description": "Not authenticated"},
        403: {"description": "User lacks necessary permissions"},
    },
    summary="Add a score set to a collection",
)
async def add_score_set_to_collection(
    *,
    body: collection.AddScoreSetToCollectionRequest,
    collection_urn: str,
    db: Session = Depends(deps.get_db),
    user_data: UserData = Depends(require_current_user_with_email),
) -> Any:
    """
    Add an existing score set to an existing collection.

    The score set will be appended to the end of the collection's score set list.
    To specify a different position, use the PATCH endpoint with the full ordered list.
    """
    save_to_logging_context({"requested_resource": collection_urn})

    item = db.execute(select(Collection).where(Collection.urn == collection_urn)).scalars().one_or_none()
    if not item:
        logger.info(
            msg="Failed to add score set to collection; The requested collection does not exist.",
            extra=logging_context(),
        )
        raise HTTPException(status_code=404, detail=f"collection with URN '{collection_urn}' not found")

    score_set = db.execute(select(ScoreSet).where(ScoreSet.urn == body.score_set_urn)).scalars().one_or_none()
    if not score_set:
        logger.info(
            msg="Failed to add score set to collection; The requested score set does not exist.",
            extra=logging_context(),
        )
        raise HTTPException(
            status_code=404,
            detail=f"score set with URN '{body.score_set_urn}' not found",
        )

    assert_permission(user_data, item, Action.ADD_SCORE_SET)

    # Append to end with next available position
    next_position = len(item.score_set_associations)
    item.score_set_associations.append(CollectionScoreSetAssociation(score_set=score_set, position=next_position))
    item.modification_date = date.today()
    item.modified_by = user_data.user

    db.add(item)
    db.commit()
    db.refresh(item)

    save_to_logging_context({"updated_resource": item.urn})

    # filter score set and experiment associations based on user permissions
    # note that this filtering occurs after saving changes to db; the filtering is only for the returned view model
    # work with associations directly to preserve position ordering
    item.score_set_associations = [
        assoc for assoc in item.score_set_associations if has_permission(user_data, assoc.score_set, Action.READ)
    ]
    item.experiment_associations = [
        assoc for assoc in item.experiment_associations if has_permission(user_data, assoc.experiment, Action.READ)
    ]

    # Only collection admins can see all user roles for the collection. Other users can only see the list of admins.
    # We could create a new permission action for this. But for now, assume that any user who has the ADD_ROLE
    # permission is a collection admin and should be able to see all user roles for the collection.
    if not has_permission(user_data, item, Action.ADD_ROLE):
        admins = []
        for user_assoc in item.user_associations:
            if user_assoc.contribution_role == ContributionRole.admin:
                admin = user_assoc.user
                # role must be set in order to assign users to collection
                setattr(admin, "role", ContributionRole.admin)
                admins.append(admin)
        item.users = admins

    return item


@router.delete(
    "/collections/{collection_urn}/score-sets/{score_set_urn}",
    response_model=collection.Collection,
    responses={**ACCESS_CONTROL_ERROR_RESPONSES, **BASE_409_RESPONSE},
    summary="Remove a score set from a collection",
)
async def delete_score_set_from_collection(
    *,
    collection_urn: str,
    score_set_urn: str,
    db: Session = Depends(deps.get_db),
    user_data: UserData = Depends(require_current_user_with_email),
) -> Any:
    """
    Remove a score set from an existing collection. The score set will be preserved in the database. This endpoint will only remove
    the association between the score set and the collection.
    """
    save_to_logging_context({"requested_resource": collection_urn})

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

    # Find and verify the association exists
    assoc_to_remove = next((a for a in item.score_set_associations if a.score_set.urn == score_set_urn), None)
    if not assoc_to_remove:
        logger.info(
            msg="Failed to remove score set from collection; The requested score set is not associated with the requested collection.",
            extra=logging_context(),
        )
        raise HTTPException(
            status_code=409,
            detail=f"association between score set '{score_set_urn}' and collection '{collection_urn}' not found",
        )

    # add and remove permissions are the same
    assert_permission(user_data, item, Action.ADD_SCORE_SET)

    # Remove the association
    item.score_set_associations.remove(assoc_to_remove)

    # Re-index remaining associations to maintain contiguous positions
    for idx, assoc in enumerate(item.score_set_associations):
        assoc.position = idx

    item.modification_date = date.today()
    item.modified_by = user_data.user

    db.add(item)
    db.commit()
    db.refresh(item)

    save_to_logging_context({"updated_resource": item.urn})

    # filter score set and experiment associations based on user permissions
    # note that this filtering occurs after saving changes to db; the filtering is only for the returned view model
    # work with associations directly to preserve position ordering
    item.score_set_associations = [
        assoc for assoc in item.score_set_associations if has_permission(user_data, assoc.score_set, Action.READ)
    ]
    item.experiment_associations = [
        assoc for assoc in item.experiment_associations if has_permission(user_data, assoc.experiment, Action.READ)
    ]

    # Only collection admins can see all user roles for the collection. Other users can only see the list of admins.
    # We could create a new permission action for this. But for now, assume that any user who has the ADD_ROLE
    # permission is a collection admin and should be able to see all user roles for the collection.
    if not has_permission(user_data, item, Action.ADD_ROLE):
        admins = []
        for user_assoc in item.user_associations:
            if user_assoc.contribution_role == ContributionRole.admin:
                admin = user_assoc.user
                # role must be set in order to assign users to collection
                setattr(admin, "role", ContributionRole.admin)
                admins.append(admin)
        item.users = admins

    return item


@router.post(
    "/collections/{collection_urn}/experiments",
    response_model=collection.Collection,
    responses={**ACCESS_CONTROL_ERROR_RESPONSES},
    summary="Add an experiment to a collection",
)
async def add_experiment_to_collection(
    *,
    body: collection.AddExperimentToCollectionRequest,
    collection_urn: str,
    db: Session = Depends(deps.get_db),
    user_data: UserData = Depends(require_current_user_with_email),
) -> Any:
    """
    Add an existing experiment to an existing collection.

    The experiment will be appended to the end of the collection's experiment list.
    To specify a different position, use the PATCH endpoint with the full ordered list.
    """
    save_to_logging_context({"requested_resource": collection_urn})

    item = db.execute(select(Collection).where(Collection.urn == collection_urn)).scalars().one_or_none()
    if not item:
        logger.info(
            msg="Failed to add experiment to collection; The requested collection does not exist.",
            extra=logging_context(),
        )
        raise HTTPException(status_code=404, detail=f"collection with URN '{collection_urn}' not found")

    experiment = db.execute(select(Experiment).where(Experiment.urn == body.experiment_urn)).scalars().one_or_none()
    if not experiment:
        logger.info(
            msg="Failed to add experiment to collection; The requested experiment does not exist.",
            extra=logging_context(),
        )
        raise HTTPException(
            status_code=404,
            detail=f"experiment with URN '{body.experiment_urn}' not found",
        )

    assert_permission(user_data, item, Action.ADD_EXPERIMENT)

    # Append to end with next available position
    next_position = len(item.experiment_associations)
    item.experiment_associations.append(CollectionExperimentAssociation(experiment=experiment, position=next_position))
    item.modification_date = date.today()
    item.modified_by = user_data.user

    db.add(item)
    db.commit()
    db.refresh(item)
    # note that this filtering occurs after saving changes to db; the filtering is only for the returned view model
    # work with associations directly to preserve position ordering
    item.score_set_associations = [
        assoc for assoc in item.score_set_associations if has_permission(user_data, assoc.score_set, Action.READ)
    ]
    item.experiment_associations = [
        assoc for assoc in item.experiment_associations if has_permission(user_data, assoc.experiment, Action.READ)
    ]

    # Only collection admins can see all user roles for the collection. Other users can only see the list of admins.
    # We could create a new permission action for this. But for now, assume that any user who has the ADD_ROLE
    # permission is a collection admin and should be able to see all user roles for the collection.
    if not has_permission(user_data, item, Action.ADD_ROLE):
        admins = []
        for user_assoc in item.user_associations:
            if user_assoc.contribution_role == ContributionRole.admin:
                admin = user_assoc.user
                # role must be set in order to assign users to collection
                setattr(admin, "role", ContributionRole.admin)
                admins.append(admin)
        item.users = admins

    return item


@router.delete(
    "/collections/{collection_urn}/experiments/{experiment_urn}",
    response_model=collection.Collection,
    responses={**ACCESS_CONTROL_ERROR_RESPONSES, **BASE_409_RESPONSE},
    summary="Remove an experiment from a collection",
)
async def delete_experiment_from_collection(
    *,
    collection_urn: str,
    experiment_urn: str,
    db: Session = Depends(deps.get_db),
    user_data: UserData = Depends(require_current_user_with_email),
) -> Any:
    """
    Remove an experiment from an existing collection. The experiment will be preserved in the database. This endpoint will only remove
    the association between the experiment and the collection.
    """
    save_to_logging_context({"requested_resource": collection_urn})

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

    # Find and verify the association exists
    assoc_to_remove = next((a for a in item.experiment_associations if a.experiment.urn == experiment_urn), None)
    if not assoc_to_remove:
        logger.info(
            msg="Failed to remove experiment from collection; The requested experiment is not associated with the requested collection.",
            extra=logging_context(),
        )
        raise HTTPException(
            status_code=409,
            detail=f"association between experiment '{experiment_urn}' and collection '{collection_urn}' not found",
        )

    # add and remove permissions are the same
    assert_permission(user_data, item, Action.ADD_EXPERIMENT)

    # Remove the association
    item.experiment_associations.remove(assoc_to_remove)

    # Re-index remaining associations to maintain contiguous positions
    for idx, assoc in enumerate(item.experiment_associations):
        assoc.position = idx

    item.modification_date = date.today()
    item.modified_by = user_data.user

    db.add(item)
    db.commit()
    db.refresh(item)
    # note that this filtering occurs after saving changes to db; the filtering is only for the returned view model
    # work with associations directly to preserve position ordering
    item.score_set_associations = [
        assoc for assoc in item.score_set_associations if has_permission(user_data, assoc.score_set, Action.READ)
    ]
    item.experiment_associations = [
        assoc for assoc in item.experiment_associations if has_permission(user_data, assoc.experiment, Action.READ)
    ]

    # Only collection admins can see all user roles for the collection. Other users can only see the list of admins.
    # We could create a new permission action for this. But for now, assume that any user who has the ADD_ROLE
    # permission is a collection admin and should be able to see all user roles for the collection.
    if not has_permission(user_data, item, Action.ADD_ROLE):
        admins = []
        for user_assoc in item.user_associations:
            if user_assoc.contribution_role == ContributionRole.admin:
                admin = user_assoc.user
                # role must be set in order to assign users to collection
                setattr(admin, "role", ContributionRole.admin)
                admins.append(admin)
        item.users = admins

    return item


@router.post(
    "/collections/{urn}/{role}s",
    response_model=collection.Collection,
    responses={**ACCESS_CONTROL_ERROR_RESPONSES, **BASE_409_RESPONSE},
    summary="Add a user to a collection role",
)
async def add_user_to_collection_role(
    *,
    body: collection.AddUserToCollectionRoleRequest,
    urn: str,
    role: ContributionRole,
    db: Session = Depends(deps.get_db),
    user_data: UserData = Depends(require_current_user_with_email),
) -> Any:
    """
    Add an existing user to a collection under the specified role.
    If a user is already in a role for this collection, this will remove the user from any other roles in this collection.
    """
    save_to_logging_context({"requested_resource": urn})

    item = db.execute(select(Collection).where(Collection.urn == urn)).scalars().one_or_none()
    if not item:
        logger.info(
            msg="Failed to add user to collection role; The requested collection does not exist.",
            extra=logging_context(),
        )
        raise HTTPException(status_code=404, detail=f"collection with URN '{urn}' not found")

    user = db.execute(select(User).where(User.username == body.orcid_id)).scalars().one_or_none()
    if not user:
        logger.info(
            msg="Failed to add user to collection role; The requested user does not exist.",
            extra=logging_context(),
        )
        raise HTTPException(status_code=404, detail=f"user with ORCID iD '{body.orcid_id}' not found")

    # get current user role
    collection_user_association = (
        db.execute(
            select(CollectionUserAssociation)
            .where(CollectionUserAssociation.collection_id == item.id)
            .where(CollectionUserAssociation.user_id == user.id)
        )
        .scalars()
        .one_or_none()
    )

    assert_permission(user_data, item, Action.ADD_ROLE)

    # Since this is a post request, user should not already be in this role
    if collection_user_association and collection_user_association.contribution_role == role:
        logger.info(
            msg="Failed to add user to collection role; the requested user already has the requested role for this collection.",
            extra=logging_context(),
        )
        raise HTTPException(
            status_code=409,
            detail=f"user with ORCID iD '{body.orcid_id}' is already a {role} for collection '{urn}'",
        )
    # A user can only be in one role per collection, so remove from any other roles
    elif collection_user_association:
        item.users.remove(user)

    setattr(user, "role", role)
    item.users.append(user)

    item.modified_by = user_data.user

    db.add(item)
    db.commit()
    db.refresh(item)

    save_to_logging_context({"updated_resource": item.urn})

    # filter score set and experiment associations based on user permissions
    # note that this filtering occurs after saving changes to db; the filtering is only for the returned view model
    # work with associations directly to preserve position ordering
    item.score_set_associations = [
        assoc for assoc in item.score_set_associations if has_permission(user_data, assoc.score_set, Action.READ)
    ]
    item.experiment_associations = [
        assoc for assoc in item.experiment_associations if has_permission(user_data, assoc.experiment, Action.READ)
    ]

    # Only collection admins can get to this point in the function, so here we don't need to filter the list of user
    # roles to show only admins.

    return item


@router.delete(
    "/collections/{urn}/{role}s/{orcid_id}",
    response_model=collection.Collection,
    responses={**ACCESS_CONTROL_ERROR_RESPONSES, **BASE_409_RESPONSE},
    summary="Remove a user from a collection role",
)
async def remove_user_from_collection_role(
    *,
    urn: str,
    role: ContributionRole,
    orcid_id: str,
    db: Session = Depends(deps.get_db),
    user_data: UserData = Depends(require_current_user_with_email),
) -> Any:
    """
    Remove a user from a collection role. Both the user and the role should be provided explicitly and match
    the current assignment.
    """
    save_to_logging_context({"requested_resource": urn})

    item = db.execute(select(Collection).where(Collection.urn == urn)).scalars().one_or_none()
    if not item:
        logger.info(
            msg="Failed to add user to collection role; The requested collection does not exist.",
            extra=logging_context(),
        )
        raise HTTPException(status_code=404, detail=f"collection with URN '{urn}' not found")

    user = db.execute(select(User).where(User.username == orcid_id)).scalars().one_or_none()
    if not user:
        logger.info(
            msg="Failed to add user to collection role; The requested user does not exist.",
            extra=logging_context(),
        )
        raise HTTPException(status_code=404, detail=f"user with ORCID iD '{orcid_id}' not found")

    # get current user role
    collection_user_association = (
        db.execute(
            select(CollectionUserAssociation).where(
                and_(
                    CollectionUserAssociation.collection_id == item.id,
                    CollectionUserAssociation.user_id == user.id,
                )
            )
        )
        .scalars()
        .one_or_none()
    )

    assert_permission(user_data, item, Action.ADD_ROLE)

    # Since this is a post request, user should not already be in this role
    if collection_user_association is not None and collection_user_association.contribution_role != role:
        logger.info(
            msg="Failed to remove user from collection role; the requested user does not currently hold the requested role for this collection.",
            extra=logging_context(),
        )
        raise HTTPException(
            status_code=409,
            detail=f"user with ORCID iD '{orcid_id}' does not currently hold the role {role} for collection '{urn}'",
        )

    item.users.remove(user)
    item.modified_by = user_data.user

    db.add(item)
    db.commit()
    db.refresh(item)

    save_to_logging_context({"updated_resource": item.urn})

    # filter score set and experiment associations based on user permissions
    # note that this filtering occurs after saving changes to db; the filtering is only for the returned view model
    # work with associations directly to preserve position ordering
    item.score_set_associations = [
        assoc for assoc in item.score_set_associations if has_permission(user_data, assoc.score_set, Action.READ)
    ]
    item.experiment_associations = [
        assoc for assoc in item.experiment_associations if has_permission(user_data, assoc.experiment, Action.READ)
    ]

    # Only collection admins can get to this point in the function, so here we don't need to filter the list of user
    # roles to show only admins.

    return item


@router.delete(
    "/collections/{urn}",
    responses={**ACCESS_CONTROL_ERROR_RESPONSES},
    summary="Delete a collection",
)
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

    item = db.execute(select(Collection).where(Collection.urn == urn)).scalars().one_or_none()
    if not item:
        logger.info(
            msg="Failed to delete collection; The requested collection does not exist.",
            extra=logging_context(),
        )
        raise HTTPException(status_code=404, detail=f"collection with URN '{urn}' not found")

    assert_permission(user_data, item, Action.DELETE)

    db.delete(item)
    db.commit()
