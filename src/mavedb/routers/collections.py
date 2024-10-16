import logging
from typing import Any

from fastapi import APIRouter, Depends, HTTPException
from fastapi.encoders import jsonable_encoder
from sqlalchemy import select
from sqlalchemy.orm import Session
from sqlalchemy.orm.exc import MultipleResultsFound, NoResultFound

from mavedb import deps
from mavedb.lib.authentication import UserData
from mavedb.lib.authorization import require_current_user_with_email
from mavedb.lib.logging import LoggedRoute
from mavedb.lib.logging.context import format_raised_exception_info_as_dict, logging_context, save_to_logging_context
from mavedb.models.collection import Collection
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
