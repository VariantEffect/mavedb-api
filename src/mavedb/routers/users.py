import logging
from typing import Any

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from starlette.convertors import Convertor, register_url_convertor

from mavedb import deps
from mavedb.lib.authentication import UserData
from mavedb.lib.authorization import RoleRequirer, require_current_user
from mavedb.lib.logging import LoggedRoute
from mavedb.lib.logging.context import logging_context, save_to_logging_context
from mavedb.lib.permissions import Action, assert_permission
from mavedb.models.enums.user_role import UserRole
from mavedb.models.user import User
from mavedb.view_models import user

router = APIRouter(
    prefix="/api/v1",
    tags=["access keys"],
    responses={404: {"description": "Not found"}},
    route_class=LoggedRoute,
)

logger = logging.getLogger(__name__)


# Define custom type convertor (see https://www.starlette.io/routing/#path-parameters)
# in order to recognize user lookup id as an int or orcid id, and call the appropriate function
class OrcidIdConverter(Convertor):
    regex = "\d{4}-\d{4}-\d{4}-(\d{4}|\d{3}X)"

    def convert(self, value: str) -> str:
        return value

    def to_string(self, value: str) -> str:
        return str(value)


register_url_convertor("orcid_id", OrcidIdConverter())


# Trailing slash is deliberate
@router.get("/users/", status_code=200, response_model=list[user.AdminUser], responses={404: {}})
async def list_users(
    *,
    db: Session = Depends(deps.get_db),
    user_data: UserData = Depends(RoleRequirer([UserRole.admin])),
) -> Any:
    """
    List users.
    """
    items = db.query(User).order_by(User.username).all()
    return items


@router.get(
    "/users/me",
    status_code=200,
    response_model=user.CurrentUser,
    responses={404: {}, 500: {}},
)
async def show_me(*, user_data: UserData = Depends(require_current_user)) -> Any:
    """
    Return the current user.
    """
    return user_data.user


@router.get(
    "/users/{id:int}",
    status_code=200,
    response_model=user.AdminUser,
    responses={404: {}, 500: {}},
)
async def show_user_admin(
    *,
    id: int,
    user_data: UserData = Depends(RoleRequirer([UserRole.admin])),
    db: Session = Depends(deps.get_db),
) -> Any:
    """
    Fetch a single user by ID. Returns admin view of requested user.
    """
    save_to_logging_context({"requested_user": id})
    item = db.query(User).filter(User.id == id).one_or_none()
    if not item:
        logger.warning(
            msg="Could not show user; Requested user does not exist.",
            extra=logging_context(),
        )
        raise HTTPException(status_code=404, detail=f"User with ID {id} not found")

    # moving toward always accessing permissions module, even though this function does already require admin role to access
    assert_permission(user_data, item, Action.READ)
    return item


@router.get(
    "/users/{orcid_id:orcid_id}",
    status_code=200,
    response_model=user.User,
    responses={404: {}, 500: {}},
)
async def show_user(
    *,
    orcid_id: str,
    user_data: UserData = Depends(require_current_user),
    db: Session = Depends(deps.get_db),
) -> Any:
    """
    Fetch a single user by Orcid ID. Returns limited view of user.
    """
    save_to_logging_context({"requested_user": orcid_id})

    item = db.query(User).filter(User.username == orcid_id).one_or_none()
    if not item:
        logger.warning(
            msg="Could not show user; Requested user does not exist.",
            extra=logging_context(),
        )
        raise HTTPException(status_code=404, detail=f"User with ID {orcid_id} not found")

    # moving toward always accessing permissions module, even though this function does already require existing user in order to access
    assert_permission(user_data, item, Action.LOOKUP)
    return item


@router.put(
    "/users/me",
    status_code=200,
    response_model=user.CurrentUser,
    responses={404: {}, 500: {}},
)
async def update_me(
    *,
    user_update: user.CurrentUserUpdate,
    db: Session = Depends(deps.get_db),
    user_data: UserData = Depends(require_current_user),
) -> Any:
    """
    Update the current user.
    """
    current_user = user_data.user
    assert_permission(user_data, current_user, Action.UPDATE)
    current_user.email = user_update.email
    current_user.is_first_login = False
    db.add(current_user)
    db.commit()
    db.refresh(current_user)
    return current_user


@router.put(
    "/users/me/has-logged-in",
    status_code=200,
    response_model=user.CurrentUser,
    responses={404: {}, 500: {}},
)
async def user_has_logged_in(
    *,
    db: Session = Depends(deps.get_db),
    user_data: UserData = Depends(require_current_user),
) -> Any:
    """
    Update the current users log in state.
    """
    current_user = user_data.user
    assert_permission(user_data, current_user, Action.UPDATE)
    current_user.is_first_login = False
    db.add(current_user)
    db.commit()
    db.refresh(current_user)
    return current_user


# Double slash is deliberate.
@router.put(
    "/users//{id}",
    status_code=200,
    response_model=user.AdminUser,
    responses={404: {}, 500: {}},
)
async def update_user(
    *,
    id: int,
    item_update: user.AdminUserUpdate,
    db: Session = Depends(deps.get_db),
    user_data: UserData = Depends(require_current_user),
) -> Any:
    """
    Update a user.
    """
    save_to_logging_context({"requested_user": id})
    item = db.query(User).filter(User.id == id).one_or_none()
    if not item:
        logger.warning(
            msg="Could not update user; Requested user does not exist.",
            extra=logging_context(),
        )
        raise HTTPException(status_code=404, detail=f"User with id {id} not found.")

    assert_permission(user_data, item, Action.UPDATE)
    assert_permission(user_data, item, Action.ADD_ROLE)

    if item_update.first_name:
        item.first_name = item_update.first_name
    if item_update.last_name:
        item.last_name = item_update.last_name
    if item_update.email:
        item.email = item_update.email
    if item_update.roles:
        await item.set_roles(db, item_update.roles)

    db.add(item)
    db.commit()
    db.refresh(item)
    return item
