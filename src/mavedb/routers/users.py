from typing import Any

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.exc import MultipleResultsFound
from sqlalchemy.orm import Session

from mavedb import deps
from mavedb.lib.authorization import require_current_user, RoleRequirer
from mavedb.models.user import User
from mavedb.view_models import user

router = APIRouter(prefix="/api/v1", tags=["access keys"], responses={404: {"description": "Not found"}})


@router.get("/users/", status_code=200, response_model=list[user.AdminUser], responses={404: {}})
async def list_users(*, db: Session = Depends(deps.get_db), user: User = Depends(RoleRequirer(["admin"]))) -> Any:
    """
    List users.
    """
    items = db.query(User).order_by(User.username).all()
    return items


@router.get("/users/me", status_code=200, response_model=user.CurrentUser, responses={404: {}, 500: {}})
async def show_me(*, user: User = Depends(require_current_user)) -> Any:
    """
    Return the current user.
    """
    return user


@router.get("/users/{id}", status_code=200, response_model=user.AdminUser, responses={404: {}, 500: {}})
async def show_user(
    *, id: int, user: User = Depends(RoleRequirer(["admin"])), db: Session = Depends(deps.get_db)
) -> Any:
    """
    Fetch a single user by ID.
    """
    try:
        item = db.query(User).filter(User.id == id).one_or_none()
    except MultipleResultsFound:
        raise HTTPException(status_code=500, detail=f"Multiple users with ID {id} were found.")
    if not item:
        raise HTTPException(status_code=404, detail=f"User with ID {id} not found")
    return item


@router.put("/users/me", status_code=200, response_model=user.CurrentUser, responses={404: {}, 500: {}})
async def update_me(
    *, user_update: user.CurrentUserUpdate, db: Session = Depends(deps.get_db), user: User = Depends(require_current_user)
) -> Any:
    """
    Update the current user.
    """
    user.email = user_update.email
    db.add(user)
    db.commit()
    db.refresh(user)
    return user


@router.put("/users//{id}", status_code=200, response_model=user.AdminUser, responses={404: {}, 500: {}})
async def update_user(
    *,
    id: int,
    item_update: user.AdminUserUpdate,
    db: Session = Depends(deps.get_db),
    user: User = Depends(require_current_user),
) -> Any:
    """
    Update a user.
    """
    if not item_update:
        raise HTTPException(status_code=400, detail="The request contained no updated item.")

    try:
        item = db.query(User).filter(User.id == id).one_or_none()
    except MultipleResultsFound:
        raise HTTPException(status_code=500, detail=f"Multiple users with ID {id} were found.")
    if not item:
        raise HTTPException(status_code=404, detail=f"User with id {id} not found.")
    # TODO Ensure that the current user has edit rights for this score set.

    item.first_name = item_update.first_name
    item.last_name = item_update.last_name
    item.email = item_update.email
    await item.set_roles(db, item_update.roles)
    db.add(item)
    db.commit()
    db.refresh(item)
    return item
