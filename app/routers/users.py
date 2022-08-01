from pprint import pprint
from typing import Any

from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session

from app import deps
from app.lib.auth import require_current_user
from app.models.user import User
from app.view_models import user

router = APIRouter(
    prefix='/api/v1',
    tags=['access keys'],
    responses={404: {'description': 'Not found'}}
)


@router.get('/users/me', status_code=200, response_model=user.User, responses={404: {}, 500: {}})
def show_me(
    *,
    user: User = Depends(require_current_user)
) -> Any:
    """
    Return the current user.
    """
    return user


@router.put('/users/me', status_code=200, response_model=user.User, responses={404: {}, 500: {}})
def update_me(
    *,
    user_update: user.UserUpdate,
    db: Session = Depends(deps.get_db),
    user: User = Depends(require_current_user)
) -> Any:
    """
    Return the current user.
    """
    user.email = user_update.email
    db.add(user)
    db.commit()
    db.refresh(user)
    return user
