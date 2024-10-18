import logging
import secrets
from typing import Any

from cryptography.hazmat.backends import default_backend as crypto_default_backend
from cryptography.hazmat.primitives import serialization as crypto_serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from fastapi import APIRouter, Depends
from fastapi.encoders import jsonable_encoder
from fastapi.exceptions import HTTPException
from sqlalchemy.orm import Session

from mavedb import deps
from mavedb.lib.authentication import UserData
from mavedb.lib.authorization import require_current_user
from mavedb.lib.logging import LoggedRoute
from mavedb.lib.logging.context import logging_context, save_to_logging_context
from mavedb.models.access_key import AccessKey
from mavedb.models.enums.user_role import UserRole
from mavedb.view_models import access_key

router = APIRouter(
    prefix="/api/v1",
    tags=["access keys"],
    responses={404: {"description": "Not found"}},
    route_class=LoggedRoute,
)

logger = logging.getLogger(__name__)


def generate_key_pair():
    key = rsa.generate_private_key(backend=crypto_default_backend(), public_exponent=65537, key_size=2048)

    private_key = key.private_bytes(
        crypto_serialization.Encoding.PEM,
        crypto_serialization.PrivateFormat.PKCS8,
        crypto_serialization.NoEncryption(),
    )

    public_key = key.public_key().public_bytes(
        crypto_serialization.Encoding.OpenSSH, crypto_serialization.PublicFormat.OpenSSH
    )

    return private_key, public_key


@router.get(
    "/users/me/access-keys",
    status_code=200,
    response_model=list[access_key.AccessKey],
    responses={404: {}, 500: {}},
)
def list_my_access_keys(*, user_data: UserData = Depends(require_current_user)) -> Any:
    """
    List the current user's access keys.
    """
    return user_data.user.access_keys


@router.post(
    "/users/me/access-keys",
    status_code=200,
    response_model=access_key.NewAccessKey,
    responses={404: {}, 500: {}},
)
def create_my_access_key(
    *,
    db: Session = Depends(deps.get_db),
    user_data: UserData = Depends(require_current_user),
) -> Any:
    """
    Create a new access key for the current user, with the default user role.
    """
    private_key, public_key = generate_key_pair()

    item = AccessKey(user=user_data.user, key_id=secrets.token_urlsafe(32), public_key=public_key)
    db.add(item)
    db.commit()
    db.refresh(item)

    response_item = access_key.NewAccessKey(**jsonable_encoder(item, by_alias=False), private_key=private_key)
    response_item.private_key = private_key
    return response_item


@router.post(
    "/users/me/access-keys/{role}",
    status_code=200,
    response_model=access_key.NewAccessKey,
    responses={404: {}, 500: {}},
)
async def create_my_access_key_with_role(
    *,
    role: UserRole,
    db: Session = Depends(deps.get_db),
    user_data: UserData = Depends(require_current_user),
) -> Any:
    """
    Create a new access key for the current user, with the specified role.
    """
    save_to_logging_context({"requested_role": role.name})
    # Allow the user to create an access key for any of their potential roles, not just their active one.
    if not any(user_role == role for user_role in user_data.user.roles):
        logger.warning(
            msg="Could not create API key for user; User does not belong to the requested role.",
            extra=logging_context(),
        )

        raise HTTPException(
            status_code=403,
            detail="User cannot create an API key for a role they do not have.",
        )

    private_key, public_key = generate_key_pair()

    item = AccessKey(user=user_data.user, key_id=secrets.token_urlsafe(32), public_key=public_key)
    await item.set_role(db, role)
    db.add(item)
    db.commit()
    db.refresh(item)

    response_item = access_key.NewAccessKey(**jsonable_encoder(item, by_alias=False), private_key=private_key)
    response_item.private_key = private_key
    return response_item


@router.delete("/users/me/access-keys/{key_id}", status_code=200, responses={404: {}, 500: {}})
def delete_my_access_key(
    *,
    key_id: str,
    db: Session = Depends(deps.get_db),
    user_data: UserData = Depends(require_current_user),
) -> Any:
    """
    Delete one of the current user's access keys.
    """
    item = db.query(AccessKey).filter(AccessKey.key_id == key_id).one_or_none()
    if item and item.user.id == user_data.user.id:
        db.delete(item)
        db.commit()
        logger.debug(msg="Successfully deleted provided API key.", extra=logging_context())
