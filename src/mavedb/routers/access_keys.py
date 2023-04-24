import secrets
from typing import Any

from cryptography.hazmat.backends import default_backend as crypto_default_backend
from cryptography.hazmat.primitives import serialization as crypto_serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from fastapi import APIRouter, Depends
from fastapi.encoders import jsonable_encoder
from sqlalchemy.orm import Session

from mavedb import deps
from mavedb.lib.authorization import require_current_user
from mavedb.models.access_key import AccessKey
from mavedb.models.user import User
from mavedb.view_models import access_key

router = APIRouter(prefix="/api/v1", tags=["access keys"], responses={404: {"description": "Not found"}})


def generate_key_pair():
    key = rsa.generate_private_key(backend=crypto_default_backend(), public_exponent=65537, key_size=2048)

    private_key = key.private_bytes(
        crypto_serialization.Encoding.PEM, crypto_serialization.PrivateFormat.PKCS8, crypto_serialization.NoEncryption()
    )

    public_key = key.public_key().public_bytes(
        crypto_serialization.Encoding.OpenSSH, crypto_serialization.PublicFormat.OpenSSH
    )

    return private_key, public_key


@router.get(
    "/users/me/access-keys", status_code=200, response_model=list[access_key.AccessKey], responses={404: {}, 500: {}}
)
def list_my_access_keys(*, user: User = Depends(require_current_user)) -> Any:
    """
    List the current user's access keys.
    """
    return user.access_keys


@router.post(
    "/users/me/access-keys", status_code=200, response_model=access_key.NewAccessKey, responses={404: {}, 500: {}}
)
def create_my_access_key(*, db: Session = Depends(deps.get_db), user: User = Depends(require_current_user)) -> Any:
    """
    Create a new access key for the current user.
    """
    private_key, public_key = generate_key_pair()

    item = AccessKey(user=user, key_id=secrets.token_urlsafe(32), public_key=public_key)
    db.add(item)
    db.commit()
    db.refresh(item)

    response_item = access_key.NewAccessKey(**jsonable_encoder(item, by_alias=False), private_key=private_key)
    response_item.private_key = private_key
    return response_item


@router.delete("/users/me/access-keys/{key_id}", status_code=200, responses={404: {}, 500: {}})
def delete_my_access_key(
    *, key_id: str, db: Session = Depends(deps.get_db), user: User = Depends(require_current_user)
) -> Any:
    """
    Delete one of the current user's access keys.
    """
    item = db.query(AccessKey).filter(AccessKey.key_id == key_id).one_or_none()
    if item.user.id == user.id:
        db.delete(item)
    db.commit()
