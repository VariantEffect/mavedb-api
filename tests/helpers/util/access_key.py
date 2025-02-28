import secrets

from sqlalchemy import select
from sqlalchemy.orm import Session
from fastapi.testclient import TestClient

from mavedb.models.access_key import AccessKey
from mavedb.models.user import User
from mavedb.models.enums.user_role import UserRole

from mavedb.routers.access_keys import generate_key_pair


def create_api_key_for_user(db: Session, username: str) -> str:
    user = db.scalars(select(User).where(User.username == username)).one()
    private_key, public_key = generate_key_pair()

    item = AccessKey(user=user, key_id=secrets.token_urlsafe(32), public_key=public_key)
    db.add(item)
    db.commit()
    db.refresh(item)

    return item.key_id


def create_admin_key_for_user(db: Session, username: str) -> str:
    user = db.scalars(select(User).where(User.username == username)).one()
    private_key, public_key = generate_key_pair()

    item = AccessKey(user=user, key_id=secrets.token_urlsafe(32), public_key=public_key, role=UserRole.admin)
    db.add(item)
    db.commit()
    db.refresh(item)

    return item.public_key


def create_api_key_for_current_user(client: TestClient) -> str:
    response = client.post("api/v1/users/me/access-keys")
    assert response.status_code == 200
    return response.json()["keyId"]


def create_admin_key_for_current_user(client: TestClient) -> str:
    response = client.post("api/v1/users/me/access-keys/admin")
    assert response.status_code == 200
    return response.json()["keyId"]
