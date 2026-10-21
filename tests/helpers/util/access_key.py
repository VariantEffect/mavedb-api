from fastapi.testclient import TestClient
from sqlalchemy import select
from sqlalchemy.orm import Session

from mavedb.models.user import User
from mavedb.routers.access_keys import new_access_key


def create_api_key_for_user(db: Session, username: str) -> str:
    user = db.scalars(select(User).where(User.username == username)).one()
    key, _, item = new_access_key(user)
    db.add(item)
    db.commit()

    return key


def create_api_key_for_current_user(client: TestClient) -> dict:
    response = client.post("api/v1/users/me/access-keys")
    assert response.status_code == 200
    return response.json()


def create_admin_key_for_current_user(client: TestClient) -> dict:
    response = client.post("api/v1/users/me/access-keys/admin")
    assert response.status_code == 200
    return response.json()
