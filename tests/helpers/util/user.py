from typing import Any

from sqlalchemy.orm import Session

from mavedb.models.user import User

from tests.helpers.constants import EXTRA_USER


def mark_user_inactive(session: Session, username: str) -> User:
    user = session.query(User).where(User.username == username).one()
    user.is_active = False

    session.add(user)
    session.commit()
    session.refresh(user)

    return user


def change_ownership(db: Session, urn: str, model: Any) -> None:
    """Change the ownership of the record with given urn and model to the extra user."""
    item = db.query(model).filter(model.urn == urn).one_or_none()
    assert item is not None
    extra_user = db.query(User).filter(User.username == EXTRA_USER["username"]).one_or_none()
    assert extra_user is not None
    item.created_by_id = extra_user.id
    item.modified_by_id = extra_user.id
    db.add(item)
    db.commit()
