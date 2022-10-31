import os
from typing import Generator

from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.types import JSON

from app.db.session import SessionLocal


def get_db() -> Generator:
    db = SessionLocal()
    db.current_user_id = None
    try:
        yield db
    finally:
        db.close()


if 'PYTEST_RUN_CONFIG' in os.environ:
    JSONB = JSON
else:
    JSONB = JSONB
