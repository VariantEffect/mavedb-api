# import os
import sys
from typing import Generator

from sqlalchemy.dialects.postgresql import JSONB as POSTGRES_JSONB
from sqlalchemy.types import JSON

from mavedb.db.session import SessionLocal


def get_db() -> Generator:
    db = SessionLocal()
    db.current_user_id = None
    try:
        yield db
    finally:
        db.close()


# if 'PYTEST_RUN_CONFIG' in os.environ:
if "pytest" in sys.modules:
    JSONB = JSON
else:
    JSONB = POSTGRES_JSONB
