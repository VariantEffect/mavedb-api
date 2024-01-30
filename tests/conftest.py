from fastapi.testclient import TestClient
import pytest
import pytest_postgresql
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import NullPool
from mavedb.server_main import app
from mavedb.db.base import Base
from mavedb.deps import get_db
from mavedb.lib.authentication import get_current_user
from mavedb.models.user import User

import sys
sys.path.append('.')

from tests.helpers.constants import TEST_USER

# needs the pytest_postgresql plugin installed
assert pytest_postgresql.factories


@pytest.fixture()
def session(postgresql):
    connection = f'postgresql+psycopg2://{postgresql.info.user}:' \
        f'@{postgresql.info.host}:{postgresql.info.port}/{postgresql.info.dbname}'

    engine = create_engine(connection, echo=False, poolclass=NullPool)
    session = sessionmaker(autocommit=False, autoflush=False, bind=engine)

    Base.metadata.create_all(bind=engine)

    try:
        yield session()
    finally:
        Base.metadata.drop_all(bind=engine)


@pytest.fixture()
def client(session):
    def override_get_db():
        try:
            yield session
        finally:
            session.close()

    def override_current_user():
        default_user = session.query(User).filter(User.username == TEST_USER["username"]).one_or_none()
        yield default_user

    app.dependency_overrides[get_db] = override_get_db
    app.dependency_overrides[get_current_user] = override_current_user

    yield TestClient(app)
