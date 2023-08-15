from tempfile import TemporaryDirectory
from pathlib import Path
from fastapi.testclient import TestClient
import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from mavedb.server_main import app
from mavedb.db.base import Base
from mavedb.deps import get_db
from mavedb.lib.authentication import get_current_user
from mavedb.models.user import User
from tests.helpers.constants import TEST_USER


@pytest.fixture()
def session():
    Base.metadata.drop_all(bind=engine)
    Base.metadata.create_all(bind=engine)

    db = TestingSessionLocal()

    try:
        yield db
    finally:
        db.close()


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


# create the test database
db_directory = TemporaryDirectory()
engine = create_engine(f"sqlite:///{Path(db_directory.name, 'test.db')}", connect_args={"check_same_thread": False})
TestingSessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
