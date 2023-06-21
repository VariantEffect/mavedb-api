from tempfile import TemporaryDirectory
from pathlib import Path
from fastapi.testclient import TestClient
import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from mavedb.server_main import app
from mavedb.db.base import Base
from mavedb.deps import get_db
from mavedb.lib.authorization import require_current_user
from mavedb.models.user import User

TEST_USER = {
    "username": "0000-1111-2222-3333",
    "first_name": "First",
    "last_name": "Last",
}


def override_get_db():
    try:
        db = TestingSessionLocal()
        yield db
    finally:
        db.close()


def override_current_user():
    yield User(
        is_active=True,
        is_staff=False,
        is_superuser=False,
        **TEST_USER,
    )


@pytest.fixture()
def test_empty_db():
    Base.metadata.create_all(bind=engine)
    yield
    Base.metadata.drop_all(bind=engine)


# create the test database
db_directory = TemporaryDirectory()
engine = create_engine(f"sqlite:///{Path(db_directory.name, 'test.db')}", connect_args={"check_same_thread": False})
TestingSessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# set up the test environment by overriding the db and user behavior
app.dependency_overrides[get_db] = override_get_db
app.dependency_overrides[require_current_user] = override_current_user
client = TestClient(app)
