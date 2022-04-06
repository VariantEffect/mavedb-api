from fastapi.testclient import TestClient
import os
import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.db.base import Base
from app.deps import get_db
from main import app


# To store the test database temporarily on disk:
SQLITE_DB_FILE = './test.db'

# To store the test database in memory:
# SQLITE_DB_FILE = None

SQLALCHEMY_DATABASE_URL = 'sqlite://' if SQLITE_DB_FILE is None else f'sqlite:///{SQLITE_DB_FILE}'

engine = create_engine(
    SQLALCHEMY_DATABASE_URL, connect_args={"check_same_thread": False}
)
TestingSessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


def override_get_db():
    try:
        db = TestingSessionLocal()
        yield db
    finally:
        db.close()


app.dependency_overrides[get_db] = override_get_db
client = TestClient(app)


@pytest.fixture()
def test_empty_db():
    Base.metadata.create_all(bind=engine)
    yield
    Base.metadata.drop_all(bind=engine)
    if SQLITE_DB_FILE is not None:
        os.remove(SQLITE_DB_FILE)


@pytest.fixture()
def test_db_with_dataset():
    Base.metadata.create_all(bind=engine)
    yield
    Base.metadata.drop_all(bind=engine)
