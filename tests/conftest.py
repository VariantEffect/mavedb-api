import logging  # noqa: F401
import sys

import email_validator
import pytest
import pytest_postgresql
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import NullPool

from mavedb.db.base import Base

sys.path.append(".")

# Attempt to import optional top level fixtures. If the modules they depend on are not installed,
# we won't have access to our full fixture suite and only a limited subset of tests can be run.
try:
    from tests.conftest_optional import *  # noqa: F401, F403

except ModuleNotFoundError:
    pass

# needs the pytest_postgresql plugin installed
assert pytest_postgresql.factories

# Allow the @test domain name through our email validator.
email_validator.SPECIAL_USE_DOMAIN_NAMES.remove("test")


@pytest.fixture()
def session(postgresql):
    # Un-comment this line to log all database queries:
    # logging.getLogger("sqlalchemy.engine").setLevel(logging.INFO)

    connection = (
        f"postgresql+psycopg2://{postgresql.info.user}:"
        f"@{postgresql.info.host}:{postgresql.info.port}/{postgresql.info.dbname}"
    )

    engine = create_engine(connection, echo=False, poolclass=NullPool)
    session = sessionmaker(autocommit=False, autoflush=False, bind=engine)()

    Base.metadata.create_all(bind=engine)

    try:
        yield session
    finally:
        session.close()
        Base.metadata.drop_all(bind=engine)
