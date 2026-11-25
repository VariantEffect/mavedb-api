# ruff: noqa: E402

import time

import pytest

pytest.importorskip("psycopg2")

from sqlalchemy import text
from sqlalchemy.exc import DataError, OperationalError, ProgrammingError

from mavedb.db.errors import ConnectionDropped, DatabaseBusy, MissingGrant, RowSecurityViolation


@pytest.mark.unit
class TestTranslateDatabaseErrors:
    @pytest.mark.parametrize(
        "form,translated",
        [
            ("missing_grant", MissingGrant),
            ("missing_sequence_grant", MissingGrant),
            ("row_security", RowSecurityViolation),
            ("named_policy", RowSecurityViolation),
            ("on_conflict", RowSecurityViolation),
        ],
        ids=["missing_grant", "missing_sequence_grant", "row_security", "named_policy", "on_conflict"],
    )
    def test_insufficient_privilege_raises_its_typed_exception(self, raise_insufficient_privilege, form, translated):
        with pytest.raises(translated) as excinfo:
            raise_insufficient_privilege(form)

        assert isinstance(excinfo.value, ProgrammingError)
        assert excinfo.value.orig.pgcode == "42501"

    def test_statement_timeout_raises_database_busy(self, session):
        session.execute(text("SET LOCAL statement_timeout = '10ms'"))

        with pytest.raises(DatabaseBusy) as excinfo:
            session.execute(text("SELECT pg_sleep(1)"))

        assert isinstance(excinfo.value, OperationalError)
        assert excinfo.value.orig.pgcode == "57014"

    def test_connection_the_server_closed_raises_connection_dropped(self, session):
        session.execute(text("SET idle_in_transaction_session_timeout = '50ms'"))
        session.execute(text("SELECT 1"))
        time.sleep(0.5)

        with pytest.raises(ConnectionDropped) as excinfo:
            session.execute(text("SELECT 1"))

        assert isinstance(excinfo.value, OperationalError)
        assert excinfo.value.connection_invalidated

    def test_other_database_error_is_unchanged(self, session):
        with pytest.raises(DataError) as excinfo:
            session.execute(text("SELECT 1 / 0"))

        assert type(excinfo.value) is DataError
