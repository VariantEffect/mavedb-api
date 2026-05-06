from contextlib import contextmanager
from typing import Generator, TypedDict, Union
from unittest.mock import AsyncMock, MagicMock, patch

from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session

from tests.helpers.util.common import create_failing_side_effect


class TransactionSpy:
    """Factory for creating database transaction spy context managers."""

    class Spies(TypedDict):
        flush: Union[MagicMock, AsyncMock]
        rollback: Union[MagicMock, AsyncMock]
        commit: Union[MagicMock, AsyncMock]

    class SpiesWithException(Spies):
        exception: Exception

    @staticmethod
    @contextmanager
    def spy(
        session: Session,
        expect_rollback: bool = False,
        expect_flush: bool = False,
        expect_commit: bool = False,
    ) -> Generator[Spies, None, None]:
        """
        Create spies for database transaction methods.

        Args:
            session: Database session to spy on
            expect_rollback: Whether to assert db.rollback to be called
            expect_flush: Whether to assert db.flush to be called
            expect_commit: Whether to assert db.commit to be called

        Yields:
            dict: Dictionary containing all the spies for granular assertion

        Note:
            Use caution when combining expectations. For example, if expect_commit
            is True, you may wish to set expect_flush to True as well, since commit
            typically implies a flush operation within SQLAlchemy internals.

        Example:
        ```
            with TransactionSpy.spy(session, expect_rollback=True) as spies:
                # perform operation
                ...

                # Make manual granular assertions on spies if desired
                spies['rollback'].assert_called_once()

            # if assert_XXX=True is set, automatic assertions will be made at context exit.
            # In this example, expect_rollback=True will ensure rollback was called at some point.
        ```
        """
        with (
            patch.object(session, "rollback", wraps=session.rollback) as rollback_spy,
            patch.object(session, "flush", wraps=session.flush) as flush_spy,
            patch.object(session, "commit", wraps=session.commit) as commit_spy,
        ):
            spies: TransactionSpy.Spies = {
                "flush": flush_spy,
                "rollback": rollback_spy,
                "commit": commit_spy,
            }

            yield spies

            # Automatic assertions based on session expectations.
            if expect_flush:
                flush_spy.assert_called()
            else:
                flush_spy.assert_not_called()
            if expect_rollback:
                rollback_spy.assert_called()
            else:
                rollback_spy.assert_not_called()
            if expect_commit:
                commit_spy.assert_called()
            else:
                commit_spy.assert_not_called()

    @staticmethod
    @contextmanager
    def mock_database_execution_failure(
        session: Session,
        exception=None,
        fail_on_call=1,
        expect_rollback: bool = False,
        expect_flush: bool = False,
        expect_commit: bool = False,
    ) -> Generator[SpiesWithException, None, None]:
        """
        Create a context that mocks database execution failures with transaction spies. This context
        will automatically assert calls to rollback, flush, and commit based on the provided expectations
        which all default to False.

        Args:
            session: Database session to mock
            exception: Exception to raise (defaults to SQLAlchemyError)
            fail_on_call: Which call should fail (defaults to first call)
            expect_rollback: Whether to assert rollback called (defaults to False)
            expect_flush: Whether to assert flush called (defaults to False)
            expect_commit: Whether to assert commit called (defaults to False)
        Yields:
            dict: Dictionary containing spies and the exception that will be raised
        """
        exception = exception or SQLAlchemyError("DB Error")

        with (
            patch.object(
                session,
                "execute",
                side_effect=create_failing_side_effect(exception, session.execute, fail_on_call),
            ),
            TransactionSpy.spy(
                session,
                expect_rollback=expect_rollback,
                expect_flush=expect_flush,
                expect_commit=expect_commit,
            ) as transaction_spies,
        ):
            spies: TransactionSpy.SpiesWithException = {
                **transaction_spies,
                "exception": exception,
            }

            yield spies

    @staticmethod
    @contextmanager
    def mock_database_flush_failure(
        session: Session,
        exception=None,
        fail_on_call=1,
        expect_rollback: bool = True,
        expect_flush: bool = True,
        expect_commit: bool = False,
    ) -> Generator[SpiesWithException, None, None]:
        """
        Create a context that mocks flush failures specifically. This context will automatically
        assert that rollback and flush are called, and that commit is not called. These automatic
        assertions can be overridden via the expect_XXX parameters.

        Args:
            session: Database session to mock
            exception: Exception to raise on flush (defaults to SQLAlchemyError)
            fail_on_call: Which flush call should fail (defaults to first call)
            expect_rollback: Whether to assert rollback called (defaults to True)
            expect_flush: Whether to assert flush called (defaults to True)
            expect_commit: Whether to assert commit called (defaults to False)
        Yields:
            dict: Dictionary containing spies and the exception
        """
        exception = exception or SQLAlchemyError("Flush Error")

        with (
            patch.object(
                session, "flush", side_effect=create_failing_side_effect(exception, session.flush, fail_on_call)
            ),
            TransactionSpy.spy(
                session,
                expect_rollback=expect_rollback,
                expect_flush=expect_flush,
                expect_commit=expect_commit,
            ) as transaction_spies,
        ):
            spies: TransactionSpy.SpiesWithException = {
                **transaction_spies,
                "exception": exception,
            }

            yield spies

    @staticmethod
    @contextmanager
    def mock_database_rollback_failure(
        session: Session,
        exception=None,
        fail_on_call=1,
        expect_rollback: bool = True,
        expect_flush: bool = False,
        expect_commit: bool = False,
    ) -> Generator[SpiesWithException, None, None]:
        """
        Create a context that mocks rollback failures specifically. This context will automatically
        assert that rollback is called, flush is not called, and commit is not called. These automatic
        assertions can be overridden via the expect_XXX parameters.

        Args:
            session: Database session to mock
            exception: Exception to raise on rollback (defaults to SQLAlchemyError)
            fail_on_call: Which rollback call should fail (defaults to first call)
            expect_rollback: Whether to assert rollback called (defaults to True)
            expect_flush: Whether to assert flush called (defaults to False)
            expect_commit: Whether to assert commit called (defaults to False)
        Yields:
            dict: Dictionary containing spies and the exception
        """
        exception = exception or SQLAlchemyError("Rollback Error")

        with (
            patch.object(
                session, "rollback", side_effect=create_failing_side_effect(exception, session.rollback, fail_on_call)
            ),
            TransactionSpy.spy(
                session,
                expect_rollback=expect_rollback,
                expect_flush=expect_flush,
                expect_commit=expect_commit,
            ) as transaction_spies,
        ):
            spies: TransactionSpy.SpiesWithException = {
                **transaction_spies,
                "exception": exception,
            }

            yield spies
