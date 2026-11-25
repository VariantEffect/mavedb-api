# ruff: noqa: E402

import importlib
import inspect
import json
import pkgutil
from typing import Callable
from unittest.mock import patch

import pytest

pytest.importorskip("psycopg2")
pytest.importorskip("fastapi")

from fastapi.exceptions import RequestValidationError, WebSocketRequestValidationError
from fastapi.testclient import TestClient
from sqlalchemy.exc import OperationalError
from sqlalchemy.exc import TimeoutError as PoolTimeoutError
from sqlalchemy.orm.exc import StaleDataError
from starlette.exceptions import HTTPException as StarletteHTTPException

import mavedb.db.errors
import mavedb.lib.exception_handlers
import mavedb.lib.exceptions
import mavedb.lib.permissions.exceptions
from mavedb.db.errors import DatabaseBusy, MissingGrant, RowSecurityViolation
from mavedb.lib.exception_handlers import (
    ERROR_RESPONSES,
    ErrorResponse,
    error_response_for,
    handle_mapped_exception,
    handle_uncaught_exception,
    merge_error_responses,
)
from mavedb.lib.exceptions import (
    AmbiguousIdentifierError,
    HGNCGeneNotFoundError,
    HGNCServiceError,
    MixedTargetError,
    MondoServiceError,
    NonexistentIdentifierError,
    PublicationServiceError,
    PublicationServiceTimeoutError,
)
from mavedb.lib.permissions.exceptions import PermissionException
from mavedb.server_main import app

# Unlike any real route, so registering it can't shadow one.
BOOM_PATH = "/__test_mapped_boom__"
"""The path of the test route that raises an exception."""

# CORS only adds its headers to a request that carries an Origin.
TEST_ORIGIN = "https://www.mavedb.org"
"""The Origin header the tests send, to check the response passed through CORS."""

EXAMPLES: dict[type[Exception], Callable[[], Exception]] = {
    PermissionException: lambda: PermissionException(403, "Forbidden."),
    RequestValidationError: lambda: RequestValidationError([]),
    AmbiguousIdentifierError: lambda: AmbiguousIdentifierError("ambiguous"),
    NonexistentIdentifierError: lambda: NonexistentIdentifierError("missing"),
    MixedTargetError: lambda: MixedTargetError("mixed"),
    HGNCGeneNotFoundError: lambda: HGNCGeneNotFoundError("no gene"),
    HGNCServiceError: lambda: HGNCServiceError("hgnc down"),
    MondoServiceError: lambda: MondoServiceError("mondo down"),
    PublicationServiceError: lambda: PublicationServiceError("pubmed down"),
    PublicationServiceTimeoutError: lambda: PublicationServiceTimeoutError("pubmed slow"),
    RowSecurityViolation: lambda: RowSecurityViolation("INSERT", {}, Exception("policy")),
    MissingGrant: lambda: MissingGrant("SELECT", {}, Exception("grant")),
    StaleDataError: lambda: StaleDataError("matched 0 rows"),
    DatabaseBusy: lambda: DatabaseBusy("SELECT", {}, Exception("timeout")),
    PoolTimeoutError: lambda: PoolTimeoutError("QueuePool limit reached"),
}
"""An instance of each ``ERROR_RESPONSES`` key, for raising it from a test route."""

# Listed rather than discovered, because a subclass walk only finds classes whose modules are already imported.
EXCEPTION_MODULES = [mavedb.lib.exceptions, mavedb.lib.permissions.exceptions, mavedb.db.errors]
"""The modules whose exceptions must each be mapped in ``ERROR_RESPONSES`` or listed in ``UNMAPPED``."""

# Each is caught before it reaches the app or raised only in the worker.
UNMAPPED = {
    mavedb.lib.exceptions.CalibrationAlreadyReplacedError,
    mavedb.lib.exceptions.EmptyTaxonomySearchError,
    mavedb.lib.exceptions.LDHSubmissionFailureError,
    mavedb.lib.exceptions.LinkingEnqueueError,
    mavedb.lib.exceptions.MappingEnqueueError,
    mavedb.lib.exceptions.NCBITaxonomyNotFoundError,
    mavedb.lib.exceptions.NoMappedVariantsError,
    mavedb.lib.exceptions.NonexistentMappingReferenceError,
    mavedb.lib.exceptions.NonexistentMappingResultsError,
    mavedb.lib.exceptions.NonexistentMappingScoresError,
    mavedb.lib.exceptions.NonexistentOrcidUserError,
    mavedb.lib.exceptions.OpenIDConnectException,
    mavedb.lib.exceptions.PipelineNotFoundError,
    mavedb.lib.exceptions.SubmissionEnqueueError,
    mavedb.lib.exceptions.UniProtIDMappingEnqueueError,
    mavedb.lib.exceptions.UniProtPollingEnqueueError,
    mavedb.lib.exceptions.ValidationError,
}
"""Exceptions from ``EXCEPTION_MODULES`` deliberately left to the 500 path."""

FRAMEWORK_HANDLED = {StarletteHTTPException, WebSocketRequestValidationError}
"""Types FastAPI registers its own handlers for."""


@pytest.fixture
def raising_route():
    """Register a route that raises whichever exception the test sets on ``raised["error"]``."""
    raised: dict[str, Callable[[], None]] = {}

    @app.post(BOOM_PATH + "/{form}")
    def boom(form: str):
        raised["error"]()

    route = app.router.routes[-1]
    try:
        yield raised
    finally:
        app.router.routes.remove(route)


@pytest.fixture
def slack_messages():
    """Capture the text of each Slack message at the webhook boundary, after the real payload is built."""
    with patch("mavedb.lib.slack.send_slack_message") as mocked:
        yield lambda: [call.args[0] for call in mocked.call_args_list]


def _raise(exc: Exception) -> Callable[[], None]:
    def raise_it() -> None:
        raise exc

    return raise_it


@pytest.mark.unit
class TestMappedExceptions:
    @pytest.mark.parametrize("exc_type", list(ERROR_RESPONSES), ids=lambda t: t.__name__)
    def test_mapped_exception_returns_its_status_and_alerts_as_its_entry_says(
        self, raising_route, slack_messages, exc_type
    ):
        exc = EXAMPLES[exc_type]()
        raising_route["error"] = _raise(exc)

        with TestClient(app) as tc:
            response = tc.post(f"{BOOM_PATH}/example", headers={"Origin": TEST_ORIGIN})

        entry = ERROR_RESPONSES[exc_type]
        assert response.status_code == entry.status_for(exc)
        assert response.headers.get("access-control-allow-origin") in ("*", TEST_ORIGIN)
        assert len(slack_messages()) == (1 if entry.alert else 0)

    def test_most_specific_class_wins(self):
        assert error_response_for(PublicationServiceTimeoutError("slow")).status_for(Exception()) == 504
        assert error_response_for(PublicationServiceError("down")).status_for(Exception()) == 502

    @pytest.mark.parametrize(
        "form,status_code",
        [("row_security", 403), ("named_policy", 403), ("on_conflict", 403), ("missing_grant", 500)],
        ids=["row_security", "named_policy", "on_conflict", "missing_grant"],
    )
    def test_insufficient_privilege_from_the_database_returns_status_and_attributable_alert(
        self, raising_route, raise_insufficient_privilege, slack_messages, form, status_code
    ):
        raising_route["error"] = lambda: raise_insufficient_privilege(form)

        with TestClient(app) as tc:
            response = tc.post(f"{BOOM_PATH}/{form}", headers={"Origin": TEST_ORIGIN})

        assert response.status_code == status_code
        correlation_id = response.json()["correlation_id"]
        assert correlation_id

        (message,) = slack_messages()
        payload = json.loads(message)
        assert payload["correlation_id"] == correlation_id
        assert payload["route"] == f"POST {BOOM_PATH}/{{form}}"

    @pytest.mark.parametrize(
        "exc_type,status_code,alerted",
        [(StaleDataError, 404, True), (DatabaseBusy, 503, False), (PoolTimeoutError, 503, False)],
        ids=["stale_row", "database_busy", "pool_timeout"],
    )
    def test_database_error_returns_attributable_body_and_alerts_as_mapped(
        self, raising_route, slack_messages, exc_type, status_code, alerted
    ):
        raising_route["error"] = _raise(EXAMPLES[exc_type]())

        with TestClient(app) as tc:
            response = tc.post(f"{BOOM_PATH}/example")

        assert response.status_code == status_code
        assert response.json()["correlation_id"]
        assert len(slack_messages()) == (1 if alerted else 0)

    def test_unmapped_parent_of_a_mapped_database_error_is_still_500(self, raising_route, slack_messages):
        """``DatabaseBusy`` refines ``OperationalError``; any other ``OperationalError`` stays a 500 that alerts."""
        raising_route["error"] = _raise(OperationalError("SELECT", {}, Exception("server closed the connection")))

        with TestClient(app) as tc:
            response = tc.post(f"{BOOM_PATH}/example")

        assert response.status_code == 500
        assert len(slack_messages()) == 1


@pytest.mark.unit
class TestConventions:
    def test_every_mapped_exception_has_an_example(self):
        assert set(EXAMPLES) == set(ERROR_RESPONSES)

    def test_every_exception_is_mapped_or_listed_as_unmapped(self):
        defined = {
            cls
            for module in EXCEPTION_MODULES
            for _, cls in inspect.getmembers(module, inspect.isclass)
            if issubclass(cls, BaseException) and cls.__module__ == module.__name__
        }

        assert defined - set(ERROR_RESPONSES) - UNMAPPED == set(), (
            "Map these in ERROR_RESPONSES or list them in UNMAPPED"
        )
        assert UNMAPPED <= defined
        assert not UNMAPPED & set(ERROR_RESPONSES)

    def test_app_registers_only_the_table(self):
        handlers = dict(app.exception_handlers)

        assert handlers.pop(Exception) is handle_uncaught_exception
        for exc_type in FRAMEWORK_HANDLED:
            handlers.pop(exc_type)
        assert handlers == {exc_type: handle_mapped_exception for exc_type in ERROR_RESPONSES}

    def test_error_response_rejects_a_non_error_status(self):
        with pytest.raises(ValueError):
            ErrorResponse(status=200, body=lambda _exc: {}, alert=False)

    def test_merge_rejects_an_exception_two_tables_map(self):
        entry = ErrorResponse(status=400, body=lambda _exc: {}, alert=False)

        with pytest.raises(ValueError, match="MixedTargetError"):
            merge_error_responses({MixedTargetError: entry}, {MixedTargetError: entry})

    def test_every_domain_table_is_merged(self):
        package = mavedb.lib.exception_handlers
        tables = [
            table
            for info in pkgutil.iter_modules(package.__path__, f"{package.__name__}.")
            for name, table in vars(importlib.import_module(info.name)).items()
            if name.endswith("_ERROR_RESPONSES")
        ]

        assert tables
        for table in tables:
            assert table.items() <= ERROR_RESPONSES.items()
