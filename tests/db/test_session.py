# ruff: noqa: E402

import pytest

pytest.importorskip("psycopg2")

from mavedb.db import session as db_session


@pytest.mark.unit
def test_engine_hides_bound_parameters():
    """The api and the worker share this engine, so both keep bound values out of exception text."""
    assert db_session.engine.hide_parameters is True
