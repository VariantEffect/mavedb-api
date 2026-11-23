"""Unit tests for the ValidTime mixin.

Tested against minimal, purpose-built models rather than any real MaveDB models. ``_VtParent`` is
a cascade-bearing entity (natural key ``key``); ``_VtChild`` is a leaf link (natural key
``(parent_id, tag)``). Both carry a partial unique index over their natural key
``WHERE valid_to IS NULL``, mirroring the contract real consumers must honor.

Many checks pass an explicit ``at`` that differs from the DB clock: a single-transaction test cannot
otherwise distinguish a deliberate shared timestamp from the coincidental same-transaction handoff
the gap bug relied on, so the explicit ``at`` is what actually proves the timestamp is threaded.
"""

# ruff: noqa: E402
import asyncio
from datetime import datetime, timedelta, timezone

import pytest

pytest.importorskip("psycopg2")

from sqlalchemy import Column, ForeignKey, Index, Integer, String, delete, func, select, text, update
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session, aliased, declarative_base, relationship

from mavedb.models.mixins.valid_time import (
    SupersedeLockTimeout,
    ValidTime,
    _lock_key,
    holds_supersede_lock,
    superseding,
)

VtBase = declarative_base()

# Fixed, deterministic windows — far from the test's transaction clock so an accidental
# server_default (func.now()) stamp would be visibly wrong rather than coincidentally equal.
T0 = datetime(2020, 1, 1, tzinfo=timezone.utc)
T1 = datetime(2021, 1, 1, tzinfo=timezone.utc)
T2 = datetime(2022, 1, 1, tzinfo=timezone.utc)


class _VtParent(ValidTime, VtBase):
    __tablename__ = "vt_parents"

    id = Column(Integer, primary_key=True)
    key = Column(Integer, nullable=False)
    children = relationship("_VtChild", back_populates="parent")

    __retire_cascade__ = ("children",)
    __table_args__ = (Index("uq_vt_parents_live", "key", unique=True, postgresql_where=text("valid_to IS NULL")),)


class _VtChild(ValidTime, VtBase):
    __tablename__ = "vt_children"

    id = Column(Integer, primary_key=True)
    parent_id = Column(Integer, ForeignKey("vt_parents.id"), nullable=False)
    tag = Column(Integer, nullable=False)
    parent = relationship("_VtParent", back_populates="children")

    __table_args__ = (
        Index("uq_vt_children_live", "parent_id", "tag", unique=True, postgresql_where=text("valid_to IS NULL")),
    )


class _VtNote(VtBase):
    """An unversioned table, for checking what a block commits."""

    __tablename__ = "vt_notes"

    id = Column(Integer, primary_key=True)
    text = Column(String, nullable=False)


@pytest.fixture(autouse=True)
def vt_tables(session):
    """Create the throwaway ValidTime tables on the test engine, drop them after. Autouse so every
    test in the module gets the tables without naming the fixture in its signature."""
    engine = session.get_bind()
    VtBase.metadata.create_all(bind=engine)
    yield
    # Release any locks the test's still-open transaction holds before dropping: a post-commit
    # attribute access reopens a transaction holding ACCESS SHARE on these tables, and DROP TABLE
    # would block on it forever (this fixture tears down while the session fixture is still open).
    session.rollback()
    VtBase.metadata.drop_all(bind=engine)


def _parent(session, key, *, valid_from=None, valid_to=None):
    p = _VtParent(key=key, valid_from=valid_from, valid_to=valid_to)
    with superseding(session, _VtParent):
        session.add(p)
    return p


def _child(session, parent, tag, *, valid_from=None, valid_to=None):
    c = _VtChild(parent_id=parent.id, tag=tag, valid_from=valid_from, valid_to=valid_to)
    with superseding(session, _VtChild):
        session.add(c)
    return c


def _live(session, model):
    return session.scalars(select(model).where(model.current).order_by(model.id)).all()


class TestCurrentAndAsOf:
    def test_fresh_row_is_current(self, session):
        p = _parent(session, 1)
        assert p.valid_to is None
        assert p.current is True

    def test_retired_row_is_not_current(self, session):
        p = _parent(session, 1, valid_from=T0, valid_to=T1)
        assert p.current is False

    def test_current_expression_filters_to_live_rows(self, session):
        live = _parent(session, 1)
        _parent(session, 2, valid_from=T0, valid_to=T1)

        assert [r.id for r in _live(session, _VtParent)] == [live.id]

    def test_as_of_selects_rows_live_at_timestamp(self, session):
        p = _parent(session, 1, valid_from=T0, valid_to=T2)

        def live_ids(ts):
            return [r.id for r in session.scalars(select(_VtParent).where(_VtParent.as_of(ts))).all()]

        assert live_ids(T0 - timedelta(days=1)) == []  # before the window opens
        assert live_ids(T0) == [p.id]  # at valid_from (inclusive)
        assert live_ids(T1) == [p.id]  # inside the window
        assert live_ids(T2) == []  # at valid_to (exclusive)
        assert live_ids(T2 + timedelta(days=1)) == []  # after the window closes


class TestLiveAt:
    """``live_at(as_of)`` is the one-call ``as_of(ts) if ts else current`` — same result, but it also
    works on an ``aliased()`` entity, which the raw ternary at call sites does too, but only clumsily."""

    def test_live_at_none_is_current_on_instance(self, session):
        live = _parent(session, 1)
        retired = _parent(session, 2, valid_from=T0, valid_to=T1)
        assert live.live_at(None) is True
        assert retired.live_at(None) is False

    def test_live_at_ts_matches_the_window_on_instance(self, session):
        p = _parent(session, 1, valid_from=T0, valid_to=T2)
        assert p.live_at(T0 - timedelta(days=1)) is False  # before the window opens
        assert p.live_at(T0) is True  # at valid_from (inclusive)
        assert p.live_at(T1) is True  # inside the window
        assert p.live_at(T2) is False  # at valid_to (exclusive)

    def test_live_at_expression_agrees_with_current_and_as_of(self, session):
        p = _parent(session, 1, valid_from=T0, valid_to=T2)

        def ids(pred):
            return [r.id for r in session.scalars(select(_VtParent).where(pred)).all()]

        assert ids(_VtParent.live_at(None)) == ids(_VtParent.current) == []  # retired -> not current
        assert ids(_VtParent.live_at(T1)) == ids(_VtParent.as_of(T1)) == [p.id]

    def test_live_at_is_alias_aware(self, session):
        # The point of live_at: it scopes to the alias, not the base table (a plain classmethod on an
        # aliased entity is the trap this avoids). Both the compiled SQL and a query through the alias.
        p = _parent(session, 1, valid_from=T0)
        alias = aliased(_VtParent, name="pa")
        assert "pa." in str(alias.live_at(T1))
        rows = session.scalars(select(alias).where(alias.live_at(T1))).all()
        assert [r.id for r in rows] == [p.id]


class TestSupersede:
    def test_handoff_is_gap_free_and_reuses_the_live_key(self, session):
        old = _parent(session, 1)
        new = _VtParent(key=1)

        with superseding(session, _VtParent) as block:
            _VtParent.supersede(session, retiring=[old], inserting=[new])
            at = block.at

        session.refresh(old)
        session.refresh(new)
        assert old.valid_to == new.valid_from == at
        assert [r.id for r in _live(session, _VtParent)] == [new.id]

    def test_retiring_alone_withdraws(self, session):
        c = _child(session, _parent(session, 1), 1)

        with superseding(session, _VtChild):
            _VtChild.supersede(session, retiring=[c])

        assert _live(session, _VtChild) == []

    def test_cascades_to_live_children(self, session):
        p = _parent(session, 1)
        children = [_child(session, p, tag) for tag in (1, 2)]

        with superseding(session, _VtParent) as block:
            _VtParent.supersede(session, retiring=[p])
            at = block.at

        for child in children:
            session.refresh(child)
            assert child.valid_to == at

    def test_already_retired_rows_keep_their_valid_to(self, session):
        p = _parent(session, 1, valid_from=T0, valid_to=T1)

        with superseding(session, _VtParent):
            _VtParent.supersede(session, retiring=[p])

        session.refresh(p)
        assert p.valid_to == T1

    def test_retires_in_batches(self, session, monkeypatch):
        monkeypatch.setattr("mavedb.models.mixins.valid_time.STATEMENT_CHUNK_SIZE", 1)
        p = _parent(session, 1)
        old = [_child(session, p, tag) for tag in (1, 2)]

        with superseding(session, _VtChild):
            _VtChild.supersede(session, retiring=old, inserting=[_VtChild(parent_id=p.id, tag=tag) for tag in (1, 2)])

        live = _live(session, _VtChild)
        assert sorted(c.tag for c in live) == [1, 2]
        assert not {c.id for c in live} & {c.id for c in old}

    def test_outside_a_block_raises(self, session):
        with pytest.raises(RuntimeError, match="outside superseding"):
            _VtParent.supersede(session, inserting=[_VtParent(key=1)])

    def test_rows_of_another_model_raise(self, session):
        with superseding(session, _VtParent):
            with pytest.raises(TypeError, match="_VtChild"):
                _VtParent.supersede(session, inserting=[_VtChild(parent_id=1, tag=1)])


class TestRetireWhere:
    def test_retires_matching_live_rows_and_counts_them(self, session):
        p = _parent(session, 1)
        _child(session, p, 1)
        _child(session, p, 2)
        _child(session, p, 3, valid_from=T0, valid_to=T1)

        with superseding(session, _VtChild):
            retired = _VtChild.retire_where(session, _VtChild.parent_id == p.id)

        assert retired == 2
        assert _live(session, _VtChild) == []

    def test_cascades_to_live_children(self, session):
        p = _parent(session, 1)
        _child(session, p, 1)

        with superseding(session, _VtParent):
            _VtParent.retire_where(session, _VtParent.key == 1)

        assert _live(session, _VtChild) == []


class TestSupersedingLocksCascadeTargets:
    def test_a_parent_block_locks_its_children(self, session):
        with superseding(session, _VtParent):
            assert holds_supersede_lock(session, _VtChild)


class TestWritesNeedABlock:
    def test_orm_insert_outside_a_block_raises(self, session):
        session.add(_VtParent(key=1))
        with pytest.raises(RuntimeError, match="outside superseding"):
            session.flush()

    def test_orm_update_outside_a_block_raises(self, session):
        p = _parent(session, 1)
        p.key = 2
        with pytest.raises(RuntimeError, match="outside superseding"):
            session.flush()

    def test_statement_outside_a_block_raises(self, session):
        with pytest.raises(RuntimeError, match="outside superseding"):
            session.execute(update(_VtParent).values(key=2))

    def test_block_for_another_table_doesnt_count(self, session):
        with pytest.raises(RuntimeError, match="outside superseding"):
            with superseding(session, _VtChild):
                session.add(_VtParent(key=1))

    def test_deletes_are_allowed(self, session):
        _parent(session, 1)
        session.execute(delete(_VtParent))
        session.commit()


@pytest.fixture
def other_session(session):
    """A second connection, standing in for another job."""
    other = Session(bind=session.get_bind())
    yield other
    other.rollback()
    other.close()


@pytest.fixture
def fast_polling(monkeypatch):
    monkeypatch.setattr("mavedb.models.mixins.valid_time.SUPERSEDE_POLL_SECONDS", 0.01)


def _hold_lock(other_session, model):
    other_session.execute(select(func.pg_advisory_xact_lock(_lock_key(model.__tablename__))))


class TestSuperseding:
    def test_commits_on_exit_and_releases_the_lock(self, session):
        p = _parent(session, 1)
        with superseding(session, _VtChild):
            assert holds_supersede_lock(session, _VtChild)
            session.add(_VtChild(parent_id=p.id, tag=1))

        session.rollback()
        assert _live(session, _VtChild) != []
        assert not holds_supersede_lock(session, _VtChild)

    def test_rolls_back_on_exception(self, session):
        p = _parent(session, 1)
        with pytest.raises(ZeroDivisionError):
            with superseding(session, _VtChild):
                session.add(_VtChild(parent_id=p.id, tag=1))
                session.flush()
                1 / 0

        assert _live(session, _VtChild) == []
        assert not holds_supersede_lock(session, _VtChild)

    def test_dry_run_rolls_back(self, session):
        p = _parent(session, 1)
        with superseding(session, _VtChild, dry_run=True):
            session.add(_VtChild(parent_id=p.id, tag=1))
            session.flush()

        assert _live(session, _VtChild) == []

    def test_commits_pending_work_before_waiting(self, session):
        session.add(_VtNote(text="pending"))
        with superseding(session, _VtChild):
            pass

        session.rollback()
        assert [n.text for n in session.scalars(select(_VtNote))] == ["pending"]

    def test_nested_block_commits_with_the_outer_one_and_shares_its_time(self, session):
        p = _parent(session, 1)
        with pytest.raises(ZeroDivisionError):
            with superseding(session, _VtParent) as outer:
                with superseding(session, _VtChild) as inner:
                    assert inner.at == outer.at
                    session.add(_VtChild(parent_id=p.id, tag=1))
                assert holds_supersede_lock(session, _VtChild)
                1 / 0

        assert _live(session, _VtChild) == []

    @pytest.mark.asyncio
    async def test_nested_async_block_raises(self, session):
        with superseding(session, _VtParent):
            with pytest.raises(RuntimeError, match="plain `with`"):
                async with superseding(session, _VtChild):
                    pass

    def test_inside_a_savepoint_raises(self, session):
        with session.begin_nested():
            with pytest.raises(RuntimeError, match="savepoint"):
                with superseding(session, _VtChild):
                    pass

    def test_commit_inside_the_block_raises_on_exit(self, session):
        with pytest.raises(RuntimeError, match="committed inside the block"):
            with superseding(session, _VtChild):
                session.commit()

    def test_times_out_waiting_and_calls_on_wait(self, session, other_session, fast_polling):
        _hold_lock(other_session, _VtChild)
        waits = []

        with pytest.raises(SupersedeLockTimeout):
            with superseding(session, _VtChild, max_wait=timedelta(seconds=0.2), on_wait=lambda: waits.append(1)):
                pass

        assert waits, "on_wait is called while waiting"

    @pytest.mark.asyncio
    async def test_async_block_waits_until_the_lock_is_free(self, session, other_session, fast_polling):
        _hold_lock(other_session, _VtChild)
        asyncio.get_running_loop().call_later(0.2, other_session.commit)

        async with superseding(session, _VtChild, max_wait=timedelta(seconds=5)):
            assert holds_supersede_lock(session, _VtChild)

    def test_releases_a_partial_set_while_waiting(self, session, other_session, fast_polling):
        # vt_children sorts before vt_parents, so this block gets _VtChild's lock, then waits for _VtParent's.
        _hold_lock(other_session, _VtParent)

        with pytest.raises(SupersedeLockTimeout):
            with superseding(session, _VtParent, _VtChild, max_wait=timedelta(seconds=0.2)):
                pass

        assert other_session.scalar(select(func.pg_try_advisory_xact_lock(_lock_key(_VtChild.__tablename__))))


class TestPartialUniqueIndexBackstop:
    def test_two_live_rows_for_one_key_raise(self, session):
        # The DB index is the loud backstop: forgetting to retire a predecessor before inserting its
        # successor fails at flush instead of silently leaving two live rows.
        with pytest.raises(IntegrityError):
            with superseding(session, _VtParent):
                session.add(_VtParent(key=1))
                session.add(_VtParent(key=1))
