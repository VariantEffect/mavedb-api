"""Unit tests for the ValidTime mixin.

Tested against minimal, purpose-built models rather than any real MaveDB models. ``_VtParent`` is
a cascade-bearing entity (natural key ``key``); ``_VtChild`` is a leaf link (natural key
``(parent_id, tag)``). Both carry a partial unique index over their natural key
``WHERE valid_to IS NULL``, mirroring the contract real consumers must honor.

Many checks pass an explicit ``at`` that differs from the DB clock: a single-transaction test cannot
otherwise distinguish a deliberate shared timestamp from the coincidental same-transaction handoff
the gap bug relied on, so the explicit ``at`` is what actually proves the timestamp is threaded.
"""

from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy import Column, ForeignKey, Index, Integer, select, text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import declarative_base, relationship

from mavedb.db.mixins import ValidTime

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


def _parent(session, key, *, valid_from=None):
    p = _VtParent(key=key, valid_from=valid_from) if valid_from else _VtParent(key=key)
    session.add(p)
    session.commit()
    return p


def _child(session, parent, tag, *, valid_from=None):
    c = (
        _VtChild(parent_id=parent.id, tag=tag, valid_from=valid_from)
        if valid_from
        else _VtChild(parent_id=parent.id, tag=tag)
    )
    session.add(c)
    session.commit()
    return c


class TestCurrentAndAsOf:
    def test_fresh_row_is_current(self, session):
        p = _parent(session, 1)
        assert p.valid_to is None
        assert p.current is True

    def test_retired_row_is_not_current(self, session):
        p = _parent(session, 1)
        p.retire(session, at=T1)
        assert p.current is False

    def test_current_expression_filters_to_live_rows(self, session):
        live = _parent(session, 1)
        retired = _parent(session, 2)
        retired.retire(session, at=T1)
        session.commit()

        rows = session.scalars(select(_VtParent).where(_VtParent.current)).all()
        assert [r.id for r in rows] == [live.id]

    def test_as_of_selects_rows_live_at_timestamp(self, session):
        # Window [T0, T2): explicit valid_from and a retire at T2.
        p = _parent(session, 1, valid_from=T0)
        p.retire(session, at=T2)
        session.commit()

        def live_ids(ts):
            return [r.id for r in session.scalars(select(_VtParent).where(_VtParent.as_of(ts))).all()]

        assert live_ids(T0 - timedelta(days=1)) == []  # before the window opens
        assert live_ids(T0) == [p.id]  # at valid_from (inclusive)
        assert live_ids(T1) == [p.id]  # inside the window
        assert live_ids(T2) == []  # at valid_to (exclusive)
        assert live_ids(T2 + timedelta(days=1)) == []  # after the window closes


class TestRetire:
    def test_retire_closes_valid_to(self, session):
        c = _child(session, _parent(session, 1), 1)
        c.retire(at=T1)
        assert c.valid_to == T1

    def test_retire_is_idempotent(self, session):
        c = _child(session, _parent(session, 1), 1)
        c.retire(at=T1)
        c.retire(at=T2)  # already closed — keeps the original valid_to
        assert c.valid_to == T1

    def test_leaf_retire_needs_no_session(self, session):
        c = _child(session, _parent(session, 1), 1)
        c.retire()  # no cascade declared, so no session required
        session.commit()
        assert c.valid_to is not None

    def test_retire_cascades_to_live_child_links(self, session):
        p = _parent(session, 1)
        c1 = _child(session, p, 1)
        c2 = _child(session, p, 2)

        p.retire(session, at=T1)
        session.commit()

        assert p.valid_to == T1
        assert c1.valid_to == T1  # cascade closes the children at the same instant
        assert c2.valid_to == T1

    def test_cascade_retire_without_session_raises(self, session):
        p = _parent(session, 1)
        with pytest.raises(ValueError, match="cascade-retire"):
            p.retire()  # cascade declared but no session to issue the child UPDATE

    def test_cascade_runs_even_when_row_already_closed(self, session):
        # A half-finished prior retire (parent closed, child not) must still converge.
        p = _parent(session, 1)
        c = _child(session, p, 1)
        p.valid_to = T0  # simulate parent closed without its child
        session.commit()

        p.retire(session, at=T1)  # idempotent on the parent, but still cascades
        session.commit()
        assert p.valid_to == T0  # unchanged (idempotent)
        assert c.valid_to == T1  # child still retired


class TestRetireLiveWhere:
    def test_retires_matching_live_rows(self, session):
        p = _parent(session, 1)
        c1 = _child(session, p, 1)
        c2 = _child(session, p, 2)

        session.execute(_VtChild.retire_live_where(_VtChild.parent_id == p.id, at=T1))
        session.commit()

        assert c1.valid_to == T1
        assert c2.valid_to == T1

    def test_leaves_already_retired_rows_untouched(self, session):
        p = _parent(session, 1)
        already = _child(session, p, 1)
        already.retire(at=T0)
        session.commit()
        live = _child(session, p, 2)

        session.execute(_VtChild.retire_live_where(_VtChild.parent_id == p.id, at=T1))
        session.commit()

        assert already.valid_to == T0  # retired row keeps its original valid_to
        assert live.valid_to == T1


class TestSupersedeWith:
    def test_gap_free_handoff_with_explicit_at(self, session):
        old = _parent(session, 1, valid_from=T0)
        new = _VtParent(key=1)

        old.supersede_with(session, new, at=T1)
        session.commit()

        # The retired valid_to equals the successor's valid_from exactly — no point-in-time hole.
        assert old.valid_to == T1
        assert new.valid_from == T1

    def test_default_at_handoff_is_gap_free(self, session):
        old = _parent(session, 1)
        new = _VtParent(key=1)

        old.supersede_with(session, new)  # at defaults to a single captured func.now()
        session.commit()

        assert old.valid_to is not None
        assert old.valid_to == new.valid_from

    def test_cascades_child_links_at_the_handoff_timestamp(self, session):
        old = _parent(session, 1)
        child = _child(session, old, 1)
        new = _VtParent(key=1)

        old.supersede_with(session, new, at=T1)
        session.commit()

        assert old.valid_to == T1
        assert new.valid_from == T1
        assert child.valid_to == T1  # the old parent's child link retired under the same timestamp

    def test_respects_partial_unique_index_via_flush_ordering(self, session):
        # old and new share natural key=1; supersede flushes the retire before inserting the
        # successor, so the two are never simultaneously live and the unique index does not trip.
        old = _parent(session, 1)
        new = _VtParent(key=1)

        old.supersede_with(session, new, at=T1)
        session.commit()  # would raise IntegrityError if both were live at the INSERT

        live = session.scalars(select(_VtParent).where(_VtParent.key == 1, _VtParent.current)).all()
        assert [r.id for r in live] == [new.id]


class TestSupersedeLiveWhere:
    def test_stamps_explicit_valid_from_on_inserts(self, session):
        # Without flushing, valid_from would be None under the server_default path; supersede sets it
        # explicitly, which is what keeps the handoff gap-free across a later-transaction flush.
        new = _VtChild(parent_id=1, tag=1)
        _VtChild.supersede_live_where(session, [new], _VtChild.parent_id == 1, at=T1)
        assert new.valid_from == T1

    def test_gap_free_handoff_with_reused_natural_key(self, session):
        p = _parent(session, 1)
        old = _child(session, p, 1)
        new = _VtChild(parent_id=p.id, tag=1)  # same natural key (parent_id, tag)

        _VtChild.supersede_live_where(session, [new], _VtChild.parent_id == p.id, at=T1)
        session.commit()  # reused key: retire executes before the insert, so the index holds

        assert old.valid_to == T1
        assert new.valid_from == T1
        live = session.scalars(select(_VtChild).where(_VtChild.parent_id == p.id, _VtChild.current)).all()
        assert [r.id for r in live] == [new.id]

    def test_empty_new_rows_retires_the_scope(self, session):
        # A re-run that produces nothing should still withdraw the prior live set.
        p = _parent(session, 1)
        old = _child(session, p, 1)

        _VtChild.supersede_live_where(session, [], _VtChild.parent_id == p.id, at=T1)
        session.commit()

        assert old.valid_to == T1
        assert session.scalars(select(_VtChild).where(_VtChild.current)).all() == []

    def test_refuses_a_cascade_bearing_class(self, session):
        # Bulk supersede cannot fire __retire_cascade__, so it must refuse rather than orphan links.
        with pytest.raises(ValueError, match="__retire_cascade__"):
            _VtParent.supersede_live_where(session, [], _VtParent.key == 1)


class TestPartialUniqueIndexBackstop:
    def test_two_live_rows_for_one_key_raise(self, session):
        # The DB index is the loud backstop: forgetting to retire a predecessor before inserting its
        # successor fails at flush instead of silently leaving two live rows.
        session.add(_VtParent(key=1))
        session.add(_VtParent(key=1))
        with pytest.raises(IntegrityError):
            session.commit()
