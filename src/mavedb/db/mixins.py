"""Reusable declarative mixins."""

from datetime import datetime
from typing import ClassVar, Optional, Sequence, TypeVar

from sqlalchemy import ColumnElement, DateTime, Update, func, select, update
from sqlalchemy import inspect as sa_inspect
from sqlalchemy.ext.hybrid import hybrid_method, hybrid_property
from sqlalchemy.orm import Mapped, Session, mapped_column

T = TypeVar("T", bound="ValidTime")


class ValidTime:
    """Valid-time versioning for rows that change over time (transaction-time SCD Type 2).

    Applies to either a versioned entity (a row that is replaced by a newer version of the same
    logical thing — e.g. a mapping record) or a link/association row (a relationship that comes
    and goes). Immutable, content-addressed rows (deduplicated entities like an allele or a
    source-versioned annotation record) do not use this — their "what applies now" is answered by
    the links that reference them, not by versioning the row itself.

    A row is live while ``valid_to`` is NULL. Superseding a row sets its ``valid_to`` to the
    successor's ``valid_from`` instead of deleting it, so the full history is retained and a
    point-in-time query is a single half-open ``[valid_from, valid_to)`` predicate. ``current``
    and ``as_of`` express that predicate so call sites never hand-roll it.

    Convention: replacing a live row with a successor goes through :meth:`supersede_with` (single
    row) or :meth:`supersede_live_where` (bulk), which stamp the retired ``valid_to`` and the new
    ``valid_from`` with one timestamp so the handoff is gap-free regardless of transaction
    boundaries. ``retire`` (single) / ``retire_live_where`` (bulk) are *withdrawal* primitives —
    retiring with no successor; do not pair a bare retire with a separate insert, or a slow job
    between the two opens a window where the row is neither live-old nor live-new. The method pairs
    mirror: ``retire``/``supersede_with`` act on one row, ``*_live_where`` act on a predicate.

    ``current`` is derived (``valid_to IS NULL``), not stored — one source of truth, no
    dual-write to keep consistent. Each table using this mixin should add a partial unique index
    over its natural key ``WHERE valid_to IS NULL`` to enforce a single live row per key (the link
    key for an association, the logical-entity key for an entity).

    Transaction time only: ``valid_from``/``valid_to`` record when *we* held the row, not when an
    external source considered it effective. Tables fed by externally-versioned sources (e.g. a
    ClinVar release, a gnomAD version) should add their own source-version column for that axis.

    Consumer contract — a model mixing this in, and any code writing it, MUST honor:

    1. **Add the partial unique index.** Declare a unique index over the natural key
       ``WHERE valid_to IS NULL`` (one live row per key). It is the loud backstop: forgetting to
       retire a predecessor before inserting its successor raises ``IntegrityError`` instead of
       silently leaving two live rows.
    2. **Replace through supersede, never retire-then-insert.** Use :meth:`supersede_with` (single
       row) or :meth:`supersede_live_where` (bulk) to swap a live row for a successor. A bare
       :meth:`retire` followed by a separate insert is the gap footgun — if a slow job or a commit
       falls between them, the retired ``valid_to`` and the new ``valid_from`` come from different
       transaction clocks and a point-in-time query lands in a hole where the row is neither
       old-live nor new-live. Reserve ``retire``/``retire_live_where`` for genuine withdrawal (no
       successor). Gaps only arise on a *same-key* handoff; the cascade in (3) only ever retires
       children under a superseded parent's old key, so it never needs a supersede.
    3. **Declare ``__retire_cascade__`` for parent rows with ``ValidTime`` children.** A live link to
       a retired parent is stale; retiring a parent must retire its live child links. The ORM
       relationship cascade does not do this (it fires only on hard DELETE). Bulk
       :meth:`supersede_live_where` refuses to run on a class that declares ``__retire_cascade__``
       precisely because it cannot cascade — supersede such rows one at a time with
       :meth:`supersede_with`.
    4. **Filter reads with ``current``/``as_of``.** Never read live rows by assuming a query returns
       only current state; constrain with ``current`` (or ``as_of(ts)``) explicitly. Because of (3),
       a live link implies a live parent, so allele/parent-side ``current`` queries do not surface
       links dangling off retired parents.
    """

    valid_from: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())
    valid_to: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)

    # Names of relationships to child ``ValidTime`` rows that should be retired when this row is.
    # A link to a retired row is itself stale, so closing a parent's window must close its live
    # links' windows too — the relationship cascade ("all, delete-orphan") only fires on a hard
    # DELETE, not on this soft ``valid_to`` close. Each named relationship must target a
    # ``ValidTime`` model; ``retire`` issues one bulk UPDATE per relationship.
    #
    # This covers only the transaction-time axis: "the parent was superseded, so its links are too."
    # Links retired on a source-version axis (e.g. a new gnomAD release superseding an
    # allele→gnomAD-variant link while the allele itself stays live) are a separate trigger and are
    # not driven from here. Cascade is one level deep (bulk UPDATE, not per-row ``retire``); a deeper
    # chain would need its own handling.
    __retire_cascade__: ClassVar[tuple[str, ...]] = ()

    @hybrid_property
    def current(self) -> bool:
        return self.valid_to is None

    @current.inplace.expression
    @classmethod
    def _current_expression(cls) -> ColumnElement[bool]:
        return cls.valid_to.is_(None)

    @classmethod
    def as_of(cls, ts: datetime) -> ColumnElement[bool]:
        """Filter clause selecting the rows live at ``ts`` (half-open ``[valid_from, valid_to)``)."""
        return (cls.valid_from <= ts) & (cls.valid_to.is_(None) | (cls.valid_to > ts))

    @hybrid_method
    def live_at(self, as_of: Optional[datetime]) -> bool:
        """Live at ``as_of``, or currently live when ``as_of`` is ``None``.

        The one-call form of the ``as_of(ts) if ts is not None else current`` branch every temporal
        read otherwise repeats. Same result as :attr:`current` / :meth:`as_of`, but usable uniformly on
        the class, an instance, and an ``aliased()`` class (SQLAlchemy adapts the column refs to it)."""
        if as_of is None:
            return self.valid_to is None
        return self.valid_from <= as_of and (self.valid_to is None or self.valid_to > as_of)

    @live_at.expression
    @classmethod
    def _live_at_expression(cls, as_of: Optional[datetime]) -> ColumnElement[bool]:
        if as_of is None:
            return cls.valid_to.is_(None)
        return (cls.valid_from <= as_of) & (cls.valid_to.is_(None) | (cls.valid_to > as_of))

    def retire(self, session: Optional[Session] = None, at: Optional[datetime] = None) -> None:
        """Close this row's validity window, cascading to the live child links named in
        ``__retire_cascade__``. Idempotent — an already-closed row keeps its original valid_to.

        Pass ``at`` to stamp an explicit ``valid_to`` shared with a successor's ``valid_from``; this
        is how :meth:`supersede_with` keeps the handoff gap-free. Without ``at`` the close uses
        ``func.now()`` (Postgres ``transaction_timestamp``). This is a *withdrawal* primitive on its
        own — to replace a row with a successor, use :meth:`supersede_with` so the handoff is closed.

        ``session`` is required only when ``__retire_cascade__`` is non-empty (the cascade issues a
        bulk UPDATE per child relationship). A leaf row with no declared cascades retires without one.
        The cascade runs even when this row was already closed, so a half-finished prior retire (row
        closed, links not) still converges.
        """
        stamp = at if at is not None else func.now()
        if self.valid_to is None:
            self.valid_to = stamp

        if not self.__retire_cascade__:
            return
        if session is None:
            raise ValueError(
                f"{type(self).__name__}.retire() needs a session to cascade-retire {self.__retire_cascade__}."
            )

        mapper = sa_inspect(type(self))
        assert mapper is not None  # a mapped class always inspects to a Mapper
        for name in self.__retire_cascade__:
            rel = mapper.relationships[name]
            child_cls = rel.mapper.class_
            conditions = [remote == getattr(self, local.key) for local, remote in rel.local_remote_pairs]
            session.execute(child_cls.retire_live_where(*conditions, at=at))

    @classmethod
    def retire_live_where(cls, *conditions: ColumnElement[bool], at: Optional[datetime] = None) -> Update:
        """An UPDATE that retires the live rows matching ``conditions`` (closes their valid_to).
        Only currently-live rows are touched, so already-retired rows keep their original valid_to.
        Pass ``at`` to stamp an explicit ``valid_to`` (used by :meth:`supersede`); defaults to
        ``func.now()``.

        This is a *withdrawal* primitive — retiring live rows with no successor. To replace live rows
        with new ones, use :meth:`supersede_live_where` so retire and insert share one timestamp.

        Usage: ``session.execute(Model.retire_live_where(Model.foo == bar))``.
        """
        return (
            update(cls).where(cls.valid_to.is_(None), *conditions).values(valid_to=at if at is not None else func.now())
        )

    def supersede_with(self: T, session: Session, replacement: T, at: Optional[datetime] = None) -> T:
        """Retire this row (cascading to its child links) and insert ``replacement``, stamping the
        retired row's ``valid_to`` and the replacement's ``valid_from`` with one timestamp so the
        handoff has no gap — independent of how many transactions or how much wall-clock separate
        them. The single-row counterpart to :meth:`supersede_live_where`, for superseding one known
        row that may carry ``__retire_cascade__`` children.

        Flushes the retire before the insert: the partial unique index (one live row per natural key)
        is checked per statement and the unit of work emits INSERTs before UPDATEs, so without the
        flush the replacement and the not-yet-closed original would both be live and trip the index.
        """
        if at is None:
            at = session.scalar(select(func.now()))
        assert at is not None  # SELECT now() always returns a timestamp
        self.retire(session, at=at)
        session.flush()
        replacement.valid_from = at
        session.add(replacement)
        return replacement

    @classmethod
    def supersede_live_where(
        cls,
        session: Session,
        new_rows: Sequence[T],
        *conditions: ColumnElement[bool],
        at: Optional[datetime] = None,
    ) -> Sequence[T]:
        """Retire the live rows matching ``conditions`` and insert ``new_rows``, stamping both sides
        with one timestamp so every retired row's ``valid_to`` equals every new row's ``valid_from``
        — a gap-free handoff independent of transaction boundaries. The bulk counterpart to
        :meth:`supersede_with` (and the supersede form of :meth:`retire_live_where`), for replacing a
        *set* of live rows in one scope (e.g. a score set's derived links) with a freshly computed
        set.

        The retire executes immediately (before the inserts flush), so a reused natural key does not
        trip the partial unique index. Only for leaf ``ValidTime`` rows: a bulk retire cannot fire
        ``__retire_cascade__``, so this refuses to run on a class that declares one — use
        :meth:`supersede_with` per row there.
        """
        if cls.__retire_cascade__:
            raise ValueError(
                f"{cls.__name__} declares __retire_cascade__; bulk supersede cannot cascade to children. "
                "Use supersede_with per row so child links retire too."
            )
        if at is None:
            at = session.scalar(select(func.now()))
        assert at is not None  # SELECT now() always returns a timestamp
        session.execute(cls.retire_live_where(*conditions, at=at))
        for row in new_rows:
            row.valid_from = at
        session.add_all(new_rows)
        return new_rows
