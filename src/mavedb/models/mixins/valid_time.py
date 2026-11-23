"""Valid-time versioning for rows that change over time."""

import asyncio
import hashlib
import time
from datetime import datetime, timedelta
from typing import TYPE_CHECKING, Any, Callable, ClassVar, Iterable, Optional

from sqlalchemy import ColumnElement, DateTime, event, func, select, text, tuple_, update
from sqlalchemy import inspect as sa_inspect
from sqlalchemy.ext.hybrid import hybrid_method, hybrid_property
from sqlalchemy.orm import Mapped, ORMExecuteState, Session, mapped_column, object_session
from sqlalchemy.orm.attributes import set_committed_value


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
    and ``as_of`` express that predicate so call sites never hand-roll it. ``current`` is derived
    (``valid_to IS NULL``), not stored.

    Transaction time only: ``valid_from``/``valid_to`` record when *we* held the row, not when an
    external source considered it effective. Tables fed by externally-versioned sources (e.g. a
    ClinVar release, a gnomAD version) should add their own source-version column for that axis.

    Consumer contract — a model mixing this in, and any code writing it, MUST honor:

    1. **Add the partial unique index.** Declare a unique index over the natural key
       ``WHERE valid_to IS NULL`` (one live row per key). It is the loud backstop: forgetting to
       retire a predecessor before inserting its successor raises ``IntegrityError`` instead of
       silently leaving two live rows.
    2. **Write inside a** :class:`superseding` **block.** Every insert or update to a ``ValidTime`` table
       happens inside a block holding that table's lock, which the listeners at the end of this module
       enforce. Read the live rows you'll replace inside the same block, so no other job writes in
       between, and replace them with :meth:`supersede` or :meth:`retire_where`, which stamp everything
       with the block's one timestamp so every handoff is gap-free.
    3. **Declare ``__retire_cascade__`` for parent rows with ``ValidTime`` children.** A live link to
       a retired parent is stale, so the block retires a parent's live children with it. The ORM
       relationship cascade does not do this (it fires only on hard DELETE).
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
    # ``ValidTime`` model; retiring issues one bulk UPDATE per relationship.
    #
    # This covers only the transaction-time axis: "the parent was superseded, so its links are too."
    # Links retired on a source-version axis (e.g. a new gnomAD release superseding an
    # allele→gnomAD-variant link while the allele itself stays live) are a separate trigger and are
    # not driven from here. Cascade is one level deep; a deeper chain would need its own handling.
    __retire_cascade__: ClassVar[tuple[str, ...]] = ()

    if TYPE_CHECKING:
        __tablename__: str

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

    @classmethod
    def supersede(
        cls, session: Session, retiring: Iterable["ValidTime"] = (), inserting: Iterable["ValidTime"] = ()
    ) -> None:
        """Retire the live rows in ``retiring`` and their ``__retire_cascade__`` children, then add ``inserting``.

        Runs inside a :class:`superseding` block for this table and stamps everything with the block's time, so the
        handoff is gap-free. A successor can reuse its predecessor's live key: the retirements run immediately, the
        inserts at the next flush. Already-retired rows are left alone. Pass only ``retiring`` to withdraw rows.
        """
        at = _block_at(session, cls)
        retiring, inserting = list(retiring), list(inserting)
        strays = [row for row in [*retiring, *inserting] if not isinstance(row, cls)]
        if strays:
            raise TypeError(f"{cls.__name__}.supersede got rows of {sorted({type(row).__name__ for row in strays})}")

        live = [row for row in retiring if row.valid_to is None]
        primary_key = _primary_key(cls)
        for chunk in _chunks(live):
            ids = [getattr(row, primary_key.key) for row in chunk]
            session.execute(
                update(cls).where(primary_key.in_(ids), cls.valid_to.is_(None)).values(valid_to=at),
                execution_options={"synchronize_session": False},
            )
        for row in live:
            set_committed_value(row, "valid_to", at)
        _retire_children(session, cls, live, at)

        for row in inserting:
            row.valid_from = at
            session.add(row)

    @classmethod
    def retire_where(cls, session: Session, *conditions: ColumnElement[bool]) -> int:
        """Retire this table's live rows matching ``conditions``, and their ``__retire_cascade__`` children, at the
        enclosing :class:`superseding` block's time. Returns how many were retired."""
        at = _block_at(session, cls)
        statement = update(cls).where(cls.valid_to.is_(None), *conditions).values(valid_to=at)
        retired = session.execute(
            statement.returning(*_cascade_columns(cls, _primary_key(cls))),
            execution_options={"synchronize_session": False},
        ).all()
        _retire_children(session, cls, list(retired), at)
        return len(retired)


STATEMENT_CHUNK_SIZE = 1000
"""Rows per statement in bulk writes, well under Postgres's 65,535 bind parameter limit."""

SUPERSEDE_MAX_WAIT = timedelta(minutes=15)
"""How long a :class:`superseding` block waits for its locks before raising :class:`SupersedeLockTimeout`."""

SUPERSEDE_POLL_SECONDS = 1.0
"""How often a waiting :class:`superseding` block retries its locks."""

SUPERSEDE_HEARTBEAT_SECONDS = 60.0
"""How often a waiting :class:`superseding` block calls its ``on_wait`` callback."""

_HELD_LOCKS = "valid_time.superseding.locks"
"""``Session.info`` key for the tables whose supersede locks the session's transaction holds."""

_BLOCK_AT = "valid_time.superseding.at"
"""``Session.info`` key for the outermost block's timestamp, which nested blocks share."""


class SupersedeLockTimeout(TimeoutError):
    """A :class:`superseding` block gave up waiting for another transaction to release its locks."""


def _chunks(items: list) -> list[list]:
    return [items[i : i + STATEMENT_CHUNK_SIZE] for i in range(0, len(items), STATEMENT_CHUNK_SIZE)]


def _held_locks(session: Session) -> set[str]:
    return session.info.setdefault(_HELD_LOCKS, set())


def _lock_key(table: str) -> int:
    """A stable signed 64-bit advisory lock key for ``table``. Python's ``hash`` changes per process."""
    digest = hashlib.blake2b(f"mavedb.supersede.{table}".encode(), digest_size=8).digest()
    return int.from_bytes(digest, "big", signed=True)


def holds_supersede_lock(session: Session, model: type[ValidTime]) -> bool:
    """Whether this session's transaction holds ``model``'s supersede lock, as Postgres sees it."""
    return _holds_lock(session, model.__tablename__)


def _holds_lock(session: Session, table: str) -> bool:
    key = _lock_key(table)
    return bool(
        session.scalar(
            text(
                "SELECT EXISTS (SELECT 1 FROM pg_locks WHERE locktype = 'advisory' AND pid = pg_backend_pid() "
                "AND classid = :classid AND objid = :objid AND objsubid = 1)"
            ),
            {"classid": (key >> 32) & 0xFFFFFFFF, "objid": key & 0xFFFFFFFF},
        )
    )


class superseding:
    """The only way to write ``ValidTime`` rows: a block that retires and inserts them with no other transaction
    in between.

    Takes a transaction-scoped advisory lock per table and commits on exit, which releases them (an exception rolls
    back instead, which also releases them). Other blocks for the same tables wait until then, so live rows read
    inside a block are still current when it writes. Use ``async with`` from jobs: a waiting job sleeps, so other
    jobs in the same worker keep running. Plain ``with`` sleeps the thread, so keep it to short holds.

    Inside, :meth:`ValidTime.supersede` and :meth:`ValidTime.retire_where` stamp everything with :attr:`at`, so every
    handoff is gap-free. Rows added to the session directly get the same time from ``valid_from``'s default.

    The outermost block commits pending work before waiting, so it holds nothing while it waits, and calls
    ``on_wait`` about once a minute so a job can record that it's waiting. A block nested inside another doesn't
    commit: it adds its locks to the outer block's, which commits them all, so what the two blocks write becomes
    visible together. A nested block must use plain ``with``, since it waits while holding the outer block's locks.
    Nest tables in the same order everywhere: two jobs nesting the same pair in opposite orders wait on each other
    until one times out. Don't commit inside a block: it releases the locks early, and the block raises on exit.
    With ``dry_run``, the outermost block rolls back on exit instead of committing.
    """

    def __init__(
        self,
        session: Session,
        *models: type[ValidTime],
        max_wait: timedelta = SUPERSEDE_MAX_WAIT,
        on_wait: Optional[Callable[[], None]] = None,
        dry_run: bool = False,
    ):
        if not models:
            raise ValueError("superseding needs at least one model")

        self.session = session
        self.models = sorted(set(models) | _cascade_targets(models), key=lambda model: model.__tablename__)
        self.max_wait = max_wait
        self.on_wait = on_wait
        self.dry_run = dry_run
        self._outermost = False

    @property
    def at(self) -> datetime:
        """The block's one timestamp: every row it retires closes at this time, and every row it inserts opens."""
        return self.session.info[_BLOCK_AT]

    def _begin(self, sleeping_is_async: bool) -> None:
        held = _held_locks(self.session)
        self._outermost = not held
        if self._outermost:
            if self.session.in_nested_transaction():
                raise RuntimeError("superseding can't start inside a savepoint: it owns its transaction's commit")
            self.session.commit()
            return

        if sleeping_is_async:
            raise RuntimeError(
                "A nested superseding block must use plain `with`: `async with` would sleep holding locks"
            )
        if self.on_wait is not None:
            raise ValueError("A nested superseding block can't take on_wait: it may commit and release the outer locks")

    def _try_acquire(self) -> bool:
        """Try each lock without waiting. The outermost block releases a partial set, so it waits holding nothing."""
        held = _held_locks(self.session)
        for model in self.models:
            if model.__tablename__ in held:
                continue
            if not self.session.scalar(select(func.pg_try_advisory_xact_lock(_lock_key(model.__tablename__)))):
                if self._outermost:
                    self.session.rollback()
                    held.clear()
                return False
            held.add(model.__tablename__)
        return True

    def _acquired(self) -> "superseding":
        if self._outermost:
            self.session.info[_BLOCK_AT] = self.session.scalars(select(func.now())).one()
        return self

    def _wait_step(self, deadline: float, next_heartbeat: float) -> float:
        """Check the deadline and heartbeat before sleeping again. Returns the next heartbeat time."""
        now = time.monotonic()
        if now >= deadline:
            names = ", ".join(model.__tablename__ for model in self.models)
            raise SupersedeLockTimeout(f"Timed out after {self.max_wait} waiting to supersede rows in {names}")
        if self.on_wait is not None and now >= next_heartbeat:
            self.on_wait()
            return now + SUPERSEDE_HEARTBEAT_SECONDS
        return next_heartbeat

    def __enter__(self) -> "superseding":
        self._begin(sleeping_is_async=False)
        deadline, next_heartbeat = time.monotonic() + self.max_wait.total_seconds(), time.monotonic()
        while not self._try_acquire():
            next_heartbeat = self._wait_step(deadline, next_heartbeat)
            time.sleep(SUPERSEDE_POLL_SECONDS)
        return self._acquired()

    async def __aenter__(self) -> "superseding":
        self._begin(sleeping_is_async=True)
        deadline, next_heartbeat = time.monotonic() + self.max_wait.total_seconds(), time.monotonic()
        while not self._try_acquire():
            next_heartbeat = self._wait_step(deadline, next_heartbeat)
            await asyncio.sleep(SUPERSEDE_POLL_SECONDS)
        return self._acquired()

    def __exit__(self, exc_type, exc, tb) -> None:
        if not self._outermost:
            return

        held = _held_locks(self.session)
        try:
            if exc_type is not None or self.dry_run:
                self.session.rollback()
                return

            lost = [table for table in held if not _holds_lock(self.session, table)]
            if lost:
                self.session.rollback()
                raise RuntimeError(
                    f"Lost the supersede lock for {', '.join(lost)}: something committed inside the block"
                )

            self.session.commit()
        finally:
            held.clear()
            self.session.info.pop(_BLOCK_AT, None)

    async def __aexit__(self, exc_type, exc, tb) -> None:
        self.__exit__(exc_type, exc, tb)


def _block_at(session: Session, model: type[ValidTime]) -> datetime:
    """The enclosing block's time, if the block holds ``model``'s lock."""
    _require_superseding(session, model.__tablename__)
    return session.info[_BLOCK_AT]


def _retire_children(session: Session, model: type[ValidTime], parents: list[Any], at: datetime) -> None:
    """Retire the live children of ``parents`` (rows, or rows returned by ``retire_where``)."""
    if not parents:
        return

    mapper = sa_inspect(model)
    assert mapper is not None
    for name in model.__retire_cascade__:
        relationship = mapper.relationships[name]
        child_model = relationship.mapper.class_
        # A composite foreign key pairs several columns, so match the children on the tuple.
        pairs = relationship.local_remote_pairs
        remote = tuple_(*(remote for _, remote in pairs))
        values = [tuple(getattr(parent, local.key) for local, _ in pairs) for parent in parents]
        for chunk in _chunks(values):
            session.execute(
                update(child_model).where(remote.in_(chunk), child_model.valid_to.is_(None)).values(valid_to=at),
                execution_options={"synchronize_session": False},
            )


def _cascade_targets(models: Iterable[type[ValidTime]]) -> set[type[ValidTime]]:
    """The child tables retiring ``models`` also retires, which a block for them must lock too."""
    targets: set[type[ValidTime]] = set()
    for model in models:
        mapper = sa_inspect(model)
        assert mapper is not None
        targets.update(mapper.relationships[name].mapper.class_ for name in model.__retire_cascade__)
    return targets


def _primary_key(model: type[ValidTime]) -> Any:
    mapper = sa_inspect(model)
    assert mapper is not None and len(mapper.primary_key) == 1, f"{model.__name__} needs a single-column primary key"
    return mapper.primary_key[0]


def _cascade_columns(model: type[ValidTime], primary_key: Any) -> list[Any]:
    """The columns ``retire_where`` returns: the primary key, plus each cascade's parent column."""
    mapper = sa_inspect(model)
    assert mapper is not None
    columns = {primary_key.key: primary_key}
    for name in model.__retire_cascade__:
        for local, _ in mapper.relationships[name].local_remote_pairs:
            columns[local.key] = local
    return list(columns.values())


def _require_superseding(session: Optional[Session], table: str) -> None:
    if session is None or table not in _held_locks(session):
        raise RuntimeError(f"Write to {table} outside superseding(session, ...) for it")


@event.listens_for(ValidTime, "before_insert", propagate=True)
@event.listens_for(ValidTime, "before_update", propagate=True)
def _orm_write_needs_superseding(mapper, connection, target) -> None:
    """ORM writes: ``session.add``, and attribute changes on a row."""
    _require_superseding(object_session(target), mapper.class_.__tablename__)


@event.listens_for(Session, "do_orm_execute")
def _statement_write_needs_superseding(state: ORMExecuteState) -> None:
    """Statements built from a model, including the block's own retirements."""
    if not (state.is_insert or state.is_update):
        return
    mapper = state.bind_mapper
    if mapper is not None and issubclass(mapper.class_, ValidTime):
        _require_superseding(state.session, mapper.class_.__tablename__)
