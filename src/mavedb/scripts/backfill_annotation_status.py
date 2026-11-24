"""Replay ``annotation_event`` into ``annotation_status`` (#973), so the log can be dropped.

Each pass replays the events past the high-water mark in ``annotation_event_backfill``, through the same
:func:`~mavedb.lib.annotation_status_manager.plan` the jobs flush with, timestamped at each event's
``created_at``. So a reconfirmation writes nothing, a changed outcome supersedes, and a failure keeps its own row,
exactly as if the jobs had always written statuses. Events written in one transaction share a ``created_at`` and
are planned together, as one flush.

Run it with the worker stopped, so no event commits below the mark after a pass reads past it. A pass refuses to
start while events are still being written. The first pass replays the whole log, committing per chunk of subjects.
If it stops partway, rerun with ``--reset``, which empties ``annotation_status`` first. Later passes replay only the
events since, in one transaction.

Two kinds of event change shape on the way:

- ``clinvar_control`` events split, as the ClinVar job now records them: whether the allele's CAID has a ClinVar
  AlleleID becomes one ``clinvar_allele_id`` status per allele, and only an allele with an AlleleID gets a
  ``clinvar_control`` status for the event's release.
- ``mapped_hgvs`` and ``variant_translation`` events are dropped. No job writes either any more.

VEP events keep their Ensembl release as ``source_version``, without the resolver version the job now adds. The
resolver behind an old event is unknown, so the VEP job checks those alleles once more rather than trust it.

Usage:
    poetry run python -m mavedb.scripts.backfill_annotation_status run --commit
    poetry run python -m mavedb.scripts.backfill_annotation_status run --commit --reset
    poetry run python -m mavedb.scripts.backfill_annotation_status status
"""

import logging
from collections import defaultdict
from dataclasses import replace
from datetime import datetime, timedelta
from itertools import groupby, islice
from typing import Any, Iterable, Iterator, NamedTuple, Optional

import asyncclick as click
from sqlalchemy import Interval, func, insert, select, text, type_coerce, update
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.orm import Session

from mavedb.lib.annotation_status_manager import (
    STATEMENT_CHUNK_SIZE,
    LiveRows,
    Status,
    StatusKey,
    _annotation_status_row,
    plan,
)
from mavedb.lib.utils import batched
from mavedb.models.annotation_event import AnnotationEvent, AnnotationEventBackfill
from mavedb.models.annotation_status import AnnotationStatus
from mavedb.models.enums.annotation_type import AnnotationType
from mavedb.models.enums.disposition import Disposition
from mavedb.models.enums.event_reason import EventReason
from mavedb.models.enums.status_kind import StatusKind
from mavedb.scripts.environment import DatabaseSessionAction, script_environment, with_database_session

logger = logging.getLogger(__name__)

DROPPED_TYPES = ("mapped_hgvs", "variant_translation")
"""Event types no status replaces."""

SUBJECTS_PER_CHUNK = 5000
"""Subjects whose events one chunk replays and writes together."""

QUIET_BEFORE_PASS = timedelta(minutes=10)
"""How long no event must have been written before a pass starts, as a sign the worker is stopped."""


class Event(NamedTuple):
    id: int
    annotation_type: str
    variant_id: Optional[int]
    allele_id: Optional[int]
    disposition: str
    reason: str
    source_version: Optional[str]
    metadata: Optional[dict]
    job_run_id: Optional[int]
    created_at: datetime


class Report(NamedTuple):
    """A status an event reports, with when and by which run."""

    at: datetime
    job_run_id: Optional[int]
    status: Status


def event_reports(event: Event, clinvar_allele_ids: dict[int, str]) -> list[Report]:
    """The statuses one event reports. ``clinvar_allele_ids`` holds each allele's last known ClinVar AlleleID, and
    this updates it."""
    disposition = Disposition(event.disposition)

    def report(annotation_type: AnnotationType, disposition: Disposition, reason: str) -> Report:
        source_version = event.source_version if annotation_type is not AnnotationType.CLINVAR_ALLELE_ID else None
        key = StatusKey.validated(
            annotation_type, variant_id=event.variant_id, allele_id=event.allele_id, source_version=source_version
        )
        return Report(
            event.created_at,
            event.job_run_id,
            Status(key, disposition, reason, source_version, event.metadata),
        )

    if event.annotation_type != AnnotationType.CLINVAR_CONTROL.value:
        return [report(AnnotationType(event.annotation_type), disposition, event.reason)]

    clinvar_allele_id = (event.metadata or {}).get("clinvar_allele_id")
    no_allele_id = disposition is Disposition.ABSENT and clinvar_allele_id is None
    if disposition in (Disposition.NOT_APPLICABLE, Disposition.FAILED) or no_allele_id:
        return [report(AnnotationType.CLINVAR_ALLELE_ID, disposition, event.reason)]

    # The ClinVar job's own rule: an AlleleID first seen is created, the same one again preexisting, and a new
    # one corrected. A present event without one was confirmed through its live link, so it already had one.
    assert event.allele_id is not None
    if clinvar_allele_id is None:
        id_reason = EventReason.PREEXISTING
    else:
        known = clinvar_allele_ids.get(event.allele_id)
        id_reason = (
            EventReason.CREATED
            if known is None
            else EventReason.PREEXISTING
            if known == str(clinvar_allele_id)
            else EventReason.CORRECTED
        )
        clinvar_allele_ids[event.allele_id] = str(clinvar_allele_id)

    return [
        report(AnnotationType.CLINVAR_ALLELE_ID, Disposition.PRESENT, id_reason.value),
        report(AnnotationType.CLINVAR_CONTROL, disposition, event.reason),
    ]


class LiveRow(NamedTuple):
    status: Status
    valid_from: datetime


class Replay:
    """Replays reports onto live rows in memory, collecting what to write.

    Rows read from the database keep their ids, and retiring one becomes an update. Rows the replay inserts get
    negative ids until they're written.
    """

    def __init__(self, live: dict[tuple[StatusKey, StatusKind], LiveRow]):
        self.live: dict[StatusKey, LiveRows] = defaultdict(dict)
        self.floor: dict[StatusKey, datetime] = {}
        for (key, kind), row in live.items():
            self.live[key][(key, kind)] = row.status
            self.floor[key] = max(row.valid_from, self.floor.get(key, row.valid_from))
        self.retirements: list[dict[str, Any]] = []
        self.inserts: list[dict[str, Any]] = []

    def apply(self, key_reports: list[Report]) -> None:
        """Replay one key's reports in order, planning each transaction's reports together as one flush."""
        for at, batch in groupby(key_reports, key=lambda report: report.at):
            reports = list(batch)
            # Transactions can commit out of order, so a key's timeline only moves forward.
            key = reports[0].status.key
            now = max(at, self.floor.get(key, at))
            self.floor[key] = now
            self._apply_batch([report.status for report in reports], reports[-1].job_run_id, now)

    def _apply_batch(self, statuses: list[Status], job_run_id: Optional[int], now: datetime) -> None:
        live = self.live[statuses[0].key]
        statuses_to_retire, statuses_to_insert, _ = plan(live, statuses, now)

        for status in statuses_to_retire:
            del live[(status.key, status.kind)]
            assert status.id is not None
            if status.id > 0:
                self.retirements.append({"id": status.id, "valid_to": now})
            else:
                self.inserts[-status.id - 1]["valid_to"] = now

        for status in statuses_to_insert:
            self.inserts.append({**_annotation_status_row(status, job_run_id), "valid_from": now, "valid_to": None})
            live[(status.key, status.kind)] = replace(status, id=-len(self.inserts))


def replay_chunk(db: Session, events: list[Event]) -> tuple[int, int]:
    """Replay one chunk of subjects' events onto their live statuses and write the result. Returns the number of
    rows retired and inserted."""
    live = _live_rows(db, events)
    clinvar_allele_ids: dict[int, str] = {}
    for (key, kind), row in live.items():
        known = (row.status.metadata or {}).get("clinvar_allele_id")
        if key.annotation_type is AnnotationType.CLINVAR_ALLELE_ID and key.allele_id is not None and known is not None:
            clinvar_allele_ids[key.allele_id] = str(known)

    reports_by_key: dict[StatusKey, list[Report]] = defaultdict(list)
    for event in events:
        for report in event_reports(event, clinvar_allele_ids):
            reports_by_key[report.status.key].append(report)

    replay = Replay(live)
    for key_reports in reports_by_key.values():
        replay.apply(key_reports)

    for rows in batched(replay.retirements, STATEMENT_CHUNK_SIZE):
        db.execute(update(AnnotationStatus), list(rows))
    for rows in batched(replay.inserts, STATEMENT_CHUNK_SIZE):
        db.execute(insert(AnnotationStatus).values(list(rows)))

    return len(replay.retirements), len(replay.inserts)


def _live_rows(db: Session, events: list[Event]) -> dict[tuple[StatusKey, StatusKind], LiveRow]:
    """The live statuses of the chunk's subjects, with when each began."""
    variant_ids = sorted({event.variant_id for event in events if event.variant_id is not None})
    allele_ids = sorted({event.allele_id for event in events if event.allele_id is not None})
    live: dict[tuple[StatusKey, StatusKind], LiveRow] = {}
    for column, subject_ids in ((AnnotationStatus.variant_id, variant_ids), (AnnotationStatus.allele_id, allele_ids)):
        for batch in batched(subject_ids, STATEMENT_CHUNK_SIZE):
            for row in db.scalars(select(AnnotationStatus).where(AnnotationStatus.current, column.in_(batch))):
                key = StatusKey(AnnotationType(row.annotation_type), row.allele_id, row.variant_id, row.release)
                status = Status(
                    key,
                    Disposition(row.disposition),
                    row.reason,
                    row.source_version,
                    row.status_metadata,
                    row.failing_since,
                    row.id,
                )
                live[(key, StatusKind(row.kind))] = LiveRow(status, row.valid_from)
    return live


def _event_query(after: int, through: int):
    return select(
        AnnotationEvent.id,
        AnnotationEvent.annotation_type,
        AnnotationEvent.variant_id,
        AnnotationEvent.allele_id,
        AnnotationEvent.disposition,
        AnnotationEvent.reason,
        AnnotationEvent.source_version,
        AnnotationEvent.event_metadata,
        AnnotationEvent.job_run_id,
        AnnotationEvent.created_at,
    ).where(
        AnnotationEvent.id > after,
        AnnotationEvent.id <= through,
        AnnotationEvent.annotation_type.not_in(DROPPED_TYPES),
    )


def first_pass_chunks(db: Session, through: int) -> Iterator[list[Event]]:
    """Every event up to ``through``, a range of subjects at a time, read through the subject indexes."""
    for column in (AnnotationEvent.variant_id, AnnotationEvent.allele_id):
        low, high = db.execute(select(func.min(column), func.max(column))).one()
        if low is None:
            continue
        for start in range(low, high + 1, SUBJECTS_PER_CHUNK):
            query = (
                _event_query(0, through)
                .where(column >= start, column < start + SUBJECTS_PER_CHUNK)
                .order_by(column, AnnotationEvent.annotation_type, AnnotationEvent.id)
            )
            if events := [Event(*row) for row in db.execute(query)]:
                yield events


def catch_up_chunks(db: Session, after: int, through: int) -> Iterator[list[Event]]:
    """The events after ``after``, read by id and grouped into chunks of whole subjects."""
    query = _event_query(after, through).order_by(
        AnnotationEvent.variant_id,
        AnnotationEvent.allele_id,
        AnnotationEvent.annotation_type,
        AnnotationEvent.id,
    )
    events = (Event(*row) for row in db.execute(query))
    subjects = (list(group) for _, group in groupby(events, key=lambda event: (event.variant_id, event.allele_id)))
    while chunk := list(islice(subjects, SUBJECTS_PER_CHUNK)):
        yield [event for subject_events in chunk for event in subject_events]


def backfilled_through(db: Session) -> int:
    return db.scalar(select(AnnotationEventBackfill.through_event_id)) or 0


def run_pass(db: Session, chunks: Iterable[list[Event]], through: int, *, commit_each_chunk: bool) -> None:
    """Replay ``chunks``, then move the mark to ``through``, holding the status lock in each transaction."""
    retired = inserted = 0
    for number, events in enumerate(chunks, start=1):
        _lock_statuses(db)
        chunk_retired, chunk_inserted = replay_chunk(db, events)
        retired, inserted = retired + chunk_retired, inserted + chunk_inserted
        if commit_each_chunk:
            db.commit()
        logger.info(f"Chunk {number}: {len(events)} events; {retired} rows retired, {inserted} inserted so far.")

    _lock_statuses(db)
    db.execute(
        pg_insert(AnnotationEventBackfill)
        .values(id=1, through_event_id=through)
        .on_conflict_do_update(index_elements=["id"], set_={"through_event_id": through})
    )
    db.commit()
    logger.info(f"Backfilled through event {through}: {retired} rows retired, {inserted} inserted.")


def _lock_statuses(db: Session) -> None:
    # Waits rather than polling: nothing else should be writing statuses while the backfill runs.
    db.execute(text("SELECT pg_advisory_xact_lock(hashtext('mavedb.annotation_status'))"))


@script_environment.command(name="run")
@with_database_session(pass_action=True)
@click.option("--reset", is_flag=True, help="Empty annotation_status before a first pass that stopped partway.")
def run(db: Session, action: DatabaseSessionAction, reset: bool) -> None:
    """Replay the events past the mark into annotation_status. Commits as it goes, so needs --commit."""
    if action is not DatabaseSessionAction.COMMIT:
        raise click.UsageError("The backfill commits as it goes, so it has no dry run. Pass --commit.")

    newest_age = db.scalar(select(type_coerce(func.now() - func.max(AnnotationEvent.created_at), Interval)))
    if newest_age is not None and newest_age < QUIET_BEFORE_PASS:
        raise click.UsageError(
            f"The newest event was written {newest_age.total_seconds():.0f} seconds ago, within the last "
            f"{QUIET_BEFORE_PASS.total_seconds() / 60:.0f} minutes. Stop the worker and wait for its jobs to "
            "finish: an event that commits below the mark after the pass reads past it is never replayed."
        )

    after = backfilled_through(db)
    through = db.scalar(select(func.max(AnnotationEvent.id))) or 0
    has_statuses = db.scalar(select(AnnotationStatus.id).limit(1)) is not None

    if after == 0 and has_statuses:
        if not reset:
            raise click.UsageError(
                "annotation_status has rows but no pass has finished. If a first pass stopped partway, rerun with "
                "--reset. If jobs already write statuses, don't: the deploy came before the backfill."
            )
        click.confirm("Delete every annotation_status row and start over?", abort=True)
        db.execute(text("TRUNCATE annotation_status"))
        db.commit()
    elif reset:
        raise click.UsageError("--reset only applies before a first pass finishes.")

    if through <= after:
        logger.info(f"Nothing to do: backfilled through event {after}.")
        return

    if after == 0:
        logger.info(f"First pass: events up to {through}.")
        run_pass(db, first_pass_chunks(db, through), through, commit_each_chunk=True)
    else:
        logger.info(f"Catch-up pass: events {after + 1} to {through}.")
        run_pass(db, catch_up_chunks(db, after, through), through, commit_each_chunk=False)


@script_environment.command(name="status")
@with_database_session
def status(db: Session) -> None:
    """Show how far the backfill has got."""
    after = backfilled_through(db)
    latest = db.scalar(select(func.max(AnnotationEvent.id))) or 0
    remaining = db.scalar(select(func.count(AnnotationEvent.id)).where(AnnotationEvent.id > after)) or 0
    click.echo(f"Backfilled through event {after}; latest event {latest}; {remaining} events to go.")


if __name__ == "__main__":
    script_environment()
