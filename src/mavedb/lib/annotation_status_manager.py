"""Writer for ``annotation_status``: records what each pipeline check found, keeping only changes.

Jobs report every check through :meth:`AnnotationStatusManager.report`. On
:meth:`~AnnotationStatusManager.flush` the manager reads the reported keys' live rows and :func:`plan`
decides what changes (#973):

- A report that changes nothing writes nothing, so re-running a job over unchanged subjects adds no rows.
- A changed outcome supersedes the live outcome row, which stays as history.
- A failure never replaces an outcome. It gets its own live row, retired when a later check succeeds.

A key is the subject, the annotation type and, for ClinVar, the release. Each subject is a variant or an
allele, chosen by the annotation type, and the manager checks that pairing up front so a mis-subjected
report fails with a clear ``ValueError`` rather than a CHECK violation when it's written.
"""

import logging
import time
from collections import defaultdict
from dataclasses import dataclass, replace
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Iterable, NamedTuple, Optional, cast

from sqlalchemy import CursorResult, func, insert, or_, select, text
from sqlalchemy.orm import Session

from mavedb.lib.logging.context import increment_logging_context, logging_context, save_to_logging_context
from mavedb.lib.utils import batched
from mavedb.models.annotation_status import (
    ALLELE_SUBJECT_TYPES,
    RELEASE_KEYED_TYPES,
    VARIANT_SUBJECT_TYPES,
    AnnotationStatus,
)
from mavedb.models.annotation_status_view import CurrentAnnotationStatusView
from mavedb.models.enums.annotation_type import AnnotationType
from mavedb.models.enums.disposition import Disposition
from mavedb.models.enums.event_reason import EventReason
from mavedb.models.enums.status_kind import StatusKind

logger = logging.getLogger(__name__)

STATEMENT_CHUNK_SIZE = 1000
"""Rows per statement, so we can keep statements under Postgres's 65,535 bind parameter limit."""

STATUS_LOCK_POLL_SECONDS = 0.1
"""How often :meth:`AnnotationStatusManager.flush` retries the status lock while another job holds it."""

STATUS_MAX_WAIT = timedelta(seconds=60)
"""How long :meth:`AnnotationStatusManager.flush` waits for another job's status write to commit. The wait
sleeps the worker's thread, but status writes are short, so a longer wait means the other job is stuck."""

RUN_RELATIVE_REASONS = frozenset(
    {
        EventReason.PREEXISTING.value,
        EventReason.RECONFIRMED.value,
        EventReason.SKIPPED.value,
    }
)
"""Event reasons that never change a status."""


def _value(member: Any) -> Any:
    """A reason's string value, whichever vocabulary's enum it comes from."""
    return member.value if isinstance(member, Enum) else member


class StatusKey(NamedTuple):
    """Which status a :class:`Status` belongs to. A key can have a live outcome and a live failure at once, so the
    live unique index adds ``kind`` to these columns.

    Kept apart from ``Status`` so it can be hashed.
    """

    annotation_type: AnnotationType
    allele_id: Optional[int]
    variant_id: Optional[int]
    release: Optional[str]

    @classmethod
    def validated(
        cls,
        annotation_type: AnnotationType,
        *,
        variant_id: Optional[int] = None,
        allele_id: Optional[int] = None,
        source_version: Optional[str] = None,
    ) -> "StatusKey":
        """The key a check reports against. Raises ``ValueError`` if the subject doesn't match the type.

        Exactly one of ``variant_id`` and ``allele_id`` is the subject, as ``annotation_type`` dictates.
        ``source_version`` is part of the key only for :data:`RELEASE_KEYED_TYPES`, as the release.
        """
        type_value = annotation_type.value

        if (variant_id is None) == (allele_id is None):
            raise ValueError("Exactly one of variant_id or allele_id must be set")
        if variant_id is not None and type_value not in VARIANT_SUBJECT_TYPES:
            raise ValueError(f"annotation_type {type_value!r} has an allele-subject; pass allele_id, not variant_id")
        if allele_id is not None and type_value not in ALLELE_SUBJECT_TYPES:
            raise ValueError(f"annotation_type {type_value!r} has a variant-subject; pass variant_id, not allele_id")

        release = source_version if type_value in RELEASE_KEYED_TYPES else None
        return cls(annotation_type, allele_id, variant_id, release)


@dataclass(frozen=True)
class Status:
    """A key's status, at any stage: a job's report, a live row, or a row to insert.

    ``id`` is set only on a live row. ``failing_since`` is set only on a failure.
    """

    key: StatusKey
    disposition: Disposition
    reason: str
    source_version: Optional[str] = None
    metadata: Optional[dict] = None
    failing_since: Optional[datetime] = None
    id: Optional[int] = None

    @property
    def failed(self) -> bool:
        return self.disposition is Disposition.FAILED

    @property
    def kind(self) -> StatusKind:
        return StatusKind.for_disposition(self.disposition)


LiveRows = dict[tuple[StatusKey, StatusKind], Status]
"""Live rows by key and kind."""


def plan(live: LiveRows, reports: list[Status], now: datetime) -> tuple[list[Status], list[Status], int]:
    """Decide what a batch of reports changes: the live rows to retire, the rows to insert, and how many keys
    changed. See :func:`_plan_key` for the detailed rules for folding reports and handling failures."""
    by_key: dict[StatusKey, list[Status]] = defaultdict(list)
    for report in reports:
        by_key[report.key].append(report)

    retire: list[Status] = []
    insert: list[Status] = []
    keys_changed = 0
    for key, key_reports in by_key.items():
        key_retire, key_insert = _plan_key(
            live.get((key, StatusKind.OUTCOME)), live.get((key, StatusKind.FAILURE)), key_reports, now
        )
        retire.extend(key_retire)
        insert.extend(key_insert)
        keys_changed += bool(key_retire or key_insert)

    return retire, insert, keys_changed


def _changes_outcome(live: Status, reported: Status) -> bool:
    if reported.disposition != live.disposition or reported.source_version != live.source_version:
        return True
    # Jobs report a correction only when the stored value changed, which lives in metadata, so a second correction
    # in a row is still a change.
    if reported.reason == EventReason.CORRECTED.value:
        return True

    return reported.reason not in RUN_RELATIVE_REASONS and reported.reason != live.reason


def _plan_key(
    live_outcome: Optional[Status], live_failure: Optional[Status], reports: list[Status], now: datetime
) -> tuple[list[Status], list[Status]]:
    """The live rows one key's reports retire, and the rows they insert.

    The successful reports are folded in order onto the live outcome, each compared with the outcome the one before
    it left, so a run-relative report never hides the change before it. The result supersedes the live outcome only
    if it differs from it.

    If the last report failed and nothing in the batch succeeded its failure row is kept when the reason matches, and
    superseded with ``failing_since`` carried forward when it doesn't. A failure after a success in the batch, or with
    no live failure, starts a new outage at ``now``. If the last report succeeded, any live failure is retired.
    """
    retire: list[Status] = []
    insert: list[Status] = []

    outcome = live_outcome
    for report in reports:
        if not report.failed and (outcome is None or _changes_outcome(outcome, report)):
            outcome = report

    if outcome is not None and outcome is not live_outcome:
        if live_outcome is None or _changes_outcome(live_outcome, outcome):
            if live_outcome is not None:
                retire.append(live_outcome)
            insert.append(outcome)

    final = reports[-1]
    if final.failed:
        continuing = live_failure if all(report.failed for report in reports) else None
        if continuing is None or continuing.reason != final.reason:
            if live_failure is not None:
                retire.append(live_failure)
            insert.append(replace(final, failing_since=continuing.failing_since if continuing is not None else now))

    elif live_failure is not None:
        retire.append(live_failure)

    return retire, insert


def _annotation_status_row(status: Status, job_run_id: Optional[int]) -> dict[str, Any]:
    return {
        "annotation_type": status.key.annotation_type.value,
        "allele_id": status.key.allele_id,
        "variant_id": status.key.variant_id,
        "release": status.key.release,
        "kind": status.kind.value,
        "disposition": status.disposition.value,
        "reason": status.reason,
        "source_version": status.source_version,
        "status_metadata": status.metadata,
        "failing_since": status.failing_since,
        "job_run_id": job_run_id,
    }


class AnnotationStatusManager:
    """Buffered writer that keeps ``annotation_status`` in step with what checks found.

    Reports and withdrawals accumulate in memory until :meth:`flush` writes them. The manager never commits: the
    caller owns the transaction, and commits right after each flush. Flush at the job's batch boundaries and after the
    last :meth:`report`; anything never flushed is lost with the manager.

    Each flush adds to counters in the open logging context, so a job's canonical log shows what its checks changed:
    ``annotation_status.keys_changed`` and ``annotation_status.keys_unchanged`` for reported keys,
    ``annotation_status.withdrawn`` for live statuses withdrawals retired, and ``annotation_status.source_versions``
    for the source versions reported.
    """

    def __init__(self, session: Session, job_run_id: Optional[int] = None):
        self.session = session
        self.job_run_id = job_run_id
        self._pending_reports: list[Status] = []
        self._pending_withdrawals: defaultdict[AnnotationType, set[int]] = defaultdict(set)

    def report(
        self,
        annotation_type: AnnotationType,
        *,
        disposition: Disposition,
        reason: str,
        variant_id: Optional[int] = None,
        allele_id: Optional[int] = None,
        source_version: Optional[str] = None,
        metadata: Optional[dict] = None,
    ) -> None:
        """Buffer the result of one check. :meth:`flush` stores it only if it changes the key's status.

        The subject and ``source_version`` form the key as :meth:`StatusKey.validated` describes, and a subject that
        doesn't match ``annotation_type`` raises ``ValueError``.
        """
        key = StatusKey.validated(
            annotation_type, variant_id=variant_id, allele_id=allele_id, source_version=source_version
        )
        self._pending_reports.append(Status(key, Disposition(disposition), _value(reason), source_version, metadata))

    def withdraw(self, annotation_types: Iterable[AnnotationType | str], allele_ids: Iterable[int]) -> None:
        """Buffer retiring the live statuses of ``allele_ids`` for ``annotation_types``, so their next check starts as
        if they were never checked. For results a change elsewhere has invalidated, like everything matched through an
        allele's replaced CAID. :meth:`flush` retires them before writing any reports.
        """
        allele_ids = set(allele_ids)
        for annotation_type in annotation_types:
            self._pending_withdrawals[AnnotationType(annotation_type)] |= allele_ids

    def flush(self) -> None:
        """Write the pending withdrawals, then what the pending reports change. The caller owns the transaction:
        commit right after.

        Takes the status lock before reading the live statuses and holds it until the caller's commit, so no other
        job writes between that read and this write. Until then, other jobs' flushes wait, so don't do other work,
        and especially don't ``await``, between flushing and committing. Does nothing when nothing is pending.
        """
        if not self._pending_reports and not self._pending_withdrawals:
            return

        self._lock_statuses()
        now = self.session.scalars(select(func.now())).one()
        self._write_withdrawals(now)
        self._write_reports(now)

    def _write_withdrawals(self, now: datetime) -> None:
        for annotation_type, allele_ids in sorted(self._pending_withdrawals.items()):
            for batch in batched(sorted(allele_ids), STATEMENT_CHUNK_SIZE):
                result = cast(
                    CursorResult,
                    self.session.execute(
                        AnnotationStatus.retire_live_where(
                            AnnotationStatus.annotation_type == annotation_type.value,
                            AnnotationStatus.allele_id.in_(batch),
                            at=now,
                        )
                    ),
                )
                increment_logging_context("annotation_status.withdrawn", result.rowcount)

        self._pending_withdrawals.clear()

    def _write_reports(self, now: datetime) -> None:
        if not self._pending_reports:
            return

        reported_keys = {report.key for report in self._pending_reports}
        live_statuses = self._live_statuses(reported_keys)
        statuses_to_retire, statuses_to_insert, keys_changed = plan(live_statuses, self._pending_reports, now)

        # Retiring and inserting at the same `now` keeps each handoff gap-free, as the ValidTime mixin does.
        for ids in batched([status.id for status in statuses_to_retire], STATEMENT_CHUNK_SIZE):
            self.session.execute(AnnotationStatus.retire_live_where(AnnotationStatus.id.in_(ids), at=now))
        for statuses in batched(statuses_to_insert, STATEMENT_CHUNK_SIZE):
            rows = [{**_annotation_status_row(status, self.job_run_id), "valid_from": now} for status in statuses]
            self.session.execute(insert(AnnotationStatus).values(rows))

        increment_logging_context("annotation_status.keys_changed", keys_changed)
        increment_logging_context("annotation_status.keys_unchanged", len(reported_keys) - keys_changed)
        source_versions = {report.source_version for report in self._pending_reports if report.source_version}
        if source_versions:
            source_versions |= set(logging_context().get("annotation_status.source_versions", []))
            save_to_logging_context({"annotation_status.source_versions": sorted(source_versions)}, overwrite=True)

        logger.debug(f"Wrote {len(self._pending_reports)} annotation status reports: {keys_changed} keys changed")
        self._pending_reports.clear()

    def outcomes_at(
        self, annotation_type: AnnotationType, subject_ids: Iterable[int], source_version: str
    ) -> dict[int, Disposition]:
        """The settled outcome of each subject checked at ``source_version``: its disposition, for keys whose live
        outcome is at that version and whose checks aren't failing. A job skips these, since checking the same
        source version again can't change them."""
        return {
            row.variant_id if row.variant_id is not None else row.allele_id: Disposition(row.disposition)
            for row in self.current_statuses(annotation_type, subject_ids)
            if row.disposition is not None and row.source_version == source_version and row.failing_since is None
        }

    def _lock_statuses(self) -> None:
        """Take the transaction's status lock, which the caller's commit releases.

        Retries rather than waiting in Postgres, so a stuck holder raises after :data:`STATUS_MAX_WAIT` instead of
        hanging. The retries sleep the worker's thread, but status writes are short.
        """
        deadline = time.monotonic() + STATUS_MAX_WAIT.total_seconds()
        while not self.session.scalar(text("SELECT pg_try_advisory_xact_lock(hashtext('mavedb.annotation_status'))")):
            if time.monotonic() >= deadline:
                raise TimeoutError(f"Timed out after {STATUS_MAX_WAIT} waiting for another job's status write")
            time.sleep(STATUS_LOCK_POLL_SECONDS)

    def current_statuses(
        self, annotation_type: AnnotationType, subject_ids: Iterable[int]
    ) -> list[CurrentAnnotationStatusView]:
        """The current status of each subject for ``annotation_type``: one row per key, so one per release for a
        release-keyed type. ``subject_ids`` are variant or allele ids, whichever the type keys on. Keys never
        reported have no row."""
        annotation_type = AnnotationType(annotation_type)
        view = CurrentAnnotationStatusView
        subject = view.variant_id if annotation_type.value in VARIANT_SUBJECT_TYPES else view.allele_id
        rows: list[CurrentAnnotationStatusView] = []
        for batch in batched(sorted(subject_ids), STATEMENT_CHUNK_SIZE):
            rows.extend(
                self.session.scalars(
                    select(view).where(view.annotation_type == annotation_type.value, subject.in_(batch))
                )
            )

        return rows

    def _live_statuses(self, keys: set[StatusKey]) -> LiveRows:
        """The live statuses for ``keys``, read in batches that keep each query under the bind parameter limit."""
        live_statuses: LiveRows = {}
        for batch in batched(list(keys), STATEMENT_CHUNK_SIZE):
            live_statuses.update(self._live_statuses_batch(set(batch)))

        return live_statuses

    def _live_statuses_batch(self, keys: set[StatusKey]) -> LiveRows:
        allele_ids = {key.allele_id for key in keys if key.allele_id is not None}
        variant_ids = {key.variant_id for key in keys if key.variant_id is not None}
        # A superset: types and subjects aren't paired, and release isn't filtered. Every key has NULL columns, so
        # an exact match needs IS NOT DISTINCT FROM and can't use the subject indexes. In practice there are few
        # extra rows since most jobs report one type, and ClinVar adds other releases.
        candidates = self.session.execute(
            select(
                AnnotationStatus.annotation_type,
                AnnotationStatus.allele_id,
                AnnotationStatus.variant_id,
                AnnotationStatus.release,
                AnnotationStatus.kind,
                AnnotationStatus.id,
                AnnotationStatus.disposition,
                AnnotationStatus.reason,
                AnnotationStatus.source_version,
                AnnotationStatus.failing_since,
            ).where(
                AnnotationStatus.current,
                AnnotationStatus.annotation_type.in_({key.annotation_type.value for key in keys}),
                or_(AnnotationStatus.allele_id.in_(allele_ids), AnnotationStatus.variant_id.in_(variant_ids)),
            )
        )

        # Filter the extra rows from the superset query down to the requested keys.
        live_statuses: LiveRows = {}
        for candidate in candidates:
            key = StatusKey(
                AnnotationType(candidate.annotation_type), candidate.allele_id, candidate.variant_id, candidate.release
            )
            if key in keys:
                live_statuses[(key, StatusKind(candidate.kind))] = Status(
                    key,
                    Disposition(candidate.disposition),
                    candidate.reason,
                    candidate.source_version,
                    failing_since=candidate.failing_since,
                    id=candidate.id,
                )

        return live_statuses
