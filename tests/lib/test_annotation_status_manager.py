# ruff: noqa: E402

import threading
import time
from datetime import datetime, timezone
from typing import NamedTuple

import pytest

pytest.importorskip("psycopg2")

from sqlalchemy import insert, inspect, select, text
from sqlalchemy.orm import Session

from mavedb.lib.annotation_status_manager import (
    AnnotationStatusManager,
    Status,
    StatusKey,
    _annotation_status_row,
    plan,
)
from mavedb.models.allele import Allele
from mavedb.models.annotation_status import AnnotationStatus
from mavedb.models.enums.annotation_type import AnnotationType
from mavedb.models.enums.disposition import Disposition
from mavedb.models.enums.event_reason import EventReason
from mavedb.models.enums.status_kind import StatusKind

GNOMAD = AnnotationType.GNOMAD_ALLELE_FREQUENCY
"""The single-version allele type most tests report against."""

KEY = StatusKey(GNOMAD, allele_id=1, variant_id=None, release=None)
"""The one key the plan tests report against."""

NOW, EARLIER = (datetime(2026, 11, day, tzinfo=timezone.utc) for day in (18, 12))
"""The flush's transaction time, and the start of an outage already under way."""

UNUSED_VARIANT_ID = 2_000_000_000
"""A variant id for validation tests, which fail before anything reaches the database."""


# plan: what a batch of reports changes, decided without a database


class PlanResult(NamedTuple):
    retire: list[int]
    insert: list[Status]
    keys_changed: int


def _live(row_id, disposition, reason, source_version="4.1.0", failing_since=None):
    return Status(KEY, disposition, reason.value, source_version, failing_since=failing_since, id=row_id)


def _report(disposition, reason, source_version="4.1.0"):
    return Status(KEY, disposition, reason.value, source_version)


def _plan(*reports, outcome=None, failure=None):
    live = {(KEY, row.kind): row for row in (outcome, failure) if row is not None}
    retire, insert, keys_changed = plan(live, list(reports), NOW)
    return PlanResult([row.id for row in retire], insert, keys_changed)


ABSENT = _report(Disposition.ABSENT, EventReason.NO_RECORD)
"""The report most plan tests start from: the source has nothing."""

FAILED = _report(Disposition.FAILED, EventReason.API_ERROR)
"""A check that failed on an upstream error."""


def test_plan_first_outcome_inserts_it():
    result = _plan(ABSENT)

    assert (result.retire, result.insert, result.keys_changed) == ([], [ABSENT], 1)


@pytest.mark.parametrize(
    "repeat",
    [EventReason.NO_RECORD, EventReason.PREEXISTING, EventReason.RECONFIRMED, EventReason.SKIPPED],
    ids=["same-reason", "preexisting", "reconfirmed", "skipped"],
)
def test_plan_unchanged_outcome_writes_nothing(repeat):
    result = _plan(_report(Disposition.ABSENT, repeat), outcome=_live(1, Disposition.ABSENT, EventReason.NO_RECORD))

    assert (result.retire, result.insert, result.keys_changed) == ([], [], 0)


@pytest.mark.parametrize(
    "change",
    [
        _report(Disposition.PRESENT, EventReason.CREATED),
        _report(Disposition.ABSENT, EventReason.MIGRATED),
        _report(Disposition.ABSENT, EventReason.NO_RECORD, source_version="4.2.0"),
    ],
    ids=["new-disposition", "new-reason", "new-source-version"],
)
def test_plan_changed_outcome_supersedes_live_outcome(change):
    result = _plan(change, outcome=_live(1, Disposition.ABSENT, EventReason.NO_RECORD))

    assert (result.retire, result.insert) == ([1], [change])


def test_plan_correction_after_a_correction_supersedes_live_outcome():
    correction = _report(Disposition.PRESENT, EventReason.CORRECTED)
    result = _plan(correction, outcome=_live(1, Disposition.PRESENT, EventReason.CORRECTED))

    assert (result.retire, result.insert) == ([1], [correction])


def test_plan_run_relative_report_keeps_the_change_before_it():
    result = _plan(
        _report(Disposition.PRESENT, EventReason.CORRECTED),
        _report(Disposition.PRESENT, EventReason.RECONFIRMED),
        outcome=_live(1, Disposition.PRESENT, EventReason.CREATED),
    )

    assert result.retire == [1]
    assert [row.reason for row in result.insert] == ["corrected"]


def test_plan_first_outcome_keeps_its_reason_over_a_later_reconfirmation():
    result = _plan(
        _report(Disposition.PRESENT, EventReason.CREATED),
        _report(Disposition.PRESENT, EventReason.RECONFIRMED),
        outcome=_live(1, Disposition.ABSENT, EventReason.NO_RECORD),
    )

    assert [(row.disposition, row.reason) for row in result.insert] == [(Disposition.PRESENT, "created")]


def test_plan_batch_ending_where_it_started_writes_nothing():
    result = _plan(
        _report(Disposition.PRESENT, EventReason.CREATED),
        ABSENT,
        outcome=_live(1, Disposition.ABSENT, EventReason.NO_RECORD),
    )

    assert (result.retire, result.insert, result.keys_changed) == ([], [], 0)


def test_plan_failure_keeps_live_outcome():
    result = _plan(FAILED, outcome=_live(1, Disposition.ABSENT, EventReason.NO_RECORD))

    assert result.retire == []
    [row] = result.insert
    assert (row.kind, row.failing_since) == (StatusKind.FAILURE, NOW)


def test_plan_repeated_failure_with_same_reason_writes_nothing():
    result = _plan(FAILED, failure=_live(7, Disposition.FAILED, EventReason.API_ERROR, failing_since=EARLIER))

    assert (result.retire, result.insert, result.keys_changed) == ([], [], 0)


def test_plan_failure_reason_change_carries_outage_start():
    result = _plan(
        _report(Disposition.FAILED, EventReason.MALFORMED_RESPONSE),
        failure=_live(7, Disposition.FAILED, EventReason.API_ERROR, failing_since=EARLIER),
    )

    assert result.retire == [7]
    [row] = result.insert
    assert (row.reason, row.failing_since) == ("malformed_response", EARLIER)


def test_plan_success_after_failure_retires_failure():
    result = _plan(
        ABSENT,
        outcome=_live(1, Disposition.ABSENT, EventReason.NO_RECORD),
        failure=_live(7, Disposition.FAILED, EventReason.API_ERROR, failing_since=EARLIER),
    )

    assert (result.retire, result.insert, result.keys_changed) == ([7], [], 1)


def test_plan_success_after_first_check_failed_writes_outcome_and_retires_failure():
    result = _plan(ABSENT, failure=_live(7, Disposition.FAILED, EventReason.API_ERROR, failing_since=EARLIER))

    assert (result.retire, result.insert) == ([7], [ABSENT])


def test_plan_recovery_inside_batch_starts_new_outage():
    result = _plan(
        ABSENT,
        FAILED,
        outcome=_live(1, Disposition.ABSENT, EventReason.NO_RECORD),
        failure=_live(7, Disposition.FAILED, EventReason.API_ERROR, failing_since=EARLIER),
    )

    assert result.retire == [7]
    [row] = result.insert
    assert (row.reason, row.failing_since) == (FAILED.reason, NOW)


def test_plan_outcome_then_failure_in_one_batch_writes_both():
    result = _plan(_report(Disposition.PRESENT, EventReason.CREATED), FAILED)

    assert [(row.kind, row.failing_since) for row in result.insert] == [
        (StatusKind.OUTCOME, None),
        (StatusKind.FAILURE, NOW),
    ]
    assert result.keys_changed == 1


@pytest.mark.parametrize(
    "annotation_type, subject, message",
    [
        (AnnotationType.VRS_MAPPING, {"allele_id": 1}, "variant-subject"),
        (GNOMAD, {"variant_id": UNUSED_VARIANT_ID}, "allele-subject"),
        (GNOMAD, {}, "Exactly one"),
        (GNOMAD, {"allele_id": 1, "variant_id": UNUSED_VARIANT_ID}, "Exactly one"),
    ],
    ids=["variant-type-on-allele", "allele-type-on-variant", "no-subject", "both-subjects"],
)
def test_status_key_validated_wrong_subject_raises(annotation_type, subject, message):
    with pytest.raises(ValueError, match=message):
        StatusKey.validated(annotation_type, **subject)


# Drift guards: the manager's types restate parts of AnnotationStatus


def test_status_key_matches_live_unique_index():
    [index] = [index for index in AnnotationStatus.__table__.indexes if index.name == "uq_annotation_status_live"]

    assert {column.name for column in index.columns} == set(StatusKey._fields) | {"kind"}


def test_annotation_status_row_writes_every_insertable_column():
    columns = {attr.key for attr in inspect(AnnotationStatus).column_attrs} - {"id", "valid_from", "valid_to"}

    assert set(_annotation_status_row(ABSENT, job_run_id=None)) == columns


# AnnotationStatusManager: what a flush writes to the database


@pytest.fixture
def manager(session, job_run):
    return AnnotationStatusManager(session, job_run_id=job_run.id)


@pytest.fixture
def allele(session):
    allele = Allele(vrs_digest="asm-test-allele-digest", level="genomic")
    session.add(allele)
    session.commit()
    session.refresh(allele)
    return allele


def _record(manager, allele, disposition, reason, source_version="4.1.0", annotation_type=GNOMAD, **kw):
    manager.report(
        annotation_type,
        allele_id=allele.id,
        disposition=disposition,
        reason=reason,
        source_version=source_version,
        **kw,
    )


def _run(session, manager, allele, *reports, annotation_type=GNOMAD):
    """Report each ``(disposition, reason[, source_version])`` as its own run, written and committed."""
    for report in reports:
        _record(manager, allele, *report, annotation_type=annotation_type)
        manager.flush()
        session.commit()


def _rows(session, allele, annotation_type=GNOMAD):
    """The allele's rows for one type, oldest first."""
    return session.scalars(
        select(AnnotationStatus)
        .where(AnnotationStatus.allele_id == allele.id, AnnotationStatus.annotation_type == annotation_type.value)
        .order_by(AnnotationStatus.id)
    ).all()


def test_flush_writes_report_with_its_run_and_metadata(session, manager, allele, job_run):
    _run(session, manager, allele, (Disposition.ABSENT, EventReason.NO_RECORD))
    _record(
        manager,
        allele,
        Disposition.PRESENT,
        EventReason.CREATED,
        annotation_type=AnnotationType.CLINGEN_ALLELE_ID,
        metadata={"clingen_allele_id": "CA1"},
    )
    manager.flush()
    session.commit()

    [without_metadata] = _rows(session, allele)
    [with_metadata] = _rows(session, allele, AnnotationType.CLINGEN_ALLELE_ID)
    assert with_metadata.status_metadata == {"clingen_allele_id": "CA1"}
    assert session.scalar(
        select(AnnotationStatus.id).where(
            AnnotationStatus.id == without_metadata.id, AnnotationStatus.status_metadata.is_(None)
        )
    ), "a report without metadata is stored as SQL NULL, not JSON null"
    assert {without_metadata.job_run_id, with_metadata.job_run_id} == {job_run.id}


def test_flush_supersede_hands_off_without_a_gap(session, manager, allele):
    _run(
        session,
        manager,
        allele,
        (Disposition.ABSENT, EventReason.NO_RECORD),
        (Disposition.PRESENT, EventReason.CREATED),
    )

    old, new = _rows(session, allele)
    assert not old.current and new.current
    assert old.valid_to == new.valid_from


def test_flush_failure_reason_change_keeps_outage_start(session, manager, allele):
    _run(
        session,
        manager,
        allele,
        (Disposition.FAILED, EventReason.API_ERROR),
        (Disposition.FAILED, EventReason.MALFORMED_RESPONSE),
    )

    first, second = _rows(session, allele)
    assert first.failing_since == first.valid_from
    assert second.failing_since == first.valid_from < second.valid_from


def test_flush_clinvar_keeps_one_status_per_release(session, manager, allele):
    clinvar = AnnotationType.CLINVAR_CONTROL
    _run(
        session,
        manager,
        allele,
        (Disposition.ABSENT, EventReason.NO_RECORD, "01_2025"),
        (Disposition.ABSENT, EventReason.NO_RECORD, "01_2026"),
        (Disposition.ABSENT, EventReason.NO_RECORD, "01_2025"),
        annotation_type=clinvar,
    )

    rows = _rows(session, allele, clinvar)
    assert [(row.release, row.current) for row in rows] == [("01_2025", True), ("01_2026", True)]


def test_report_writes_nothing_until_flush(session, manager, allele):
    _record(manager, allele, Disposition.ABSENT, EventReason.NO_RECORD)
    assert _rows(session, allele) == []

    manager.flush()

    session.commit()

    assert [row.disposition for row in _rows(session, allele)] == ["absent"]


def test_flush_holds_the_status_lock_until_the_commit(session, manager, allele):
    other = Session(bind=session.get_bind())
    try_lock = text("SELECT pg_try_advisory_xact_lock(hashtext('mavedb.annotation_status'))")
    try:
        _record(manager, allele, Disposition.ABSENT, EventReason.NO_RECORD)
        manager.flush()
        assert not other.scalar(try_lock)
        other.rollback()

        session.commit()
        assert other.scalar(try_lock)
    finally:
        other.rollback()
        other.close()


def test_flush_batches_statements(session, manager, allele, monkeypatch):
    # Batches of two make three keys span two reads and two inserts, then two retires on the second run.
    monkeypatch.setattr("mavedb.lib.annotation_status_manager.STATEMENT_CHUNK_SIZE", 2)
    types = [GNOMAD, AnnotationType.CLINGEN_ALLELE_ID, AnnotationType.VEP_FUNCTIONAL_CONSEQUENCE]

    for disposition, reason in [
        (Disposition.ABSENT, EventReason.NO_RECORD),
        (Disposition.PRESENT, EventReason.CREATED),
    ]:
        for annotation_type in types:
            _record(manager, allele, disposition, reason, annotation_type=annotation_type)
        manager.flush()
        session.commit()

    assert [[row.disposition for row in _rows(session, allele, t) if row.current] for t in types] == [["present"]] * 3


def test_flush_waits_for_another_jobs_write(session, allele, monkeypatch):
    # The other job's live status lands while this one waits, so this one plans against it and writes nothing.
    # Without the lock it would read no live row and insert a duplicate.
    monkeypatch.setattr("mavedb.lib.annotation_status_manager.STATUS_LOCK_POLL_SECONDS", 0.05)
    other = Session(bind=session.get_bind())
    waiting = AnnotationStatusManager(Session(bind=session.get_bind()))
    waiting.report(GNOMAD, allele_id=allele.id, disposition=Disposition.ABSENT, reason=EventReason.NO_RECORD)

    try:
        other.execute(text("SELECT pg_advisory_xact_lock(hashtext('mavedb.annotation_status'))"))
        thread = threading.Thread(target=lambda: (waiting.flush(), waiting.session.commit()))
        thread.start()
        time.sleep(0.5)
        assert thread.is_alive()
        live = Status(StatusKey(GNOMAD, allele.id, None, None), Disposition.ABSENT, "no_record")
        other.execute(insert(AnnotationStatus).values([_annotation_status_row(live, None)]))
        other.commit()
        thread.join(timeout=10)
    finally:
        other.close()
        waiting.session.close()

    assert not thread.is_alive()
    assert len(_rows(session, allele)) == 1


def test_current_statuses_reads_allele_subject_keys_per_release(session, manager, allele):
    clinvar = AnnotationType.CLINVAR_CONTROL
    for release in ("01_2025", "01_2026"):
        _record(
            manager, allele, Disposition.ABSENT, EventReason.NO_RECORD, source_version=release, annotation_type=clinvar
        )
    _record(manager, allele, Disposition.PRESENT, EventReason.CREATED)
    manager.flush()
    session.commit()

    rows = manager.current_statuses(clinvar, [allele.id])

    assert sorted(row.release for row in rows) == ["01_2025", "01_2026"]


def test_current_statuses_reads_variant_subject_keys(session, manager, setup_lib_db_with_variant):
    variant = setup_lib_db_with_variant
    manager.report(AnnotationType.VRS_MAPPING, variant_id=variant.id, disposition=Disposition.PRESENT, reason="mapped")
    manager.flush()
    session.commit()

    [row] = manager.current_statuses(AnnotationType.VRS_MAPPING, [variant.id])

    assert (row.variant_id, row.allele_id, row.disposition) == (variant.id, None, "present")


def test_outcomes_at_returns_settled_outcomes_at_that_version(session, manager, allele):
    _record(manager, allele, Disposition.ABSENT, EventReason.NO_RECORD, source_version="4.1.0")
    _record(
        manager,
        allele,
        Disposition.PRESENT,
        EventReason.CREATED,
        source_version="116",
        annotation_type=AnnotationType.VEP_FUNCTIONAL_CONSEQUENCE,
    )
    manager.flush()
    session.commit()

    assert manager.outcomes_at(GNOMAD, [allele.id], "4.1.0") == {allele.id: Disposition.ABSENT}
    assert manager.outcomes_at(GNOMAD, [allele.id], "4.2.0") == {}


def test_outcomes_at_leaves_out_failing_keys(session, manager, allele):
    _run(
        session,
        manager,
        allele,
        (Disposition.ABSENT, EventReason.NO_RECORD),
        (Disposition.FAILED, EventReason.API_ERROR),
    )

    assert manager.outcomes_at(GNOMAD, [allele.id], "4.1.0") == {}


def test_withdraw_retires_live_statuses_of_the_given_types(session, manager, allele):
    _record(manager, allele, Disposition.ABSENT, EventReason.NO_RECORD)
    _record(manager, allele, Disposition.PRESENT, EventReason.CREATED, annotation_type=AnnotationType.CLINGEN_ALLELE_ID)
    manager.flush()
    session.commit()

    manager.withdraw([GNOMAD], [allele.id])
    assert [row.current for row in _rows(session, allele)] == [True]

    manager.flush()
    session.commit()

    assert [row.current for row in _rows(session, allele)] == [False]
    assert [row.current for row in _rows(session, allele, AnnotationType.CLINGEN_ALLELE_ID)] == [True]


def test_flush_applies_withdrawals_before_reports(session, manager, allele):
    _record(manager, allele, Disposition.ABSENT, EventReason.NO_RECORD)
    manager.flush()
    session.commit()

    manager.withdraw([GNOMAD], [allele.id])
    _record(manager, allele, Disposition.ABSENT, EventReason.NO_RECORD)
    manager.flush()
    session.commit()

    # The report lands on a withdrawn key, so it starts a new live status rather than matching the old one.
    assert [row.current for row in _rows(session, allele)] == [False, True]
