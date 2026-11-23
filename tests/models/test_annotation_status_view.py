# ruff: noqa: E402
"""Tests for the v_annotation_status_current view (each key's live outcome paired with its live failure)."""

from datetime import datetime, timezone

import pytest

pytest.importorskip("psycopg2")

from sqlalchemy import select

from mavedb.models.allele import Allele
from mavedb.models.annotation_status import AnnotationStatus
from mavedb.models.annotation_status_view import CurrentAnnotationStatusView
from mavedb.models.enums.annotation_type import AnnotationType
from mavedb.models.enums.disposition import Disposition
from mavedb.models.enums.status_kind import StatusKind
from mavedb.models.mixins.valid_time import superseding
from tests.helpers.util.valid_time import add_versioned

OUTAGE_START = datetime(2026, 11, 12, tzinfo=timezone.utc)
"""The failing_since given to failure rows, earlier than any row written during a test."""


@pytest.fixture
def allele(session):
    allele = Allele(vrs_digest="test-annotation-status-view-digest", level="genomic")
    session.add(allele)
    session.commit()
    session.refresh(allele)
    return allele


def _add(session, allele, kind, disposition, reason, annotation_type=AnnotationType.GNOMAD_ALLELE_FREQUENCY, **kw):
    if kind is StatusKind.FAILURE:
        kw.setdefault("failing_since", OUTAGE_START)
    status = AnnotationStatus(
        annotation_type=annotation_type,
        kind=kind,
        disposition=disposition,
        reason=reason,
        allele_id=allele.id,
        **kw,
    )
    add_versioned(session, status)
    session.refresh(status)
    return status


def _current(session, allele):
    return session.scalars(
        select(CurrentAnnotationStatusView)
        .where(CurrentAnnotationStatusView.allele_id == allele.id)
        .order_by(CurrentAnnotationStatusView.release)
    ).all()


def test_view_outcome_and_failure_share_one_row(session, allele):
    outcome = _add(
        session,
        allele,
        StatusKind.OUTCOME,
        Disposition.ABSENT,
        "no_record",
        source_version="4.1.0",
        status_metadata={"clingen_allele_id": "CA1"},
    )
    failure = _add(
        session,
        allele,
        StatusKind.FAILURE,
        Disposition.FAILED,
        "api_error",
        status_metadata={"error_message": "timeout"},
    )

    [row] = _current(session, allele)

    assert (row.disposition, row.reason, row.source_version) == ("absent", "no_record", "4.1.0")
    assert row.status_metadata == {"clingen_allele_id": "CA1"}
    assert row.established_at == outcome.valid_from
    # The outage began before this failure row, whose reason replaced an earlier one.
    assert failure.valid_from > OUTAGE_START
    assert (row.failing_since, row.failure_reason) == (OUTAGE_START, "api_error")
    assert row.failure_metadata == {"error_message": "timeout"}


def test_view_failure_without_outcome_has_no_outcome_columns(session, allele):
    _add(session, allele, StatusKind.FAILURE, Disposition.FAILED, "api_error")

    [row] = _current(session, allele)

    assert row.disposition is None and row.established_at is None
    assert row.failure_reason == "api_error"


def test_view_excludes_retired_rows(session, allele):
    old = _add(session, allele, StatusKind.OUTCOME, Disposition.ABSENT, "no_record")
    new = AnnotationStatus(
        annotation_type=AnnotationType.GNOMAD_ALLELE_FREQUENCY,
        kind=StatusKind.OUTCOME,
        disposition=Disposition.PRESENT,
        reason="created",
        allele_id=allele.id,
    )
    with superseding(session, AnnotationStatus):
        AnnotationStatus.supersede(session, retiring=[old], inserting=[new])

    [row] = _current(session, allele)

    assert row.disposition == "present"
    assert row.failure_reason is None


def test_view_keeps_one_row_per_annotation_type_for_one_allele(session, allele):
    # Both keys have NULL variant_id and release, so the ORM must not collapse them into one identity.
    _add(session, allele, StatusKind.OUTCOME, Disposition.ABSENT, "no_record")
    _add(
        session,
        allele,
        StatusKind.OUTCOME,
        Disposition.ABSENT,
        "no_record",
        annotation_type=AnnotationType.CLINGEN_ALLELE_ID,
    )

    assert {row.annotation_type for row in _current(session, allele)} == {
        AnnotationType.GNOMAD_ALLELE_FREQUENCY.value,
        AnnotationType.CLINGEN_ALLELE_ID.value,
    }


def test_view_keeps_one_row_per_clinvar_release(session, allele):
    for release in ("01_2025", "01_2026"):
        _add(
            session,
            allele,
            StatusKind.OUTCOME,
            Disposition.ABSENT,
            "no_record",
            annotation_type=AnnotationType.CLINVAR_CONTROL,
            release=release,
        )

    assert [row.release for row in _current(session, allele)] == ["01_2025", "01_2026"]
