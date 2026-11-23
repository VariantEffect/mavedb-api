# ruff: noqa: E402

from datetime import datetime, timezone

import pytest

pytest.importorskip("psycopg2")

from sqlalchemy import select
from sqlalchemy.exc import IntegrityError

from mavedb.models.allele import Allele
from mavedb.models.annotation_status import CAID_INDEPENDENT_TYPES, CAID_MATCHED_TYPES, AnnotationStatus
from mavedb.models.enums.annotation_type import AnnotationType
from mavedb.models.enums.disposition import Disposition
from mavedb.models.enums.status_kind import StatusKind
from mavedb.models.mixins.valid_time import superseding
from tests.helpers.util.valid_time import add_versioned

# CHECK constraints run before foreign keys, so a subject id that names no row still reaches them.
UNUSED_VARIANT_ID = 2_000_000_000
"""A variant id for subject-constraint tests that need no real variant."""

OUTAGE_START = datetime(2026, 11, 12, tzinfo=timezone.utc)
"""The failing_since given to failure rows that don't set their own."""


def test_every_annotation_type_is_classified_by_caid_dependence():
    assert sorted(CAID_MATCHED_TYPES + CAID_INDEPENDENT_TYPES) == sorted(t.value for t in AnnotationType)


@pytest.fixture
def allele(session):
    allele = Allele(vrs_digest="test-annotation-status-allele-digest", level="genomic")
    session.add(allele)
    session.commit()
    session.refresh(allele)
    return allele


def _status(
    annotation_type=AnnotationType.GNOMAD_ALLELE_FREQUENCY,
    kind=StatusKind.OUTCOME,
    disposition=Disposition.ABSENT,
    reason="no_record",
    **kw,
):
    if kind is StatusKind.FAILURE:
        kw.setdefault("failing_since", OUTAGE_START)
    return AnnotationStatus(annotation_type=annotation_type, kind=kind, disposition=disposition, reason=reason, **kw)


def _add(session, status):
    add_versioned(session, status)
    session.refresh(status)
    return status


def _expect_rejected(session, status, constraint):
    """Adding `status` must violate `constraint` on flush."""
    with pytest.raises(IntegrityError, match=constraint):
        add_versioned(session, status)


def test_insert_allele_subject_succeeds(session, allele):
    status = _add(session, _status(allele_id=allele.id, source_version="4.1.0"))

    assert status.variant_id is None
    assert status.current


def test_insert_variant_subject_succeeds(session, setup_lib_db_with_variant):
    status = _add(
        session,
        _status(
            AnnotationType.VRS_MAPPING,
            disposition=Disposition.PRESENT,
            reason="mapped",
            variant_id=setup_lib_db_with_variant.id,
        ),
    )

    assert status.allele_id is None
    assert status.current


@pytest.mark.parametrize(
    "annotation_type, subject",
    [
        (AnnotationType.VRS_MAPPING, "allele"),
        (AnnotationType.GNOMAD_ALLELE_FREQUENCY, "variant"),
        (AnnotationType.GNOMAD_ALLELE_FREQUENCY, "neither"),
        (AnnotationType.GNOMAD_ALLELE_FREQUENCY, "both"),
    ],
    ids=["variant-type-on-allele", "allele-type-on-variant", "no-subject", "both-subjects"],
)
def test_insert_wrong_subject_is_rejected(session, allele, annotation_type, subject):
    ids = {
        "allele": {"allele_id": allele.id},
        "variant": {"variant_id": UNUSED_VARIANT_ID},
        "neither": {},
        "both": {"allele_id": allele.id, "variant_id": UNUSED_VARIANT_ID},
    }[subject]

    _expect_rejected(session, _status(annotation_type, **ids), "ck_annotation_status_subject")


@pytest.mark.parametrize(
    "annotation_type, release",
    [(AnnotationType.CLINVAR_CONTROL, None), (AnnotationType.GNOMAD_ALLELE_FREQUENCY, "01_2026")],
    ids=["clinvar-without-release", "gnomad-with-release"],
)
def test_insert_release_on_wrong_type_is_rejected(session, allele, annotation_type, release):
    _expect_rejected(
        session, _status(annotation_type, allele_id=allele.id, release=release), "ck_annotation_status_release"
    )


@pytest.mark.parametrize(
    "kind, disposition",
    [(StatusKind.OUTCOME, Disposition.FAILED), (StatusKind.FAILURE, Disposition.ABSENT)],
    ids=["failed-outcome", "non-failed-failure"],
)
def test_insert_disposition_not_matching_kind_is_rejected(session, allele, kind, disposition):
    _expect_rejected(
        session, _status(kind=kind, disposition=disposition, allele_id=allele.id), "ck_annotation_status_kind"
    )


@pytest.mark.parametrize(
    "kind, disposition, failing_since",
    [(StatusKind.FAILURE, Disposition.FAILED, None), (StatusKind.OUTCOME, Disposition.ABSENT, OUTAGE_START)],
    ids=["failure-without-start", "outcome-with-start"],
)
def test_insert_failing_since_on_wrong_kind_is_rejected(session, allele, kind, disposition, failing_since):
    _expect_rejected(
        session,
        _status(kind=kind, disposition=disposition, allele_id=allele.id, failing_since=failing_since),
        "ck_annotation_status_failing_since",
    )


def test_insert_second_live_row_for_key_is_rejected(session, allele):
    _add(session, _status(allele_id=allele.id))

    _expect_rejected(
        session,
        _status(allele_id=allele.id, disposition=Disposition.PRESENT, reason="created"),
        "uq_annotation_status_live",
    )


def test_insert_live_outcome_and_live_failure_for_key_succeeds(session, allele):
    _add(session, _status(allele_id=allele.id))

    failure = _add(
        session,
        _status(kind=StatusKind.FAILURE, disposition=Disposition.FAILED, reason="api_error", allele_id=allele.id),
    )

    assert failure.current


def test_insert_clinvar_status_per_release_succeeds(session, allele):
    for release in ("01_2025", "01_2026"):
        _add(session, _status(AnnotationType.CLINVAR_CONTROL, allele_id=allele.id, release=release))

    statuses = session.scalars(select(AnnotationStatus).where(AnnotationStatus.allele_id == allele.id)).all()
    assert sorted(s.release for s in statuses) == ["01_2025", "01_2026"]


def test_supersede_keeps_old_outcome_as_history(session, allele):
    old = _add(session, _status(allele_id=allele.id))
    new = _status(allele_id=allele.id, disposition=Disposition.PRESENT, reason="created")

    with superseding(session, AnnotationStatus):
        AnnotationStatus.supersede(session, retiring=[old], inserting=[new])
    session.refresh(old)

    assert not old.current
    assert new.current
    assert old.valid_to == new.valid_from
