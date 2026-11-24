# ruff: noqa: E402

from datetime import datetime, timedelta, timezone

import pytest

pytest.importorskip("asyncclick")
pytest.importorskip("psycopg2")

from sqlalchemy import func, select

from mavedb.models.allele import Allele
from mavedb.models.annotation_event import AnnotationEvent
from mavedb.models.annotation_status import AnnotationStatus
from mavedb.models.variant import Variant
from mavedb.scripts.backfill_annotation_status import (
    backfilled_through,
    catch_up_chunks,
    first_pass_chunks,
    run_pass,
)

T0 = datetime(2026, 6, 1, tzinfo=timezone.utc)


def at(day):
    return T0 + timedelta(days=day)


@pytest.fixture
def allele(session):
    allele = Allele(vrs_digest="test-backfill-allele-digest", level="genomic")
    session.add(allele)
    session.commit()
    return allele


def _events(session, *events):
    session.add_all(AnnotationEvent(**event) for event in events)
    session.commit()


def _gnomad(allele, day, disposition="absent", reason="no_record", version="4.1"):
    return dict(
        annotation_type="gnomad_allele_frequency",
        allele_id=allele.id,
        disposition=disposition,
        reason=reason,
        source_version=version,
        created_at=at(day),
    )


def _clinvar(allele, day, release, disposition, reason, **metadata):
    return dict(
        annotation_type="clinvar_control",
        allele_id=allele.id,
        disposition=disposition,
        reason=reason,
        source_version=release,
        event_metadata={"clingen_allele_id": "CA1", **metadata},
        created_at=at(day),
    )


def _backfill(session):
    after = backfilled_through(session)
    through = session.scalar(select(func.max(AnnotationEvent.id)))
    if after == 0:
        run_pass(session, first_pass_chunks(session, through), through, commit_each_chunk=True)
    else:
        run_pass(session, catch_up_chunks(session, after, through), through, commit_each_chunk=False)


def _rows(session, allele, annotation_type="gnomad_allele_frequency"):
    """``(kind, disposition, reason, release, valid_from, valid_to)`` per row, oldest first."""
    rows = session.scalars(
        select(AnnotationStatus)
        .where(AnnotationStatus.allele_id == allele.id, AnnotationStatus.annotation_type == annotation_type)
        .order_by(AnnotationStatus.valid_from, AnnotationStatus.id)
    )
    return [(r.kind, r.disposition, r.reason, r.release, r.valid_from, r.valid_to) for r in rows]


@pytest.mark.integration
class TestBackfill:
    def test_rechecks_that_change_nothing_write_nothing(self, session, allele):
        _events(
            session,
            _gnomad(allele, 0),
            _gnomad(allele, 1, reason="skipped"),
            _gnomad(allele, 2),
        )

        _backfill(session)

        assert _rows(session, allele) == [("outcome", "absent", "no_record", None, at(0), None)]

    def test_a_changed_outcome_supersedes_at_the_events_time(self, session, allele):
        _events(session, _gnomad(allele, 0), _gnomad(allele, 3, version="4.2"))

        _backfill(session)

        assert [(row[4], row[5]) for row in _rows(session, allele)] == [(at(0), at(3)), (at(3), None)]

    def test_an_outage_keeps_its_start_until_a_check_succeeds(self, session, allele):
        _events(
            session,
            _gnomad(allele, 0),
            _gnomad(allele, 1, disposition="failed", reason="api_error"),
            _gnomad(allele, 2, disposition="failed", reason="malformed_response"),
            _gnomad(allele, 4),
        )

        _backfill(session)

        failures = session.scalars(
            select(AnnotationStatus.failing_since).where(AnnotationStatus.kind == "failure")
        ).all()
        assert failures == [at(1), at(1)]
        assert [row[:2] + row[4:] for row in _rows(session, allele)] == [
            ("outcome", "absent", at(0), None),
            ("failure", "failed", at(1), at(2)),
            ("failure", "failed", at(2), at(4)),
        ]

    def test_clinvar_events_split_into_one_allele_id_status_and_one_status_per_release(self, session, allele):
        _events(
            session,
            _clinvar(allele, 0, "01_2025", "present", "created", clinvar_allele_id=111),
            _clinvar(allele, 0, "01_2026", "absent", "no_record", clinvar_allele_id=111),
            _clinvar(allele, 5, "01_2025", "present", "preexisting"),
            _clinvar(allele, 6, "01_2026", "present", "created", clinvar_allele_id=222),
        )

        _backfill(session)

        assert [row[1:3] + row[4:] for row in _rows(session, allele, "clinvar_allele_id")] == [
            ("present", "created", at(0), at(6)),
            ("present", "corrected", at(6), None),
        ]
        assert [row[1:5] for row in _rows(session, allele, "clinvar_control") if row[5] is None] == [
            ("present", "created", "01_2025", at(0)),
            ("present", "created", "01_2026", at(6)),
        ]

    def test_an_allele_without_an_allele_id_gets_no_release_status(self, session, allele):
        _events(session, _clinvar(allele, 0, "01_2025", "absent", "no_record"))

        _backfill(session)

        assert [row[1:3] for row in _rows(session, allele, "clinvar_allele_id")] == [("absent", "no_record")]
        assert _rows(session, allele, "clinvar_control") == []

    def test_drops_legacy_types(self, session, allele, make_score_set):
        variant = Variant(urn="urn:mavedb:00000001-a-1#1", score_set_id=make_score_set().id, hgvs_nt="c.1A>G", data={})
        session.add(variant)
        session.commit()
        _events(
            session,
            dict(annotation_type="mapped_hgvs", allele_id=allele.id, disposition="present", reason="migrated"),
            dict(annotation_type="variant_translation", variant_id=variant.id, disposition="present", reason="x"),
            dict(annotation_type="vrs_mapping", variant_id=variant.id, disposition="present", reason="migrated"),
        )

        _backfill(session)

        assert session.scalars(select(AnnotationStatus.annotation_type)).all() == ["vrs_mapping"]

    def test_a_catch_up_pass_continues_from_the_live_statuses(self, session, allele):
        _events(session, _gnomad(allele, 0))
        _backfill(session)
        first_mark = backfilled_through(session)

        _events(
            session, _gnomad(allele, 2, reason="skipped"), _gnomad(allele, 3, disposition="present", reason="created")
        )
        _backfill(session)

        assert backfilled_through(session) > first_mark
        assert [row[1:3] + row[4:] for row in _rows(session, allele)] == [
            ("absent", "no_record", at(0), at(3)),
            ("present", "created", at(3), None),
        ]
