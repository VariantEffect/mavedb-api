# ruff: noqa: E402

import pytest

pytest.importorskip("psycopg2")

from sqlalchemy.exc import IntegrityError

from mavedb.models.allele import Allele
from mavedb.models.annotation_event import AnnotationEvent
from mavedb.models.enums.annotation_type import AnnotationType
from mavedb.models.enums.disposition import Disposition


@pytest.fixture
def allele(session):
    allele = Allele(vrs_digest="test-variant-event-allele-digest", level="genomic")
    session.add(allele)
    session.commit()
    session.refresh(allele)
    return allele


def _expect_rejected(session, event):
    """Adding `event` must violate a CHECK constraint on flush."""
    session.add(event)
    with pytest.raises(IntegrityError):
        session.flush()
    session.rollback()


class TestSubjectConstraint:
    def test_variant_subject_event_inserts(self, session, setup_lib_db_with_variant, job_run):
        event = AnnotationEvent(
            annotation_type=AnnotationType.VRS_MAPPING,
            variant_id=setup_lib_db_with_variant.id,
            disposition=Disposition.PRESENT,
            reason="mapped",
            job_run_id=job_run.id,
            score_set_id=setup_lib_db_with_variant.score_set_id,
        )
        session.add(event)
        session.commit()
        assert event.id is not None
        assert event.allele_id is None

    def test_allele_subject_event_inserts(self, session, allele, job_run):
        event = AnnotationEvent(
            annotation_type=AnnotationType.GNOMAD_ALLELE_FREQUENCY,
            allele_id=allele.id,
            disposition=Disposition.ABSENT,
            reason="no_record",
            source_version="4.1.0",
            job_run_id=job_run.id,
        )
        session.add(event)
        session.commit()
        assert event.id is not None
        assert event.variant_id is None

    def test_variant_subject_type_with_allele_id_rejected(self, session, allele):
        _expect_rejected(
            session,
            AnnotationEvent(
                annotation_type=AnnotationType.VRS_MAPPING,
                allele_id=allele.id,
                disposition=Disposition.PRESENT,
                reason="mapped",
            ),
        )

    def test_allele_subject_type_with_variant_id_rejected(self, session, setup_lib_db_with_variant):
        _expect_rejected(
            session,
            AnnotationEvent(
                annotation_type=AnnotationType.GNOMAD_ALLELE_FREQUENCY,
                variant_id=setup_lib_db_with_variant.id,
                disposition=Disposition.PRESENT,
                reason="created",
            ),
        )

    def test_neither_subject_set_rejected(self, session):
        _expect_rejected(
            session,
            AnnotationEvent(
                annotation_type=AnnotationType.VRS_MAPPING,
                disposition=Disposition.PRESENT,
                reason="mapped",
            ),
        )

    def test_both_subjects_set_rejected(self, session, setup_lib_db_with_variant, allele):
        _expect_rejected(
            session,
            AnnotationEvent(
                annotation_type=AnnotationType.VRS_MAPPING,
                variant_id=setup_lib_db_with_variant.id,
                allele_id=allele.id,
                disposition=Disposition.PRESENT,
                reason="mapped",
            ),
        )
