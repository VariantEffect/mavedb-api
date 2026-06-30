# ruff: noqa: E402

import pytest

pytest.importorskip("psycopg2")

from mavedb.lib.annotation_status_manager import AnnotationStatusManager
from mavedb.models.allele import Allele
from mavedb.models.enums.annotation_type import AnnotationType
from mavedb.models.enums.disposition import Disposition
from mavedb.models.enums.event_reason import EventReason


@pytest.fixture
def annotation_status_manager(session, job_run):
    return AnnotationStatusManager(session, job_run_id=job_run.id)


@pytest.fixture
def allele(session):
    allele = Allele(vrs_digest="asm-test-allele-digest", level="genomic")
    session.add(allele)
    session.commit()
    session.refresh(allele)
    return allele


class TestRecordEvent:
    def test_records_variant_subject_event(self, session, annotation_status_manager, setup_lib_db_with_variant):
        annotation_status_manager.record_event(
            AnnotationType.LDH_SUBMISSION,
            variant_id=setup_lib_db_with_variant.id,
            disposition=Disposition.PRESENT,
            reason=EventReason.SUBMITTED,
        )
        annotation_status_manager.flush()
        session.commit()

        event = annotation_status_manager.get_current_annotation(
            AnnotationType.LDH_SUBMISSION, variant_id=setup_lib_db_with_variant.id
        )
        assert event is not None
        assert event.disposition == Disposition.PRESENT
        assert event.allele_id is None
        # No current flag exists on the event log.
        assert not hasattr(event, "current")

    def test_records_allele_subject_event_with_metadata(self, session, annotation_status_manager, allele):
        annotation_status_manager.record_event(
            AnnotationType.GNOMAD_ALLELE_FREQUENCY,
            allele_id=allele.id,
            disposition=Disposition.PRESENT,
            reason=EventReason.CREATED,
            source_version="4.1.0",
            metadata={"gnomad_variant_id": "1-55051215-G-A"},
        )
        annotation_status_manager.flush()
        session.commit()

        event = annotation_status_manager.get_current_annotation(
            AnnotationType.GNOMAD_ALLELE_FREQUENCY, allele_id=allele.id
        )
        assert event.event_metadata == {"gnomad_variant_id": "1-55051215-G-A"}
        assert event.source_version == "4.1.0"
        assert event.variant_id is None


class TestAppendOnlyLatestWins:
    def test_latest_event_by_id_is_current(self, session, annotation_status_manager, allele):
        for reason in (EventReason.CREATED, EventReason.RECONFIRMED, EventReason.SKIPPED):
            annotation_status_manager.record_event(
                AnnotationType.GNOMAD_ALLELE_FREQUENCY,
                allele_id=allele.id,
                disposition=Disposition.PRESENT,
                reason=reason,
                source_version="4.1.0",
            )
        annotation_status_manager.flush()
        session.commit()

        # All three events persist (append-only — no retire of prior rows).
        history = annotation_status_manager.get_event_history(
            AnnotationType.GNOMAD_ALLELE_FREQUENCY, allele_id=allele.id
        )
        assert [e.reason for e in history] == [  # newest first
            EventReason.SKIPPED,
            EventReason.RECONFIRMED,
            EventReason.CREATED,
        ]

        # Current = newest by id.
        current = annotation_status_manager.get_current_annotation(
            AnnotationType.GNOMAD_ALLELE_FREQUENCY, allele_id=allele.id
        )
        assert current.reason == EventReason.SKIPPED

    def test_source_version_scopes_current(self, session, annotation_status_manager, allele):
        annotation_status_manager.record_event(
            AnnotationType.GNOMAD_ALLELE_FREQUENCY,
            allele_id=allele.id,
            disposition=Disposition.PRESENT,
            reason=EventReason.CREATED,
            source_version="4.0.0",
        )
        annotation_status_manager.record_event(
            AnnotationType.GNOMAD_ALLELE_FREQUENCY,
            allele_id=allele.id,
            disposition=Disposition.ABSENT,
            reason=EventReason.NO_RECORD,
            source_version="4.1.0",
        )
        annotation_status_manager.flush()
        session.commit()

        v40 = annotation_status_manager.get_current_annotation(
            AnnotationType.GNOMAD_ALLELE_FREQUENCY, allele_id=allele.id, source_version="4.0.0"
        )
        assert v40.disposition == Disposition.PRESENT


class TestSubjectValidation:
    def test_variant_subject_type_with_allele_id_raises(self, annotation_status_manager, allele):
        with pytest.raises(ValueError, match="variant-subject"):
            annotation_status_manager.record_event(
                AnnotationType.VRS_MAPPING,
                allele_id=allele.id,
                disposition=Disposition.PRESENT,
                reason=EventReason.CREATED,
            )

    def test_allele_subject_type_with_variant_id_raises(self, annotation_status_manager, setup_lib_db_with_variant):
        with pytest.raises(ValueError, match="allele-subject"):
            annotation_status_manager.record_event(
                AnnotationType.GNOMAD_ALLELE_FREQUENCY,
                variant_id=setup_lib_db_with_variant.id,
                disposition=Disposition.PRESENT,
                reason=EventReason.CREATED,
            )

    def test_neither_subject_raises(self, annotation_status_manager):
        with pytest.raises(ValueError, match="Exactly one"):
            annotation_status_manager.record_event(
                AnnotationType.VRS_MAPPING,
                disposition=Disposition.PRESENT,
                reason=EventReason.CREATED,
            )

    def test_both_subjects_raises(self, annotation_status_manager, setup_lib_db_with_variant, allele):
        with pytest.raises(ValueError, match="Exactly one"):
            annotation_status_manager.record_event(
                AnnotationType.VRS_MAPPING,
                variant_id=setup_lib_db_with_variant.id,
                allele_id=allele.id,
                disposition=Disposition.PRESENT,
                reason=EventReason.CREATED,
            )


class TestBatching:
    def test_auto_flush_at_batch_size(self, session, job_run, setup_lib_db_with_variant, annotation_status_manager):
        annotation_status_manager.batch_size = 2
        annotation_status_manager.record_event(
            AnnotationType.LDH_SUBMISSION,
            variant_id=setup_lib_db_with_variant.id,
            disposition=Disposition.PRESENT,
            reason=EventReason.SUBMITTED,
        )
        # Not yet flushed (1 < batch_size).
        assert len(annotation_status_manager._pending) == 1
        annotation_status_manager.record_event(
            AnnotationType.LDH_SUBMISSION,
            variant_id=setup_lib_db_with_variant.id,
            disposition=Disposition.PRESENT,
            reason=EventReason.SUBMITTED,
        )
        # Auto-flushed at batch_size=2.
        assert len(annotation_status_manager._pending) == 0
        session.commit()
