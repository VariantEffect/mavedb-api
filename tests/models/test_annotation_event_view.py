# ruff: noqa: E402
"""Tests for the v_current_annotation_events view (current event per subject + type)."""

import pytest

pytest.importorskip("psycopg2")

from sqlalchemy import select

from mavedb.models.allele import Allele
from mavedb.models.annotation_event import AnnotationEvent
from mavedb.models.annotation_event_view import CurrentAnnotationEventView
from mavedb.models.enums.annotation_type import AnnotationType
from mavedb.models.enums.disposition import Disposition


def _allele(session, digest, level="genomic"):
    allele = Allele(vrs_digest=digest, level=level, post_mapped={"type": "Allele"})
    session.add(allele)
    session.commit()
    session.refresh(allele)
    return allele


def _event(session, job_run, annotation_type, disposition, reason, *, allele_id=None, variant_id=None, **kw):
    event = AnnotationEvent(
        annotation_type=annotation_type,
        allele_id=allele_id,
        variant_id=variant_id,
        disposition=disposition,
        reason=reason,
        job_run_id=job_run.id,
        **kw,
    )
    session.add(event)
    session.commit()
    session.refresh(event)
    return event


def _view_rows(session, **filters):
    stmt = select(CurrentAnnotationEventView)
    for col, val in filters.items():
        stmt = stmt.where(getattr(CurrentAnnotationEventView, col) == val)
    return list(session.scalars(stmt).all())


def test_returns_only_latest_event_per_subject_and_type(session, job_run):
    allele = _allele(session, "view-latest")
    _event(
        session, job_run, AnnotationType.GNOMAD_ALLELE_FREQUENCY, Disposition.ABSENT, "no_record", allele_id=allele.id
    )
    latest = _event(
        session, job_run, AnnotationType.GNOMAD_ALLELE_FREQUENCY, Disposition.PRESENT, "created", allele_id=allele.id
    )

    rows = _view_rows(session, allele_id=allele.id, annotation_type=AnnotationType.GNOMAD_ALLELE_FREQUENCY.value)

    assert [r.id for r in rows] == [latest.id]
    assert rows[0].disposition == Disposition.PRESENT


def test_clinvar_is_multi_live_one_row_per_release(session, job_run):
    allele = _allele(session, "view-clinvar")
    e_2025 = _event(
        session,
        job_run,
        AnnotationType.CLINVAR_CONTROL,
        Disposition.PRESENT,
        "created",
        allele_id=allele.id,
        source_version="01_2025",
    )
    _event(
        session,
        job_run,
        AnnotationType.CLINVAR_CONTROL,
        Disposition.PRESENT,
        "created",
        allele_id=allele.id,
        source_version="01_2026",
    )
    e_2026_latest = _event(
        session,
        job_run,
        AnnotationType.CLINVAR_CONTROL,
        Disposition.PRESENT,
        "superseded",
        allele_id=allele.id,
        source_version="01_2026",
    )

    rows = _view_rows(session, allele_id=allele.id, annotation_type=AnnotationType.CLINVAR_CONTROL.value)

    by_version = {r.source_version: r.id for r in rows}
    assert by_version == {"01_2025": e_2025.id, "01_2026": e_2026_latest.id}


def test_non_clinvar_collapses_across_versions(session, job_run):
    """gnomAD/VEP supersede to a single current state: a re-fetch at a new source_version yields one
    row, not one per version (the CASE folds source_version in only for ClinVar)."""
    allele = _allele(session, "view-gnomad-version")
    _event(
        session,
        job_run,
        AnnotationType.GNOMAD_ALLELE_FREQUENCY,
        Disposition.PRESENT,
        "created",
        allele_id=allele.id,
        source_version="4.0.0",
    )
    latest = _event(
        session,
        job_run,
        AnnotationType.GNOMAD_ALLELE_FREQUENCY,
        Disposition.PRESENT,
        "reconfirmed",
        allele_id=allele.id,
        source_version="4.1.0",
    )

    rows = _view_rows(session, allele_id=allele.id, annotation_type=AnnotationType.GNOMAD_ALLELE_FREQUENCY.value)

    assert [r.id for r in rows] == [latest.id]
    assert rows[0].source_version == "4.1.0"


def test_variant_subject_events_present_and_keyed_by_variant(session, setup_lib_db_with_variant, job_run):
    variant = setup_lib_db_with_variant
    _event(session, job_run, AnnotationType.VRS_MAPPING, Disposition.FAILED, "failed", variant_id=variant.id)
    latest = _event(session, job_run, AnnotationType.VRS_MAPPING, Disposition.PRESENT, "mapped", variant_id=variant.id)

    rows = _view_rows(session, variant_id=variant.id, annotation_type=AnnotationType.VRS_MAPPING.value)

    assert [r.id for r in rows] == [latest.id]
    assert rows[0].allele_id is None


def test_distinct_subjects_yield_distinct_rows(session, job_run):
    a1 = _allele(session, "view-a1")
    a2 = _allele(session, "view-a2")
    _event(session, job_run, AnnotationType.CLINGEN_ALLELE_ID, Disposition.PRESENT, "created", allele_id=a1.id)
    _event(session, job_run, AnnotationType.CLINGEN_ALLELE_ID, Disposition.NOT_APPLICABLE, "no_hgvs", allele_id=a2.id)

    rows = _view_rows(session, annotation_type=AnnotationType.CLINGEN_ALLELE_ID.value)

    assert {r.allele_id for r in rows} == {a1.id, a2.id}
