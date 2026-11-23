# ruff: noqa: E402
"""Tests for the variant_annotations CLI's current_annotation_summary resolution.

The score set is the entry point, but status is resolved per-allele through the live mapping links —
so a shared allele's status counts even when a *different* score set's run produced it.
"""

import pytest

pytest.importorskip("psycopg2")

from mavedb.lib.annotation_status_manager import AnnotationStatusManager
from mavedb.models.allele import Allele
from mavedb.models.enums.annotation_type import AnnotationType
from mavedb.models.enums.disposition import Disposition
from mavedb.models.enums.event_reason import EventReason
from mavedb.models.mapping_record import MappingRecord
from mavedb.models.mapping_record_allele import MappingRecordAllele
from mavedb.models.variant import Variant
from mavedb.scripts.variant_annotations import current_annotation_summary
from tests.helpers.constants import TEST_MINIMAL_VARIANT
from tests.helpers.util.valid_time import add_versioned


def _variant_mapped_to_allele(session, score_set, allele):
    """A variant in the score set with a live mapping record + live authoritative link to ``allele``."""
    variant = Variant(**TEST_MINIMAL_VARIANT, urn=f"{score_set.urn}#1", score_set_id=score_set.id)
    session.add(variant)
    session.commit()

    record = MappingRecord(
        variant_id=variant.id, score_set_id=variant.score_set_id, assay_level="genomic", mapping_api_version="test.0.0"
    )
    add_versioned(session, record)
    add_versioned(
        session,
        MappingRecordAllele(
            mapping_record_id=record.id, score_set_id=record.score_set_id, allele_id=allele.id, is_authoritative=True
        ),
    )
    return variant


def _report(session, job_run, annotation_type, disposition, reason, **subject):
    manager = AnnotationStatusManager(session, job_run_id=job_run.id)
    manager.report(annotation_type, disposition=disposition, reason=reason, **subject)
    manager.flush()
    session.commit()


def test_summary_counts_allele_status_from_another_score_sets_run(session, setup_lib_db_with_score_set, job_run):
    """An allele-subject status produced by a *different* score set's run (or none) still counts for a
    score set whose variant currently maps to that shared allele."""
    score_set = setup_lib_db_with_score_set
    allele = Allele(
        vrs_digest="summary-shared", level="genomic", clingen_allele_id="CA1", post_mapped={"type": "Allele"}
    )
    session.add(allele)
    session.commit()
    _variant_mapped_to_allele(session, score_set, allele)

    # Statuses carry no owning score set: a different score set's run may have written this one.
    _report(
        session,
        job_run,
        AnnotationType.CLINGEN_ALLELE_ID,
        Disposition.PRESENT,
        EventReason.CREATED,
        allele_id=allele.id,
    )

    summary = current_annotation_summary(session, score_set)
    counts = {(r["annotation_type"], r["disposition"]): r["count"] for r in summary}

    assert counts.get((AnnotationType.CLINGEN_ALLELE_ID.value, Disposition.PRESENT.value)) == 1


def test_summary_includes_variant_subject_status(session, setup_lib_db_with_score_set, job_run):
    """Variant-subject types (mapping/RT/LDH) are counted per variant, off the score set's variants."""
    score_set = setup_lib_db_with_score_set
    allele = Allele(vrs_digest="summary-vs", level="genomic", post_mapped={"type": "Allele"})
    session.add(allele)
    session.commit()
    variant = _variant_mapped_to_allele(session, score_set, allele)

    _report(session, job_run, AnnotationType.VRS_MAPPING, Disposition.PRESENT, "mapped", variant_id=variant.id)

    summary = current_annotation_summary(session, score_set)
    counts = {(r["annotation_type"], r["disposition"]): r["count"] for r in summary}

    assert counts.get((AnnotationType.VRS_MAPPING.value, Disposition.PRESENT.value)) == 1


def test_summary_counts_a_failing_status_as_failed(session, setup_lib_db_with_score_set, job_run):
    """A key whose checks are failing counts as failed, though its earlier outcome is still held."""
    score_set = setup_lib_db_with_score_set
    allele = Allele(vrs_digest="summary-failing", level="genomic", post_mapped={"type": "Allele"})
    session.add(allele)
    session.commit()
    _variant_mapped_to_allele(session, score_set, allele)

    gnomad = AnnotationType.GNOMAD_ALLELE_FREQUENCY
    _report(session, job_run, gnomad, Disposition.PRESENT, EventReason.CREATED, allele_id=allele.id)
    _report(session, job_run, gnomad, Disposition.FAILED, EventReason.API_ERROR, allele_id=allele.id)

    summary = current_annotation_summary(session, score_set)
    counts = {(r["annotation_type"], r["disposition"]): r["count"] for r in summary}

    assert counts == {(gnomad.value, Disposition.FAILED.value): 1}
