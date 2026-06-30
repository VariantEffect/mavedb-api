# ruff: noqa: E402
"""Tests for the variant_annotations CLI's current_annotation_summary resolution.

The score set is the entry point, but status is resolved per-allele through the live mapping links —
so a shared allele's status counts even when a *different* score set's run produced it.
"""

import pytest

pytest.importorskip("psycopg2")

from mavedb.models.allele import Allele
from mavedb.models.annotation_event import AnnotationEvent
from mavedb.models.enums.annotation_type import AnnotationType
from mavedb.models.enums.disposition import Disposition
from mavedb.models.mapping_record import MappingRecord
from mavedb.models.mapping_record_allele import MappingRecordAllele
from mavedb.models.variant import Variant
from mavedb.scripts.variant_annotations import current_annotation_summary
from tests.helpers.constants import TEST_MINIMAL_VARIANT


def _variant_mapped_to_allele(session, score_set, allele):
    """A variant in the score set with a live mapping record + live authoritative link to ``allele``."""
    variant = Variant(**TEST_MINIMAL_VARIANT, urn=f"{score_set.urn}#1", score_set_id=score_set.id)
    session.add(variant)
    session.commit()

    record = MappingRecord(variant_id=variant.id, assay_level="genomic", mapping_api_version="test.0.0")
    session.add(record)
    session.commit()
    session.add(MappingRecordAllele(mapping_record_id=record.id, allele_id=allele.id, is_authoritative=True))
    session.commit()
    return variant


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

    # Event written with no owning score set (e.g. a different score set's run touched the shared allele).
    session.add(
        AnnotationEvent(
            annotation_type=AnnotationType.CLINGEN_ALLELE_ID,
            allele_id=allele.id,
            disposition=Disposition.PRESENT,
            reason="created",
            job_run_id=job_run.id,
            score_set_id=None,
        )
    )
    session.commit()

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

    session.add(
        AnnotationEvent(
            annotation_type=AnnotationType.VRS_MAPPING,
            variant_id=variant.id,
            disposition=Disposition.PRESENT,
            reason="mapped",
            job_run_id=job_run.id,
        )
    )
    session.commit()

    summary = current_annotation_summary(session, score_set)
    counts = {(r["annotation_type"], r["disposition"]): r["count"] for r in summary}

    assert counts.get((AnnotationType.VRS_MAPPING.value, Disposition.PRESENT.value)) == 1
