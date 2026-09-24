"""``v_current_annotation_events`` — the current-state projection over the AnnotationEvent log.

The log is append-only; "current" is derived, never stored. This view exposes the latest event per
``(subject, annotation_type)`` — one row per allele/variant per annotation type — so operators, BI,
and app code can read current status with a plain ``SELECT`` instead of re-deriving the
``DISTINCT ON`` everywhere. It mirrors the existing ``v_variant_annotations`` pattern.

ClinVar is **multi-live**: an allele accumulates one live link per archival release, so its current
status is one row *per release*. The window partition folds ``source_version`` in **only** for
``clinvar_control`` (via a CASE that is constant-NULL for every other type, collapsing those back to
one row per subject+type).

The subject is polymorphic — exactly one of ``variant_id`` / ``allele_id`` is set per row (enforced by
the log's CHECK), and the other is constant-NULL within a partition, so partitioning by both keys each
row to its real subject.

There is deliberately **no ``score_set_id`` axis**: an allele-subject status (CAID, gnomAD, ClinVar,
VEP) is a *shared* allele-level fact, not a property of any one score set. A consumer that wants a
score set's annotation status resolves the score set's current alleles (and variants) through the live
mapping links — e.g. ``lib.clingen.alleles.get_alleles_for_score_set`` — and then looks those subjects
up here. Filtering by the *run's* score set would wrongly drop an allele last (re-)annotated by another
score set's run. The derived "current-for-variant" walk (resolving an allele-subject fact down to a
variant at the level a type keys on) is intentionally **not** built here — that is the deferred
consumer surface; see ``docs/design/allele-annotation-status.md``.
"""

from sqlalchemy import case, func, select

from mavedb.db.base import Base
from mavedb.db.view import view
from mavedb.models.annotation_event import AnnotationEvent
from mavedb.models.enums.annotation_type import AnnotationType

signature = "v_current_annotation_events"

# ClinVar is the lone multi-live type: split "current" by release. For every other type this is NULL,
# so the partition collapses to (allele_id, variant_id, annotation_type) — one current row per subject.
_clinvar_release_key = case(
    (AnnotationEvent.annotation_type == AnnotationType.CLINVAR_CONTROL.value, AnnotationEvent.source_version),
    else_=None,
)

_ranked = select(
    AnnotationEvent.id.label("id"),
    AnnotationEvent.annotation_type.label("annotation_type"),
    AnnotationEvent.variant_id.label("variant_id"),
    AnnotationEvent.allele_id.label("allele_id"),
    AnnotationEvent.disposition.label("disposition"),
    AnnotationEvent.reason.label("reason"),
    AnnotationEvent.source_version.label("source_version"),
    AnnotationEvent.event_metadata.label("event_metadata"),
    AnnotationEvent.job_run_id.label("job_run_id"),
    AnnotationEvent.created_at.label("created_at"),
    func.row_number()
    .over(
        partition_by=[
            AnnotationEvent.allele_id,
            AnnotationEvent.variant_id,
            AnnotationEvent.annotation_type,
            _clinvar_release_key,
        ],
        order_by=AnnotationEvent.id.desc(),
    )
    .label("row_number"),
).subquery("ranked_annotation_events")

definition = select(
    _ranked.c.id,
    _ranked.c.annotation_type,
    _ranked.c.variant_id,
    _ranked.c.allele_id,
    _ranked.c.disposition,
    _ranked.c.reason,
    _ranked.c.source_version,
    _ranked.c.event_metadata,
    _ranked.c.job_run_id,
    _ranked.c.created_at,
).where(_ranked.c.row_number == 1)


class CurrentAnnotationEventView(Base):
    __table__ = view(signature, definition, materialized=False)
    # Each surviving event id is unique across the view, so it is a valid mapping key for the
    # otherwise-PK-less view (standard SQLAlchemy view-mapping idiom).
    __mapper_args__ = {"primary_key": [__table__.c.id]}

    id = __table__.c.id
    annotation_type = __table__.c.annotation_type
    variant_id = __table__.c.variant_id
    allele_id = __table__.c.allele_id
    disposition = __table__.c.disposition
    reason = __table__.c.reason
    source_version = __table__.c.source_version
    event_metadata = __table__.c.event_metadata
    job_run_id = __table__.c.job_run_id
    created_at = __table__.c.created_at
