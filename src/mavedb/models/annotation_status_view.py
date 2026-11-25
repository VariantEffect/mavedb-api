"""``v_annotation_status_current`` — one row per status key, pairing its live outcome with its live failure.

A key's live outcome and live failure are separate ``annotation_status`` rows, so every reader of current status
would otherwise repeat the same pairing. The outcome columns are NULL for a key that has never been checked
successfully; the failure columns are NULL for a key whose latest check succeeded. ``failing_since`` is when the
key's current outage began, which can be earlier than the live failure row if the failure's reason changed since.

There is deliberately **no ``score_set_id`` axis**: an allele-subject status is a shared allele-level fact, not a
property of any one score set. Resolve a score set's alleles and variants through the live mapping links (e.g.
``lib.clingen.alleles.get_alleles_for_score_set``) and look those subjects up here.

The migration creates the view ``WITH (security_invoker = true)``, so row-level security policies added to
``annotation_status`` (#808) will apply to whoever queries it. The view built from this module by ``create_all``,
which tests use, doesn't set the option.
"""

from sqlalchemy import func, select
from sqlalchemy.dialects.postgresql import array_agg

from mavedb.db.base import Base
from mavedb.db.view import view
from mavedb.models.annotation_status import AnnotationStatus
from mavedb.models.enums.status_kind import StatusKind

signature = "v_annotation_status_current"
"""The view's name in the database."""

_is_outcome = AnnotationStatus.kind == StatusKind.OUTCOME.value
_is_failure = AnnotationStatus.kind == StatusKind.FAILURE.value

definition = (
    select(
        AnnotationStatus.allele_id.label("allele_id"),
        AnnotationStatus.variant_id.label("variant_id"),
        AnnotationStatus.annotation_type.label("annotation_type"),
        AnnotationStatus.release.label("release"),
        func.max(AnnotationStatus.disposition).filter(_is_outcome).label("disposition"),
        func.max(AnnotationStatus.reason).filter(_is_outcome).label("reason"),
        func.max(AnnotationStatus.source_version).filter(_is_outcome).label("source_version"),
        array_agg(AnnotationStatus.status_metadata).filter(_is_outcome)[1].label("status_metadata"),
        func.max(AnnotationStatus.valid_from).filter(_is_outcome).label("established_at"),
        func.max(AnnotationStatus.job_run_id).filter(_is_outcome).label("established_job_run_id"),
        func.max(AnnotationStatus.failing_since).filter(_is_failure).label("failing_since"),
        func.max(AnnotationStatus.reason).filter(_is_failure).label("failure_reason"),
        array_agg(AnnotationStatus.status_metadata).filter(_is_failure)[1].label("failure_metadata"),
    )
    .where(AnnotationStatus.valid_to.is_(None))
    .group_by(
        AnnotationStatus.allele_id,
        AnnotationStatus.variant_id,
        AnnotationStatus.annotation_type,
        AnnotationStatus.release,
    )
)


class CurrentAnnotationStatusView(Base):
    __table__ = view(signature, definition, materialized=False)

    # The status key is the identity. A subject column and ``release`` can be NULL, which the mapper's default
    # ``allow_partial_pks=True`` accepts.
    __mapper_args__ = {
        "primary_key": [
            __table__.c.allele_id,
            __table__.c.variant_id,
            __table__.c.annotation_type,
            __table__.c.release,
        ]
    }

    allele_id = __table__.c.allele_id
    variant_id = __table__.c.variant_id
    annotation_type = __table__.c.annotation_type
    release = __table__.c.release
    disposition = __table__.c.disposition
    reason = __table__.c.reason
    source_version = __table__.c.source_version
    status_metadata = __table__.c.status_metadata
    established_at = __table__.c.established_at
    established_job_run_id = __table__.c.established_job_run_id
    failing_since = __table__.c.failing_since
    failure_reason = __table__.c.failure_reason
    failure_metadata = __table__.c.failure_metadata
