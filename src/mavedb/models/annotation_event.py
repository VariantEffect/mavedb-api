"""
SQLAlchemy model for the annotation event log.

One append-only log spanning the whole pipeline (mapping, reverse translation,
annotation). Its subjects are exactly two persistent entities — ``Variant`` and
``Allele`` — selected by ``annotation_type`` via the polymorphic-subject CHECK.
Everything else the pipeline touches (``MappingRecord``, ``MappingRecordAllele``,
the external value tables) is a vehicle or resolution path, never a status subject.

This is deliberately **not** a ``ValidTime`` table. A SCD-2 state table is a lossy
projection of an event log — it discards the skip/reconfirm/no-op events that are
the audit point. "Current" is derived (``DISTINCT ON (subject, annotation_type)
… id DESC``), never a stored ``current`` flag.
"""

from datetime import datetime
from typing import TYPE_CHECKING, Any, Dict, Optional

from sqlalchemy import CheckConstraint, DateTime, ForeignKey, Index, Integer, String, func, text
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.mutable import MutableDict
from sqlalchemy.orm import Mapped, mapped_column, relationship

from mavedb.db.base import Base
from mavedb.models.enums.annotation_type import AnnotationType
from mavedb.models.enums.disposition import Disposition
from mavedb.models.enums.event_reason import EventReason

if TYPE_CHECKING:
    from mavedb.models.allele import Allele
    from mavedb.models.job_run import JobRun
    from mavedb.models.score_set import ScoreSet
    from mavedb.models.variant import Variant

VARIANT_SUBJECT_TYPES = (
    AnnotationType.VRS_MAPPING.value,
    AnnotationType.CROSS_LEVEL_TRANSLATION.value,
    AnnotationType.VARIANT_TRANSLATION.value,
    AnnotationType.LDH_SUBMISSION.value,
)
"""annotation_type values whose subject is the variant (variant_id set, allele_id null)"""

ALLELE_SUBJECT_TYPES = (
    AnnotationType.CLINGEN_ALLELE_ID.value,
    AnnotationType.GNOMAD_ALLELE_FREQUENCY.value,
    AnnotationType.VEP_FUNCTIONAL_CONSEQUENCE.value,
    AnnotationType.CLINVAR_CONTROL.value,
    AnnotationType.MAPPED_HGVS.value,
)
"""annotation_type values whose subject is the allele (allele_id set, variant_id null)"""


def _sql_in_list(values: tuple[str, ...]) -> str:
    return ", ".join(f"'{v}'" for v in values)


class AnnotationEvent(Base):
    """An append-only event recording the *status* (not value) of one pipeline
    observation about a ``Variant`` or an ``Allele``.

    The value (frequency, consequence, CAID, control) lives in the domain
    ValidTime tables; this log records disposition + why + when. Reading the
    domain tables alone cannot distinguish confirmed-absence from never-checked —
    that gap is the whole reason the log exists.

    NOTE: JSONB ``event_metadata`` is tracked as a mutable object via MutableDict,
          which only catches top-level mutations. Mutating a nested object
          requires ``flag_modified(instance, "event_metadata")``.
    """

    __tablename__ = "annotation_event"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)

    annotation_type: Mapped[AnnotationType] = mapped_column(String(50), nullable=False)

    # Exactly one is set, per ck_annotation_event_subject.
    variant_id: Mapped[Optional[int]] = mapped_column(
        Integer, ForeignKey("variants.id", ondelete="RESTRICT"), nullable=True
    )
    allele_id: Mapped[Optional[int]] = mapped_column(
        Integer, ForeignKey("alleles.id", ondelete="RESTRICT"), nullable=True
    )

    disposition: Mapped[Disposition] = mapped_column(String(50), nullable=False)

    # Domain-specific code reusing in-code vocabularies (EventReason, plus MappingOutcome and RT
    # skip_category for those two jobs); disposition is the public axis.
    reason: Mapped[EventReason] = mapped_column(String(50), nullable=False)

    # gnomAD db_version / Ensembl release / ClinVar release / mapper version.
    source_version: Mapped[Optional[str]] = mapped_column(String(50), nullable=True)

    # DB column is "metadata"; the attribute avoids the reserved Declarative name.
    event_metadata: Mapped[Optional[Dict[str, Any]]] = mapped_column(
        "metadata", MutableDict.as_mutable(JSONB), nullable=True
    )

    job_run_id: Mapped[Optional[int]] = mapped_column(
        Integer, ForeignKey("job_runs.id", ondelete="SET NULL"), nullable=True
    )
    score_set_id: Mapped[Optional[int]] = mapped_column(
        Integer, ForeignKey("scoresets.id", ondelete="SET NULL"), nullable=True
    )

    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())

    # One-directional (no back-ref) — match the annotation-link convention.
    variant: Mapped[Optional["Variant"]] = relationship("Variant")
    allele: Mapped[Optional["Allele"]] = relationship("Allele")
    job_run: Mapped[Optional["JobRun"]] = relationship("JobRun")
    score_set: Mapped[Optional["ScoreSet"]] = relationship("ScoreSet")

    __table_args__ = (
        # Polymorphic subject: the type picks exactly one subject column.
        CheckConstraint(
            f"(annotation_type IN ({_sql_in_list(VARIANT_SUBJECT_TYPES)}) "
            "AND variant_id IS NOT NULL AND allele_id IS NULL) "
            f"OR (annotation_type IN ({_sql_in_list(ALLELE_SUBJECT_TYPES)}) "
            "AND allele_id IS NOT NULL AND variant_id IS NULL)",
            name="ck_annotation_event_subject",
        ),
        # latest-per-allele / latest-per-variant (the DISTINCT ON … id DESC projections)
        Index("ix_annotation_event_allele_type_id", "allele_id", "annotation_type", text("id DESC")),
        Index("ix_annotation_event_variant_type_id", "variant_id", "annotation_type", text("id DESC")),
        # version-keyed skip
        Index("ix_annotation_event_allele_type_version", "allele_id", "annotation_type", "source_version"),
        # audit by run
        Index("ix_annotation_event_job_run_id", "job_run_id"),
        # backs the score-set ON DELETE SET NULL cascade and score-set-scoped audit queries
        Index("ix_annotation_event_score_set_id", "score_set_id"),
    )

    def __repr__(self) -> str:
        subject = f"variant_id={self.variant_id}" if self.variant_id is not None else f"allele_id={self.allele_id}"
        return (
            f"<AnnotationEvent(id={self.id}, type='{self.annotation_type}', {subject}, "
            f"disposition='{self.disposition}', reason='{self.reason}', "
            f"source_version={self.source_version!r}, created_at={self.created_at})>"
        )
