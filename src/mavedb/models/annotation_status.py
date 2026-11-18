"""
SQLAlchemy model for annotation status: what the pipeline currently knows about each subject, and what it
used to know.

Its subjects are the same two persistent entities as the annotation pipeline's: ``Variant`` and ``Allele``,
chosen by ``annotation_type``. A live row is current; a retired row is history, with the interval it held.
A status changes by being superseded, never edited in place, so every change leaves history (#973).
"""

from datetime import datetime
from typing import TYPE_CHECKING, Any, Dict, Optional

from sqlalchemy import CheckConstraint, DateTime, ForeignKey, Index, Integer, String, text
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.mutable import MutableDict
from sqlalchemy.orm import Mapped, mapped_column, relationship

from mavedb.db.base import Base
from mavedb.models.enums.annotation_type import AnnotationType
from mavedb.models.enums.disposition import Disposition
from mavedb.models.enums.event_reason import EventReason
from mavedb.models.enums.status_kind import StatusKind
from mavedb.models.mixins.valid_time import ValidTime

if TYPE_CHECKING:
    from mavedb.models.allele import Allele
    from mavedb.models.job_run import JobRun
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

RELEASE_KEYED_TYPES = (AnnotationType.CLINVAR_CONTROL.value,)
"""annotation_type values that keep one live status per source release, so ``release`` is part of their key.
Other versioned sources (gnomAD, VEP) supersede on a new release and record it in ``source_version``."""


def sql_in_list(values: tuple[str, ...]) -> str:
    """``values`` as the body of an SQL ``IN (...)`` list, for CHECK constraints."""
    return ", ".join(f"'{v}'" for v in values)


class AnnotationStatus(ValidTime, Base):
    """The status (not the value) of one pipeline check on a ``Variant`` or an ``Allele``.

    The value (frequency, consequence, CAID, control) lives in the domain ValidTime tables. This table records
    what a check found and why, including the answers that leave no value behind: an absence, a check that
    couldn't be asked, or one that failed. Reading the domain tables alone can't tell a confirmed absence from
    a check that never ran.

    A key is the subject, ``annotation_type``, ``release`` and ``kind``, and has at most one live row. An
    ``outcome`` row is the result of the latest successful check. A ``failure`` row is live while the key's
    checks are failing and is retired when one succeeds, so a timeout never overwrites a known result.

    NOTE: JSONB ``status_metadata`` is tracked as a mutable object via MutableDict, which only catches
          top-level mutations. Rows are superseded rather than edited, so this matters only before the insert.
    """

    __tablename__ = "annotation_status"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)

    annotation_type: Mapped[AnnotationType] = mapped_column(String(50), nullable=False)
    kind: Mapped[StatusKind] = mapped_column(String(20), nullable=False)

    # Exactly one is set, per ck_annotation_status_subject.
    variant_id: Mapped[Optional[int]] = mapped_column(
        Integer, ForeignKey("variants.id", ondelete="CASCADE"), nullable=True
    )
    allele_id: Mapped[Optional[int]] = mapped_column(
        Integer, ForeignKey("alleles.id", ondelete="RESTRICT"), nullable=True
    )

    # The source release this status is for, set only for RELEASE_KEYED_TYPES.
    release: Mapped[Optional[str]] = mapped_column(String(50), nullable=True)

    disposition: Mapped[Disposition] = mapped_column(String(50), nullable=False)

    # Domain-specific code reusing in-code vocabularies (EventReason, plus MappingOutcome and RT
    # skip_category for those two jobs); disposition is the public axis.
    reason: Mapped[EventReason] = mapped_column(String(50), nullable=False)

    # gnomAD db_version / Ensembl release / ClinVar release / mapper version the check ran against.
    source_version: Mapped[Optional[str]] = mapped_column(String(50), nullable=True)

    # DB column is "metadata"; the attribute avoids the reserved Declarative name.
    status_metadata: Mapped[Optional[Dict[str, Any]]] = mapped_column(
        "metadata", MutableDict.as_mutable(JSONB), nullable=True
    )

    # When the key's current outage began, set only on failure rows. A failure whose reason changes supersedes the
    # failure row and carries this forward, so it can be earlier than the row's own valid_from.
    failing_since: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)

    job_run_id: Mapped[Optional[int]] = mapped_column(
        Integer, ForeignKey("job_runs.id", ondelete="SET NULL"), nullable=True
    )

    # One-directional (no back-ref) — match the annotation-link convention.
    variant: Mapped[Optional["Variant"]] = relationship("Variant")
    allele: Mapped[Optional["Allele"]] = relationship("Allele")
    job_run: Mapped[Optional["JobRun"]] = relationship("JobRun")

    __table_args__ = (
        # Polymorphic subject: the type picks exactly one subject column.
        CheckConstraint(
            f"(annotation_type IN ({sql_in_list(VARIANT_SUBJECT_TYPES)}) "
            "AND variant_id IS NOT NULL AND allele_id IS NULL) "
            f"OR (annotation_type IN ({sql_in_list(ALLELE_SUBJECT_TYPES)}) "
            "AND allele_id IS NOT NULL AND variant_id IS NULL)",
            name="ck_annotation_status_subject",
        ),
        CheckConstraint(
            f"(annotation_type IN ({sql_in_list(RELEASE_KEYED_TYPES)})) = (release IS NOT NULL)",
            name="ck_annotation_status_release",
        ),
        # A failure row is the only place a failed disposition can live.
        CheckConstraint(
            f"(kind = '{StatusKind.OUTCOME.value}' AND disposition <> '{Disposition.FAILED.value}') "
            f"OR (kind = '{StatusKind.FAILURE.value}' AND disposition = '{Disposition.FAILED.value}')",
            name="ck_annotation_status_kind",
        ),
        CheckConstraint(
            f"(kind = '{StatusKind.FAILURE.value}') = (failing_since IS NOT NULL)",
            name="ck_annotation_status_failing_since",
        ),
        Index(
            "uq_annotation_status_live",
            "allele_id",
            "variant_id",
            "annotation_type",
            "release",
            "kind",
            unique=True,
            postgresql_where=text("valid_to IS NULL"),
            postgresql_nulls_not_distinct=True,
        ),
        # A subject's history, live and retired.
        Index("ix_annotation_status_allele_type", "allele_id", "annotation_type"),
        Index("ix_annotation_status_variant_type", "variant_id", "annotation_type"),
        # Backs the job-run ON DELETE SET NULL, and audit by run.
        Index("ix_annotation_status_job_run_id", "job_run_id"),
    )

    def __repr__(self) -> str:
        subject = f"variant_id={self.variant_id}" if self.variant_id is not None else f"allele_id={self.allele_id}"
        release = f", release={self.release!r}" if self.release is not None else ""
        return (
            f"<AnnotationStatus(id={self.id}, type='{self.annotation_type}', kind='{self.kind}', {subject}{release}, "
            f"disposition='{self.disposition}', reason='{self.reason}', valid_to={self.valid_to})>"
        )
