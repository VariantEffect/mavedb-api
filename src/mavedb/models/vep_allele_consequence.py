from datetime import date
from typing import TYPE_CHECKING, Optional

from sqlalchemy import Column, Date, ForeignKey, Index, Integer, String, text
from sqlalchemy.orm import Mapped, relationship

from mavedb.db.base import Base
from mavedb.db.mixins import ValidTime

if TYPE_CHECKING:
    from .allele import Allele


class VepAlleleConsequence(ValidTime, Base):
    """Valid-time VEP functional-consequence result for a deduplicated :class:`Allele`.

    Replaces the frozen ``vep_functional_consequence``/``vep_access_date`` columns on
    ``MappedVariant`` for new-model writes (Step 2 of the annotation infrastructure migration,
    docs/design/annotation-infrastructure-migration.md). A row is live while ``valid_to`` is NULL;
    the partial unique index enforces **a single live consequence per allele** — VEP's most-severe
    consequence is one current value, so a changed result supersedes the prior row rather than
    accumulating. This matches the gnomAD link shape, not ClinVar's multi-live shape.

    ``source_version`` is the Ensembl release the consequence was resolved under (e.g. ``"116"``,
    from ``/info/software``). An Ensembl release is coordinated — software + transcript set +
    consequence vocabulary all bump together under one number — so this single value version-keys the
    upstream result exactly like gnomAD's ``db_version``. The job skips re-querying any allele already
    live at the current release. What it does **not** capture is our own ``VEP_CONSEQUENCES`` severity
    ordering (the list we pick "most severe" from); a change to that is a manual ``force`` re-run, not
    an automatic supersede.

    Supersede is deliberately **value-keyed, not version-keyed** (the one divergence from gnomAD): a VEP
    consequence is categorical and usually identical across releases, so superseding on every release
    bump would churn history every quarter with rows recording "still missense, still missense." Instead
    a new release that resolves the *same* consequence advances ``source_version``/``access_date`` in
    place — no supersede — and only a *changed* consequence retires the old row and inserts a successor.
    The trade-off: the live row's ``source_version`` is the latest release that confirmed the value, not
    the release it first appeared; acceptable because it describes the currently-held value's
    provenance, not when it became true. ``access_date`` is retained as a human-facing "last confirmed"
    audit stamp; it is no longer load-bearing for the skip.

    ``functional_consequence`` is nullable to leave room for a future negative cache (NULL = "VEP ran
    and found nothing"); the current job writes only non-null consequences and re-queries no-result
    alleles each run, mirroring gnomAD's no-match handling.
    """

    __tablename__ = "vep_allele_consequences"

    id: Mapped[int] = Column(Integer, primary_key=True)
    allele_id: Mapped[int] = Column(
        Integer,
        ForeignKey("alleles.id", ondelete="RESTRICT"),
        nullable=False,
    )
    functional_consequence: Mapped[Optional[str]] = Column(String, nullable=True)
    source_version: Mapped[str] = Column(String, nullable=False)
    access_date: Mapped[date] = Column(Date, nullable=False)

    allele: Mapped["Allele"] = relationship("Allele")

    __table_args__ = (
        Index(
            "ix_vep_allele_consequences_allele_id",
            "allele_id",
        ),
        # At most one live consequence per allele. A changed result supersedes (retires the old,
        # inserts the new) rather than accumulating; superseded rows stay for point-in-time queries.
        # Only the live row participates in this constraint.
        Index(
            "uq_vep_allele_consequences_live",
            "allele_id",
            unique=True,
            postgresql_where=text("valid_to IS NULL"),
        ),
    )
