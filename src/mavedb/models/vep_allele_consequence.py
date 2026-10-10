from datetime import date
from typing import TYPE_CHECKING

from sqlalchemy import Column, Date, Enum, ForeignKey, Index, Integer, String, text
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, relationship

from mavedb.db.base import Base
from mavedb.db.mixins import ValidTime
from mavedb.models.enums.vep import VepConsequenceSource

if TYPE_CHECKING:
    from .allele import Allele


class VepAlleleConsequence(ValidTime, Base):
    """Valid-time VEP functional-consequence result for a deduplicated :class:`Allele`.

    A row is live while ``valid_to`` is NULL; the partial unique index enforces a
    single live consequence per allele, matching the gnomAD link shape (not ClinVar's multi-live shape).

    Two version axes key a stored consequence, and the job's current-release skip requires *both* to
    match:

    - ``source_version`` — the Ensembl release the consequence was resolved under (e.g. ``"116"``),
      version-keying the upstream result like gnomAD's ``db_version``. Always the *latest* release that
      confirmed the value, not the release it first appeared.
    - ``resolver_version`` — ``variant_annotation.lib.vep.RESOLVER_VERSION``, the version of *our*
      resolution rule (severity ranking, transcript matching, Recoder combination). Catches a rule fix
      the Ensembl release can't see; a NULL (legacy) row never matches and is re-queried once, then
      filled in place.

    Supersede is **value-keyed, not version-keyed** (the one divergence from gnomAD): a re-run
    confirming the *same* consequence advances both versions and ``access_date`` in place rather than
    churning history; only a *changed* consequence retires the old row and inserts a successor.
    ``access_date`` is a human-facing "last confirmed" stamp — it plays no part in the skip itself.

    ``functional_consequence`` is nullable to leave room for a future negative cache; the current job
    writes only non-null consequences and re-queries no-result alleles each run.

    **Resolution provenance (#772).** ``functional_consequence`` is the headline term; three columns
    record *how* it was reached, since VEP's cross-transcript ``most_severe`` headline routinely
    describes a different overlapping isoform than the allele's own transcript:

    - ``consequence_terms`` — every term from the matched transcript entry, severity-ordered;
      ``functional_consequence`` is its first element.
    - ``consequence_source`` — transcript-matched, cross-transcript headline, or reference-identical.
    - ``matched_transcript`` — the transcript VEP actually used, set only when ``consequence_source =
      'transcript'`` (not assumed equal to the allele's own transcript, since the match is
      version-insensitive).

    Transient request failures are not stored here, they live in the annotation event stream
    (``AnnotationEvent``).
    """

    __tablename__ = "vep_allele_consequences"

    id: Mapped[int] = Column(Integer, primary_key=True)
    allele_id: Mapped[int] = Column(
        Integer,
        ForeignKey("alleles.id", ondelete="RESTRICT"),
        nullable=False,
    )
    functional_consequence = Column(String, nullable=True)
    consequence_terms = Column(JSONB(none_as_null=True), nullable=True)
    consequence_source = Column(
        Enum(
            VepConsequenceSource,
            name="vepconsequencesource",
            create_constraint=True,
            length=32,
            native_enum=False,
            validate_strings=True,
        ),
        nullable=True,
    )
    matched_transcript = Column(String, nullable=True)
    source_version: Mapped[str] = Column(String, nullable=False)
    # Nullable only for rows predating the column (treated as stale — re-queried, then filled).
    resolver_version = Column(String, nullable=True)
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
