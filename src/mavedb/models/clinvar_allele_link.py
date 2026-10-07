from typing import TYPE_CHECKING

from sqlalchemy import Column, ForeignKey, Index, Integer, text
from sqlalchemy.orm import Mapped, relationship

from mavedb.db.base import Base
from mavedb.db.mixins import ValidTime

if TYPE_CHECKING:
    from .allele import Allele
    from .clinical_control import ClinvarControl


class ClinvarAlleleLink(ValidTime, Base):
    """Valid-time link between an :class:`Allele` and a :class:`ClinvarControl` release.

    Replaces the frozen ``mapped_variants_clinical_controls`` association table for new-model writes.
    A link is live while ``valid_to`` is NULL. Unlike gnomAD/VEP (one live result per allele), the partial
    unique index is ``(allele_id, clinvar_control_id) WHERE valid_to IS NULL`` — **multi-live**: an allele
    accumulates one live link per ClinVar release, because each release is a distinct, versioned
    ``ClinvarControl`` assertion that stacks rather than supersedes. A link retires only on two theoretical
    paths (archival data does not change): ClinVar drops the variant from a release (a re-run finds no data
    for it), or the allele re-resolves to a *different* control within the same release — the job supersedes
    that newest-wins to preserve one live link per (allele, release), since this index only enforces one live
    link per (allele, control).
    """

    __tablename__ = "clinvar_allele_links"

    id: Mapped[int] = Column(Integer, primary_key=True)
    allele_id: Mapped[int] = Column(
        Integer,
        ForeignKey("alleles.id", ondelete="RESTRICT"),
        nullable=False,
    )
    clinvar_control_id: Mapped[int] = Column(
        Integer,
        ForeignKey("clinvar_controls.id", ondelete="RESTRICT"),
        nullable=False,
    )

    # One-directional to Allele (no reverse collection there); back-ref on the ClinvarControl entity only.
    allele: Mapped["Allele"] = relationship("Allele")
    clinvar_control: Mapped["ClinvarControl"] = relationship("ClinvarControl", back_populates="allele_links")

    __table_args__ = (
        Index(
            "ix_clinvar_allele_links_allele_id",
            "allele_id",
        ),
        Index(
            "ix_clinvar_allele_links_clinvar_control_id",
            "clinvar_control_id",
        ),
        # Multi-live: one live link per (allele, release). Each ClinVar release is a distinct
        # ClinvarControl row, so different releases stack as independent live links rather than
        # superseding. Only live rows participate in this constraint.
        Index(
            "uq_clinvar_allele_links_live",
            "allele_id",
            "clinvar_control_id",
            unique=True,
            postgresql_where=text("valid_to IS NULL"),
        ),
    )
