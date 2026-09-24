from typing import TYPE_CHECKING

from sqlalchemy import Column, ForeignKey, Index, Integer, text
from sqlalchemy.orm import Mapped, relationship

from mavedb.db.base import Base
from mavedb.db.mixins import ValidTime

if TYPE_CHECKING:
    from .allele import Allele
    from .gnomad_variant import GnomADVariant


class GnomadAlleleLink(ValidTime, Base):
    """Valid-time link between an :class:`Allele` and a gnomAD variant.

    Replaces the frozen ``gnomad_variants_mapped_variants`` association table for new-model writes.
    A link is live while ``valid_to`` is NULL; a gnomAD version bump retires the live row and inserts
    a successor rather than deleting, so prior-version frequency links remain queryable point-in-time.
    The partial unique index enforces **a single live link per allele** — gnomAD frequency is one
    current value, so a new version supersedes the old (unlike ClinVar, which keeps one live link per
    release). This matches the VEP consequence shape, not the ClinVar control shape.
    """

    __tablename__ = "gnomad_allele_links"

    id: Mapped[int] = Column(Integer, primary_key=True)
    allele_id: Mapped[int] = Column(
        Integer,
        ForeignKey("alleles.id", ondelete="RESTRICT"),
        nullable=False,
    )
    gnomad_variant_id: Mapped[int] = Column(
        Integer,
        ForeignKey("gnomad_variants.id", ondelete="RESTRICT"),
        nullable=False,
    )

    allele: Mapped["Allele"] = relationship("Allele")
    gnomad_variant: Mapped["GnomADVariant"] = relationship("GnomADVariant", back_populates="allele_links")

    __table_args__ = (
        Index(
            "ix_gnomad_allele_links_allele_id",
            "allele_id",
        ),
        Index(
            "ix_gnomad_allele_links_gnomad_variant_id",
            "gnomad_variant_id",
        ),
        # At most one live link per allele. A version bump supersedes (retires the old, inserts the
        # new) rather than accumulating per-version live links; superseded rows stay for point-in-time
        # queries. Only the live row participates in this constraint.
        Index(
            "uq_gnomad_allele_links_live",
            "allele_id",
            unique=True,
            postgresql_where=text("valid_to IS NULL"),
        ),
    )
