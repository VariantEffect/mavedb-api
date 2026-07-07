from datetime import date
from typing import TYPE_CHECKING, Any, Optional

from sqlalchemy import Column, Date, Index, Integer, String, UniqueConstraint, func
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy.orm import Mapped, relationship

from mavedb.db.base import Base
from mavedb.lib.hgvs import extract_accession

if TYPE_CHECKING:
    from .mapping_record_allele import MappingRecordAllele


class Allele(Base):
    __tablename__ = "alleles"

    id: Mapped[int] = Column(Integer, primary_key=True)

    vrs_digest: Mapped[str] = Column(String, nullable=False)
    level = Column(String(length=16), nullable=False)

    hgvs_g = Column(String, nullable=True)
    hgvs_c = Column(String, nullable=True)
    hgvs_p = Column(String, nullable=True)

    clingen_allele_id = Column(String, nullable=True)
    post_mapped: Optional[Any] = Column(JSONB(none_as_null=True), nullable=True)

    created_at = Column(Date, nullable=False, default=date.today)
    updated_at = Column(Date, nullable=False, default=date.today, onupdate=date.today)

    @hybrid_property
    def transcript(self) -> str:
        """Reference accession of the populated HGVS column (derived, not stored).

        Exactly one of hgvs_g/hgvs_c/hgvs_p is populated per allele, and the transcript is that
        string's accession. Derived rather than stored so it cannot drift from the HGVS column
        it duplicates.
        """
        return extract_accession(self.hgvs_g or self.hgvs_c or self.hgvs_p or "")

    @transcript.inplace.expression
    @classmethod
    def _transcript_expression(cls):
        return func.split_part(func.coalesce(cls.hgvs_g, cls.hgvs_c, cls.hgvs_p), ":", 1)

    mapping_record_links: Mapped[list["MappingRecordAllele"]] = relationship(
        "MappingRecordAllele",
        back_populates="allele",
    )

    # Annotation links (VEP, gnomAD, ClinVar) deliberately carry no reverse collection here — they are
    # one-directional annotation->Allele, navigated set-wise from the link tables, not from an Allele
    # instance. Keep new annotation links one-directional unless a read path needs the navigation.

    __table_args__ = (
        UniqueConstraint("vrs_digest", name="uq_alleles_vrs_digest"),
        Index("ix_alleles_vrs_digest", "vrs_digest"),
        Index("ix_alleles_level", "level"),
        Index("ix_alleles_clingen_allele_id", "clingen_allele_id"),
    )
