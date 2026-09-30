from datetime import date
from typing import TYPE_CHECKING, Any, Optional

from sqlalchemy import Date, Index, Integer, String, UniqueConstraint, func
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy.orm import mapped_column
from sqlalchemy.orm import Mapped, relationship

from mavedb.db.base import Base
from mavedb.lib.hgvs import extract_accession

if TYPE_CHECKING:
    from .mapping_record_allele import MappingRecordAllele


def _coalesce_hgvs(hgvs_g, hgvs_c, hgvs_p):
    """SQL form of ``Allele.hgvs``."""
    return func.coalesce(hgvs_g, hgvs_c, hgvs_p)


class Allele(Base):
    __tablename__ = "alleles"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)

    vrs_digest: Mapped[str] = mapped_column(String, nullable=False)
    level: Mapped[str] = mapped_column(String(length=16), nullable=False)

    hgvs_g: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    hgvs_c: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    hgvs_p: Mapped[Optional[str]] = mapped_column(String, nullable=True)

    clingen_allele_id: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    post_mapped: Mapped[Optional[Any]] = mapped_column(JSONB(none_as_null=True), nullable=True)

    created_at: Mapped[date] = mapped_column(Date, nullable=False, default=date.today)
    updated_at: Mapped[date] = mapped_column(Date, nullable=False, default=date.today, onupdate=date.today)

    @hybrid_property
    def transcript(self) -> str:
        """Reference accession of the populated HGVS column (derived, not stored).

        Exactly one of hgvs_g/hgvs_c/hgvs_p is populated per allele, and the transcript is that
        string's accession. Derived rather than stored so it cannot drift from the HGVS column
        it duplicates.
        """
        return extract_accession(self.hgvs or "")

    @hybrid_property
    def hgvs(self) -> Optional[str]:
        """The populated HGVS expression: whichever of hgvs_g/hgvs_c/hgvs_p is set (derived, not stored).

        None when none is, on the instance and in SQL alike (``coalesce`` yields NULL).
        """
        return self.hgvs_g or self.hgvs_c or self.hgvs_p

    @hgvs.inplace.expression
    @classmethod
    def _hgvs_expression(cls):
        return _coalesce_hgvs(cls.hgvs_g, cls.hgvs_c, cls.hgvs_p)

    @transcript.inplace.expression
    @classmethod
    def _transcript_expression(cls):
        return func.split_part(cls.hgvs, ":", 1)

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
        # One row per HGVS expression. Two digests for the same expression mean two writers built the
        # allele on different sequences, so the copies would never deduplicate. Fails at write time, in
        # get_or_create_allele, where the cause is. See AlleleIdentityConflictError.
        Index("uq_alleles_hgvs", _coalesce_hgvs(hgvs_g, hgvs_c, hgvs_p), unique=True),
    )
