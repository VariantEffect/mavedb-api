from datetime import date
from typing import TYPE_CHECKING, Optional

from sqlalchemy import Column, Date, Integer, String, UniqueConstraint
from sqlalchemy.orm import Mapped, relationship

from mavedb.db.base import Base
from mavedb.models.clinical_control_mapped_variant import mapped_variants_clinical_controls_association_table

if TYPE_CHECKING:
    from mavedb.models.clinvar_allele_link import ClinvarAlleleLink
    from mavedb.models.mapped_variant import MappedVariant


class ClinvarControl(Base):
    __tablename__ = "clinvar_controls"
    __table_args__ = (
        UniqueConstraint(
            "db_name", "db_identifier", "db_version", name="uq_clinvar_controls_db_name_identifier_version"
        ),
    )

    id: Mapped[int] = Column(Integer, primary_key=True)

    gene_symbol: Mapped[str] = Column(String, nullable=False, index=True)

    clinical_significance: Mapped[str] = Column(String, nullable=False)
    clinical_review_status: Mapped[str] = Column(String, nullable=False)

    db_name: Mapped[str] = Column(String, nullable=False, index=True)
    # ClinVar Allele ID (row level link).
    db_identifier: Mapped[str] = Column(String, nullable=False, index=True)
    db_version: Mapped[str] = Column(String, nullable=False, index=True)

    # ClinVar Variation ID (variation level link).
    clinvar_variation_id: Mapped[Optional[str]] = Column(String, nullable=True)

    creation_date = Column(Date, nullable=False, default=date.today)
    modification_date = Column(Date, nullable=False, default=date.today, onupdate=date.today)

    # Frozen serving path: links to MappedVariant via the old association table (never written for new data).
    mapped_variants: Mapped[list["MappedVariant"]] = relationship(
        "MappedVariant",
        secondary=mapped_variants_clinical_controls_association_table,
        back_populates="clinical_controls",
    )

    # New-model annotation links (one live link per allele per ClinVar release).
    allele_links: Mapped[list["ClinvarAlleleLink"]] = relationship(
        "ClinvarAlleleLink",
        back_populates="clinvar_control",
    )
