from datetime import date
from typing import TYPE_CHECKING

from sqlalchemy import Column, Date, Integer, String
from sqlalchemy.orm import Mapped, relationship

from mavedb.db.base import Base

if TYPE_CHECKING:
    from .variant import Variant


class ClinvarVariant(Base):
    __tablename__ = "clinvar_variants"

    id = Column(Integer, primary_key=True)
    allele_id = Column(Integer, nullable=False, index=True)
    gene_symbol = Column(String, nullable=False)

    clinical_significance = Column(String, nullable=False)
    clinical_review_status = Column(String, nullable=False)

    clinvar_db_version = Column(String, nullable=False)

    creation_date = Column(Date, nullable=False, default=date.today)
    modification_date = Column(Date, nullable=False, default=date.today, onupdate=date.today)

    mapped_variants: Mapped[list["Variant"]] = relationship(back_populates="clinvar_variant")
