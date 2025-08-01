from datetime import date
from typing import TYPE_CHECKING

from sqlalchemy import Column, Date, Integer, String
from sqlalchemy.orm import Mapped, relationship

from mavedb.db.base import Base
from mavedb.models.clinical_control_mapped_variant import mapped_variants_clinical_controls_association_table

if TYPE_CHECKING:
    from mavedb.models.mapped_variant import MappedVariant


class ClinicalControl(Base):
    __tablename__ = "clinical_controls"

    id = Column(Integer, primary_key=True)

    gene_symbol = Column(String, nullable=False, index=True)

    clinical_significance = Column(String, nullable=False)
    clinical_review_status = Column(String, nullable=False)

    db_name = Column(String, nullable=False, index=True)
    db_identifier = Column(String, nullable=False, index=True)
    db_version = Column(String, nullable=False, index=True)

    creation_date = Column(Date, nullable=False, default=date.today)
    modification_date = Column(Date, nullable=False, default=date.today, onupdate=date.today)

    mapped_variants: Mapped[list["MappedVariant"]] = relationship(
        "MappedVariant",
        secondary=mapped_variants_clinical_controls_association_table,
        back_populates="clinical_controls",
    )
