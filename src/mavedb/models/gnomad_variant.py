from datetime import date
from typing import TYPE_CHECKING

from sqlalchemy import Column, Date, Integer, Float, String
from sqlalchemy.orm import Mapped, relationship

from mavedb.db.base import Base
from mavedb.models.gnomad_variant_mapped_variant import gnomad_variants_mapped_variants_association_table

if TYPE_CHECKING:
    from mavedb.models.mapped_variant import MappedVariant


class GnomADVariant(Base):
    __tablename__ = "gnomad_variants"

    id = Column(Integer, primary_key=True)

    db_name = Column(String, nullable=False)
    db_identifier = Column(String, nullable=False, index=True)
    db_version = Column(String, nullable=False)

    allele_count = Column(Integer, nullable=False)
    allele_number = Column(Integer, nullable=False)
    allele_frequency = Column(Float, nullable=False)

    faf95_max = Column(Float, nullable=False)
    faf95_max_ancestry = Column(String, nullable=False)

    creation_date = Column(Date, nullable=False, default=date.today)
    modification_date = Column(Date, nullable=False, default=date.today, onupdate=date.today)

    mapped_variants: Mapped[list["MappedVariant"]] = relationship(
        "MappedVariant",
        secondary=gnomad_variants_mapped_variants_association_table,
        back_populates="gnomad_variants",
    )
