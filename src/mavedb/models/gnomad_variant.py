from datetime import date
from typing import TYPE_CHECKING

from sqlalchemy import Column, Date, Float, Integer, String
from sqlalchemy.orm import Mapped, relationship

from mavedb.db.base import Base
from mavedb.models.gnomad_variant_mapped_variant import gnomad_variants_mapped_variants_association_table

if TYPE_CHECKING:
    from mavedb.models.gnomad_allele_link import GnomadAlleleLink
    from mavedb.models.mapped_variant import MappedVariant


class GnomADVariant(Base):
    __tablename__ = "gnomad_variants"

    id: Mapped[int] = Column(Integer, primary_key=True)

    db_name = Column(String, nullable=False)
    db_identifier = Column(String, nullable=False, index=True)
    db_version = Column(String, nullable=False)

    allele_count = Column(Integer, nullable=False)
    allele_number = Column(Integer, nullable=False)
    allele_frequency = Column(Float, nullable=False)

    faf95_max = Column(Float, nullable=True)
    faf95_max_ancestry = Column(String, nullable=True)

    creation_date = Column(Date, nullable=False, default=date.today)
    modification_date = Column(Date, nullable=False, default=date.today, onupdate=date.today)

    # Frozen association to the old MappedVariant model — read by serving for existing data, never
    # written for new score sets (which link through ``allele_links`` instead).
    mapped_variants: Mapped[list["MappedVariant"]] = relationship(
        "MappedVariant",
        secondary=gnomad_variants_mapped_variants_association_table,
        back_populates="gnomad_variants",
    )

    # Valid-time links to deduplicated alleles (new-model writes).
    allele_links: Mapped[list["GnomadAlleleLink"]] = relationship(
        "GnomadAlleleLink",
        back_populates="gnomad_variant",
    )
