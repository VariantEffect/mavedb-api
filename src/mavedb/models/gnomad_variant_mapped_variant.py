from sqlalchemy import Column, Table, ForeignKey

from mavedb.db.base import Base


gnomad_variants_mapped_variants_association_table = Table(
    "gnomad_variants_mapped_variants",
    Base.metadata,
    Column("mapped_variant_id", ForeignKey("mapped_variants.id"), primary_key=True),
    Column("gnomad_variant_id", ForeignKey("gnomad_variants.id"), primary_key=True),
)
