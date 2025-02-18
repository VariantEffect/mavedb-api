from sqlalchemy import Column, Table, ForeignKey

from mavedb.db.base import Base


mapped_variants_clinical_controls_association_table = Table(
    "mapped_variants_clinical_controls",
    Base.metadata,
    Column("mapped_variant_id", ForeignKey("mapped_variants.id"), primary_key=True),
    Column("clinical_control_id", ForeignKey("clinical_controls.id"), primary_key=True),
)
