from datetime import date
from typing import TYPE_CHECKING, Optional

from sqlalchemy import Boolean, Column, Date, Enum, ForeignKey, Index, Integer, String, text
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, relationship

from mavedb.db.base import Base
from mavedb.models.clinical_control_mapped_variant import mapped_variants_clinical_controls_association_table
from mavedb.models.enums.annotation_layer import AnnotationLayer
from mavedb.models.gnomad_variant_mapped_variant import gnomad_variants_mapped_variants_association_table

if TYPE_CHECKING:
    from .clinical_control import ClinvarControl
    from .gnomad_variant import GnomADVariant
    from .target_gene_mapping import TargetGeneMapping
    from .variant import Variant


class MappedVariant(Base):
    __tablename__ = "mapped_variants"

    id = Column(Integer, primary_key=True)

    pre_mapped = Column(JSONB(none_as_null=True), nullable=True)
    post_mapped = Column(JSONB(none_as_null=True), nullable=True)
    vrs_version = Column(String, nullable=True)
    error_message = Column(String, nullable=True)
    modification_date = Column(Date, nullable=False, default=date.today, onupdate=date.today)
    mapped_date = Column(Date, nullable=False)
    mapping_api_version = Column(String, nullable=False)
    current = Column(Boolean, nullable=False)

    variant_id = Column(Integer, ForeignKey("variants.id"), index=True, nullable=False)
    variant: Mapped["Variant"] = relationship("Variant", back_populates="mapped_variants")

    # FK to the per-(target gene, alignment_level) QC record produced for this
    # variant's mapping. Nullable only for legacy rows that the backfill script
    # could not attribute; all new mappings always have this set.
    target_gene_mapping_id = Column(Integer, ForeignKey("target_gene_mappings.id"), index=True, nullable=True)
    target_gene_mapping: Mapped[Optional["TargetGeneMapping"]] = relationship(
        "TargetGeneMapping", back_populates="mapped_variants"
    )

    # Per-mapping QC annotations from dcd-mapping ScoreAnnotation.
    alignment_level = Column(
        Enum(AnnotationLayer, create_constraint=True, length=16, native_enum=False, validate_strings=True),
        nullable=True,
    )
    at_mismatched_locus = Column(Boolean, nullable=True)
    near_gap = Column(Boolean, nullable=True)

    clingen_allele_id = Column(String, index=True, nullable=True)

    vep_functional_consequence = Column(String, nullable=True)
    vep_access_date = Column(Date, nullable=True)

    # mapped hgvs
    hgvs_assay_level = Column(String, nullable=True)
    hgvs_g = Column(String, nullable=True)
    hgvs_c = Column(String, nullable=True)
    hgvs_p = Column(String, nullable=True)

    clinical_controls: Mapped[list["ClinvarControl"]] = relationship(
        "ClinvarControl",
        secondary=mapped_variants_clinical_controls_association_table,
        back_populates="mapped_variants",
    )
    gnomad_variants: Mapped[list["GnomADVariant"]] = relationship(
        "GnomADVariant",
        secondary=gnomad_variants_mapped_variants_association_table,
        back_populates="mapped_variants",
    )

    __table_args__ = (
        Index("ix_mapped_variants_pre_mapped_id", text("(pre_mapped->>'id')")),
        Index("ix_mapped_variants_post_mapped_id", text("(post_mapped->>'id')")),
    )
