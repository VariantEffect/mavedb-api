from typing import TYPE_CHECKING, Optional

from sqlalchemy import Boolean, Column, ForeignKey, ForeignKeyConstraint, Index, Integer, text
from sqlalchemy.orm import Mapped, relationship

from mavedb.db.base import Base
from mavedb.models.mixins.valid_time import ValidTime

if TYPE_CHECKING:
    from .allele import Allele
    from .mapping_record import MappingRecord
    from .projection_group import ProjectionGroup


class MappingRecordAllele(ValidTime, Base):
    __tablename__ = "mapping_record_alleles"

    id: Mapped[int] = Column(Integer, primary_key=True)
    mapping_record_id: Mapped[int] = Column(Integer, nullable=False)
    # Denormalized from the mapping record for the RLS policy, like MappingRecord.score_set_id (#833).
    score_set_id: Mapped[int] = Column(Integer, nullable=False)
    allele_id: Mapped[int] = Column(
        Integer,
        ForeignKey("alleles.id", ondelete="RESTRICT"),
        nullable=False,
    )
    # True for the assay's actual measured allele (from vrs_map); False for translator-derived
    # alleles. Lives on the link, not Allele, since the same allele can be authoritative for one
    # mapping record and derived for another.
    is_authoritative: Mapped[bool] = Column(Boolean, nullable=False, default=False)
    # The projection group this allele describes one level of. The authoritative link's group is the record's
    # measured group. NULL for an allele outside any group (the protein apex of reverse translation).
    projection_group = Column(Integer, nullable=True)

    mapping_record: Mapped["MappingRecord"] = relationship("MappingRecord", back_populates="allele_links")
    allele: Mapped["Allele"] = relationship("Allele", back_populates="mapping_record_links")
    # Read-only: writers set ``projection_group`` directly. A group row is never edited in place (a new
    # description is a new row, and its links are re-pointed), so this resolves to the group the link was
    # written against at any instant.
    group: Mapped[Optional["ProjectionGroup"]] = relationship(
        "ProjectionGroup",
        primaryjoin="foreign(MappingRecordAllele.projection_group) == ProjectionGroup.id",
        viewonly=True,
    )

    __table_args__ = (
        ForeignKeyConstraint(
            [mapping_record_id, score_set_id],
            ["mapping_records.id", "mapping_records.score_set_id"],
            name="fk_mapping_record_alleles_mapping_record_score_set",
            ondelete="CASCADE",
        ),
        # MATCH SIMPLE: a NULL projection_group is not checked.
        ForeignKeyConstraint(
            [projection_group, mapping_record_id],
            ["projection_groups.id", "projection_groups.mapping_record_id"],
            name="fk_mapping_record_alleles_projection_group",
            ondelete="CASCADE",
        ),
        Index(
            "ix_mapping_record_alleles_mapping_record_id",
            "mapping_record_id",
        ),
        Index(
            "ix_mapping_record_alleles_allele_id",
            "allele_id",
        ),
        Index("ix_mapping_record_alleles_projection_group", "projection_group"),
        # At most one live link per (mapping_record, allele); retired links (valid_to set) are
        # exempt so history is preserved.
        Index(
            "uq_mapping_record_alleles_live",
            "mapping_record_id",
            "allele_id",
            unique=True,
            postgresql_where=text("valid_to IS NULL"),
        ),
        # At most one live authoritative link per mapping record. lib/score_set_variants relies on
        # this 1:1 invariant (inner join on the authoritative link); a second one would silently
        # duplicate the variant and double-count its score.
        Index(
            "uq_mapping_record_alleles_live_authoritative",
            "mapping_record_id",
            unique=True,
            postgresql_where=text("is_authoritative AND valid_to IS NULL"),
        ),
    )
