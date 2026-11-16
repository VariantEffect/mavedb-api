from typing import TYPE_CHECKING

from sqlalchemy import Column, ForeignKeyConstraint, Index, Integer, String
from sqlalchemy.orm import Mapped, relationship

from mavedb.db.base import Base
from mavedb.models.mixins.valid_time import ValidTime

if TYPE_CHECKING:
    from .mapping_record import MappingRecord


class ProjectionGroup(ValidTime, Base):
    """One variant described at the coding, genomic and protein levels, within one mapping record.

    A record has one *measured* group, the one its authoritative allele link points at, holding the
    projections of the variant it measured; and one *candidate* group per pair reverse translation
    constructs from the protein consequence. Allele links join a group through
    ``MappingRecordAllele.projection_group``.

    The HGVS strings are the group's description, independent of whether each level has an allele: an
    intronic or UTR-spanning coding projection has no VRS allele (the transcript has no sequence there)
    but is still the variant's coding coordinate. Where a level does have an allele, its HGVS repeats the
    string here.
    """

    __tablename__ = "projection_groups"

    id: Mapped[int] = Column(Integer, primary_key=True)
    mapping_record_id: Mapped[int] = Column(Integer, nullable=False)

    # Denormalized from the mapping record for the RLS policy, like MappingRecord.score_set_id (#833).
    score_set_id: Mapped[int] = Column(Integer, nullable=False)

    hgvs_c = Column(String, nullable=True)
    hgvs_g = Column(String, nullable=True)
    # The protein consequence, shared by every group of a record. Null where the variant has none (intronic,
    # UTR) or it could not be determined.
    hgvs_p = Column(String, nullable=True)

    mapping_record: Mapped["MappingRecord"] = relationship("MappingRecord", back_populates="projection_groups")

    __table_args__ = (
        ForeignKeyConstraint(
            [mapping_record_id, score_set_id],
            ["mapping_records.id", "mapping_records.score_set_id"],
            name="fk_projection_groups_mapping_record_score_set",
            ondelete="CASCADE",
        ),
        Index("ix_projection_groups_mapping_record_id", "mapping_record_id"),
        # Target of mapping_record_alleles' composite foreign key, which keeps a link inside its own
        # record's groups.
        Index("uq_projection_groups_id_mapping_record", "id", "mapping_record_id", unique=True),
    )
