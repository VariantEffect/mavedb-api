from datetime import date
from typing import TYPE_CHECKING

from sqlalchemy import CheckConstraint, Column, Date, Enum, ForeignKey, ForeignKeyConstraint, Integer, String, event
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, Session, backref, relationship

from mavedb.db.base import Base
from mavedb.models.enums.target_category import TargetCategory
from mavedb.models.score_set import ScoreSet
from mavedb.models.target_accession import TargetAccession
from mavedb.models.target_sequence import TargetSequence

if TYPE_CHECKING:
    from mavedb.models.target_gene_external_identifier import TargetGeneExternalIdentifier
    from mavedb.models.target_gene_mapping import TargetGeneMapping

# TODO Reformat code without removing dependencies whose use is not detected.


class TargetGene(Base):
    __tablename__ = "target_genes"
    __table_args__ = (
        # A target is a sequence or an accession. Each is owned by one target gene and carries its score set.
        # These keys prevent the carried score set differing from the gene's (#809).
        CheckConstraint("num_nonnulls(target_sequence_id, accession_id) = 1", name="target_genes_one_target"),
        ForeignKeyConstraint(
            ["target_sequence_id", "scoreset_id"],
            ["target_sequences.id", "target_sequences.score_set_id"],
            name="target_genes_target_sequence_score_set_fkey",
        ),
        ForeignKeyConstraint(
            ["accession_id", "scoreset_id"],
            ["target_accessions.id", "target_accessions.score_set_id"],
            name="target_genes_accession_score_set_fkey",
        ),
    )

    id: Mapped[int] = Column(Integer, primary_key=True)

    name = Column(String, nullable=False)
    category: Mapped[TargetCategory] = Column(
        Enum(TargetCategory, create_constraint=True, length=32, native_enum=False, validate_strings=True),
        nullable=False,
    )

    score_set_id = Column("scoreset_id", Integer, ForeignKey("scoresets.id"), index=True, nullable=False)
    score_set: Mapped[ScoreSet] = relationship(back_populates="target_genes", single_parent=True, uselist=True)

    target_sequence_id = Column(Integer, index=True, nullable=True)
    accession_id = Column(Integer, index=True, nullable=True)
    target_sequence: Mapped[TargetSequence] = relationship(
        back_populates="target_genes",
        cascade="all,delete-orphan",
        single_parent=True,
        primaryjoin="TargetGene.target_sequence_id == TargetSequence.id",
        foreign_keys=[target_sequence_id],
    )
    target_accession: Mapped[TargetAccession] = relationship(
        "TargetAccession",
        backref=backref("target_genes", single_parent=True, uselist=True),
        cascade="all,delete-orphan",
        single_parent=True,
        primaryjoin="TargetGene.accession_id == TargetAccession.id",
        foreign_keys=[accession_id],
    )

    pre_mapped_metadata: Mapped[JSONB] = Column("pre_mapped_metadata", JSONB, nullable=True)
    post_mapped_metadata: Mapped[JSONB] = Column("post_mapped_metadata", JSONB, nullable=True)
    mapped_hgnc_name = Column(String, nullable=True)
    uniprot_id_from_mapped_metadata = Column(String, nullable=True)

    creation_date = Column(Date, nullable=False, default=date.today)
    modification_date = Column(Date, nullable=False, default=date.today, onupdate=date.today)

    external_identifiers: Mapped[list["TargetGeneExternalIdentifier"]] = relationship(
        back_populates="target_gene",
        cascade="all, delete-orphan",
        order_by="TargetGeneExternalIdentifier.db_name",
    )

    target_gene_mappings: Mapped[list["TargetGeneMapping"]] = relationship(
        "TargetGeneMapping",
        back_populates="target_gene",
        cascade="all, delete-orphan",
    )


@event.listens_for(Session, "before_flush")
def _carry_score_set_to_new_targets(session: Session, flush_context, instances) -> None:
    """Give each new target sequence or accession its target gene's score set before it's inserted."""
    for target_gene in [*session.new, *session.dirty]:
        if not isinstance(target_gene, TargetGene):
            continue
        for target in (target_gene.target_sequence, target_gene.target_accession):
            if target is None or target not in session.new or target.score_set_id is not None:
                continue
            if target_gene.score_set_id is not None:
                target.score_set_id = target_gene.score_set_id
            # A score set created in the same flush has no id yet; the relationship orders the inserts.
            else:
                score_set = target_gene.score_set
                target.score_set = score_set[0] if isinstance(score_set, list) else score_set
