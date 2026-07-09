from typing import TYPE_CHECKING

from sqlalchemy import Boolean, Column, ForeignKey, Index, Integer, text
from sqlalchemy.orm import Mapped, relationship

from mavedb.db.base import Base
from mavedb.db.mixins import ValidTime

if TYPE_CHECKING:
    from .allele import Allele
    from .mapping_record import MappingRecord


class MappingRecordAllele(ValidTime, Base):
    __tablename__ = "mapping_record_alleles"

    id: Mapped[int] = Column(Integer, primary_key=True)
    mapping_record_id: Mapped[int] = Column(
        Integer,
        ForeignKey("mapping_records.id", ondelete="CASCADE"),
        nullable=False,
    )
    allele_id: Mapped[int] = Column(
        Integer,
        ForeignKey("alleles.id", ondelete="RESTRICT"),
        nullable=False,
    )
    # Provenance: ``True`` when this allele was the assay's actual measurement
    # for this mapping record (sourced from vrs_map, with real alignment QC),
    # ``False`` when it was derived by the cross-level or same-level
    # synonymous translator. The flag lives on the link rather than on
    # ``Allele`` because the same VRS allele can be authoritative for one
    # mapping record and translator-derived for another.
    is_authoritative: Mapped[bool] = Column(Boolean, nullable=False, default=False)
    # Within-record projection grouping. The reverse-translation job fans a variant's protein
    # consequence out into projection pairs, each a coding/genomic pair (the same change expressed
    # at two levels). The two links of one pair share a ``projection_group`` value
    # (an integer index, 0..N-1, local to this mapping record), so we can reconstruct projections
    # onto transcripts for more precise provenance in consumers. ``NULL`` for links that belong to no
    # pair: the shared protein apex, and any link written before reverse translation has run.
    #
    # It is a grouping key, NOT a cross-record identity — regenerated whenever a record is
    # superseded and re-linked. Identity lives in the allele VRS digests; two records that happen to
    # share a transcript and codon re-encode the pairing independently. Serving resolves the
    # canonical projection by pairing this with ``is_authoritative``: the authoritative (measured)
    # allele's group gathers its sibling links, yielding its c/g projection. See the RT job for the
    # group-assignment and authoritative fold-in logic.
    projection_group = Column(Integer, nullable=True)

    mapping_record: Mapped["MappingRecord"] = relationship("MappingRecord", back_populates="allele_links")
    allele: Mapped["Allele"] = relationship("Allele", back_populates="mapping_record_links")

    __table_args__ = (
        Index(
            "ix_mapping_record_alleles_mapping_record_id",
            "mapping_record_id",
        ),
        Index(
            "ix_mapping_record_alleles_allele_id",
            "allele_id",
        ),
        # At most one live link per (mapping_record, allele). The job retires (closes valid_to)
        # rather than deletes, so superseded links remain for point-in-time queries; only the
        # live row participates in this constraint. A derived link is never written when an
        # authoritative one already exists for the pair, so is_authoritative is intentionally
        # absent from the key — a record links a given allele once.
        Index(
            "uq_mapping_record_alleles_live",
            "mapping_record_id",
            "allele_id",
            unique=True,
            postgresql_where=text("valid_to IS NULL"),
        ),
        # At most one live *authoritative* link per mapping record. The index above keys on
        # (record, allele) and so cannot stop two different alleles both being flagged authoritative
        # for one record; this one enforces the "one authoritative (measured) allele per record"
        # invariant directly. It is the backstop the serving layer relies on: the lean whole-set view
        # (lib/score_set_variants) inner-joins the authoritative link expecting it 1:1 with the
        # variant, so a second live authoritative link would silently duplicate the variant (and
        # double-count its score) rather than raise. With this index, that mistake fails loud in the
        # mapping job at write time instead.
        Index(
            "uq_mapping_record_alleles_live_authoritative",
            "mapping_record_id",
            unique=True,
            postgresql_where=text("is_authoritative AND valid_to IS NULL"),
        ),
    )
