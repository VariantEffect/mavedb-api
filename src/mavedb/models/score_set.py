from datetime import date
from sqlalchemy import Boolean, Column, Date, Enum, ForeignKey, Integer, String
from sqlalchemy.orm import relationship, backref, Mapped
from sqlalchemy.ext.associationproxy import association_proxy, AssociationProxy
from sqlalchemy.schema import Table
from sqlalchemy.dialects.postgresql import JSONB
from alembic_utils.pg_materialized_view import PGMaterializedView

from typing import List, TYPE_CHECKING, Optional

from mavedb.db.base import Base
from mavedb.models.enums.mapping_state import MappingState
from mavedb.models.enums.processing_state import ProcessingState
import mavedb.models.score_set_publication_identifier

from mavedb.models.contributor import Contributor
from mavedb.models.experiment import Experiment
from mavedb.models.user import User
from mavedb.models.license import License
from mavedb.models.legacy_keyword import LegacyKeyword
from mavedb.models.doi_identifier import DoiIdentifier
from mavedb.models.publication_identifier import PublicationIdentifier

if TYPE_CHECKING:
    from mavedb.models.variant import Variant
    from mavedb.models.target_gene import TargetGene

# from .raw_read_identifier import SraIdentifier
from mavedb.lib.temp_urns import generate_temp_urn

# TODO Reformat code without removing dependencies whose use is not detected.

score_sets_contributors_association_table = Table(
    "scoreset_contributors",
    Base.metadata,
    Column("scoreset_id", ForeignKey("scoresets.id"), primary_key=True),
    Column("contributor_id", ForeignKey("contributors.id"), primary_key=True),
)


score_sets_doi_identifiers_association_table = Table(
    "scoreset_doi_identifiers",
    Base.metadata,
    Column("scoreset_id", ForeignKey("scoresets.id"), primary_key=True),
    Column("doi_identifier_id", ForeignKey("doi_identifiers.id"), primary_key=True),
)


score_sets_legacy_keywords_association_table = Table(
    "scoreset_keywords",
    Base.metadata,
    Column("scoreset_id", ForeignKey("scoresets.id"), primary_key=True),
    Column("keyword_id", ForeignKey("keywords.id"), primary_key=True),
)


score_sets_meta_analysis_score_sets_association_table = Table(
    "scoreset_meta_analysis_sources",
    Base.metadata,
    Column("source_scoreset_id", ForeignKey("scoresets.id"), primary_key=True),
    Column("meta_analysis_scoreset_id", ForeignKey("scoresets.id"), primary_key=True),
)


# score_sets_sra_identifiers_association_table = Table(
score_sets_raw_read_identifiers_association_table = Table(
    "scoreset_sra_identifiers",
    Base.metadata,
    Column("scoreset_id", ForeignKey("scoresets.id"), primary_key=True),
    Column("sra_identifier_id", ForeignKey("sra_identifiers.id"), primary_key=True),
)

# TODO(#94): add LICENSE, plus TAX_ID if numeric
# TODO(#89): The query below should be generated from SQLAlchemy
#            models rather than hand-carved SQL

scoreset_fulltext = PGMaterializedView(
    schema="public",
    signature="scoreset_fulltext",
    definition=' union ' .join(
        [
            f"select id, to_tsvector({c}) as text from scoresets"
            for c in ('urn', 'title', 'short_description', 'abstract_text')
        ] + [
            f"select scoreset_id, to_tsvector({c}) as text from target_genes"
            for c in ('name', 'category')
        ] + [
            f"select scoreset_id, to_tsvector(TX.{c}) as text from target_genes TG join target_sequences TS on (TG.target_sequence_id = TS.id) join taxonomies TX on (TS.taxonomy_id = TX.id)"
            for c in ('organism_name', 'common_name')
        ] + [
            f"select scoreset_id, to_tsvector(TA.assembly) as text from target_genes TG join target_accessions TA on (TG.accession_id = TA.id)"
        ] + [
            f"select scoreset_id, to_tsvector(PI.{c}) as text from scoreset_publication_identifiers SPI JOIN publication_identifiers PI ON (SPI.publication_identifier_id = PI.id)"
            for c in ('identifier', 'doi', 'abstract', 'title', 'publication_journal')
        ] + [
            f"select scoreset_id, to_tsvector(jsonb_array_elements(authors)->'name') as text from scoreset_publication_identifiers SPI join publication_identifiers PI on SPI.publication_identifier_id = PI.id",
            f"select scoreset_id, to_tsvector(DI.identifier) as text from scoreset_doi_identifiers SD join doi_identifiers DI on (SD.doi_identifier_id = DI.id)",
        ] + [
            f"select scoreset_id, to_tsvector(XI.identifier) as text from target_genes TG join {x}_offsets XO on (XO.target_gene_id = TG.id) join {x}_identifiers XI on (XI.id = XO.identifier_id)"
            for x in ('uniprot', 'refseq', 'ensembl')
        ]
    ),
    with_data=True
)

class ScoreSetFullText(Base):
    __table__ = Table(
        "scoreset_fulltext",
        Base.metadata,
        Column("id", Integer, primary_key=True),
        Column("text", String)
    )

class ScoreSet(Base):
    __tablename__ = "scoresets"

    id = Column(Integer, primary_key=True)

    urn = Column(String(64), default=generate_temp_urn, index=True, nullable=True, unique=True)
    title = Column(String, nullable=False)
    method_text = Column(String, nullable=False)
    abstract_text = Column(String, nullable=False)
    short_description = Column(String, nullable=False)
    extra_metadata = Column(JSONB, nullable=False)
    dataset_columns = Column(JSONB, nullable=False, default={})
    external_links = Column(JSONB, nullable=False, default={})

    normalised = Column(Boolean, nullable=False, default=False)
    private = Column(Boolean, nullable=False, default=True)
    approved = Column(Boolean, nullable=False, default=False)
    published_date = Column(Date, nullable=True)
    processing_state = Column(
        Enum(ProcessingState, create_constraint=True, length=32, native_enum=False, validate_strings=True),
        nullable=True,
    )
    processing_errors = Column(JSONB, nullable=True)
    data_usage_policy = Column(String, nullable=True)

    num_variants = Column(Integer, nullable=False, default=0)
    variants: Mapped[list["Variant"]] = relationship(back_populates="score_set", cascade="all, delete-orphan")

    mapping_state = Column(
        Enum(MappingState, create_constraint=True, length=32, native_enum=False, validate_strings=True),
        nullable=True,
    )
    mapping_errors = Column(JSONB, nullable=True)

    experiment_id = Column(Integer, ForeignKey("experiments.id"), index=True, nullable=False)
    experiment: Mapped["Experiment"] = relationship(back_populates="score_sets")

    # TODO Standardize on US or GB spelling for licenc/se.
    licence_id = Column(Integer, ForeignKey("licenses.id"), index=True, nullable=False)
    license: Mapped["License"] = relationship("License")
    superseded_score_set_id = Column("replaces_id", Integer, ForeignKey("scoresets.id"), index=True, nullable=True)
    superseded_score_set: Mapped[Optional["ScoreSet"]] = relationship(
        "ScoreSet", uselist=False, foreign_keys="ScoreSet.superseded_score_set_id", remote_side=[id]
    )
    superseding_score_set: Mapped[Optional["ScoreSet"]] = relationship(
        "ScoreSet", uselist=False, back_populates="superseded_score_set"
    )

    created_by_id = Column(Integer, ForeignKey("users.id"), index=True, nullable=True)
    created_by: Mapped["User"] = relationship("User", foreign_keys="ScoreSet.created_by_id")
    modified_by_id = Column(Integer, ForeignKey("users.id"), index=True, nullable=True)
    modified_by: Mapped["User"] = relationship("User", foreign_keys="ScoreSet.modified_by_id")
    creation_date = Column(Date, nullable=False, default=date.today)
    modification_date = Column(Date, nullable=False, default=date.today, onupdate=date.today)

    legacy_keyword_objs: Mapped[list["LegacyKeyword"]] = relationship(
        "LegacyKeyword", secondary=score_sets_legacy_keywords_association_table, backref="score_sets"
    )
    contributors: Mapped[list["Contributor"]] = relationship(
        "Contributor", secondary=score_sets_contributors_association_table, backref="score_sets"
    )
    doi_identifiers: Mapped[list["DoiIdentifier"]] = relationship(
        "DoiIdentifier", secondary=score_sets_doi_identifiers_association_table, backref="score_sets"
    )
    publication_identifier_associations: Mapped[
        list[mavedb.models.score_set_publication_identifier.ScoreSetPublicationIdentifierAssociation]
    ] = relationship(
        "ScoreSetPublicationIdentifierAssociation", back_populates="score_set", cascade="all, delete-orphan"
    )
    publication_identifiers: AssociationProxy[List[PublicationIdentifier]] = association_proxy(
        "publication_identifier_associations",
        "publication",
        creator=lambda p: mavedb.models.score_set_publication_identifier.ScoreSetPublicationIdentifierAssociation(
            publication=p, primary=p.primary
        ),
    )
    # sra_identifiers = relationship('SraIdentifier', secondary=score_sets_sra_identifiers_association_table, backref='score_sets')
    # raw_read_identifiers = relationship('RawReadIdentifier', secondary=score_sets_raw_read_identifiers_association_table,backref='score_sets')
    meta_analyzes_score_sets: Mapped[list["ScoreSet"]] = relationship(
        "ScoreSet",
        secondary=score_sets_meta_analysis_score_sets_association_table,
        primaryjoin=(score_sets_meta_analysis_score_sets_association_table.c.meta_analysis_scoreset_id == id),
        secondaryjoin=(score_sets_meta_analysis_score_sets_association_table.c.source_scoreset_id == id),
        backref="meta_analyzed_by_score_sets",
    )

    target_genes: Mapped[List["TargetGene"]] = relationship(back_populates="score_set", cascade="all, delete-orphan")

    # Unfortunately, we can't use association_proxy here, because in spite of what the documentation seems to imply, it
    # doesn't check for a pre-existing keyword with the same text.
    # keywords = association_proxy('keyword_objs', 'text', creator=lambda text: Keyword(text=text))

    # _updated_keywords: list[str] = None
    # _updated_doi_identifiers: list[str] = None

    @property
    def legacy_keywords(self) -> list[str]:
        # if self._updated_keywords:
        #     return self._updated_keywords
        # else:
        legacy_keyword_objs = self.legacy_keyword_objs or []  # getattr(self, 'keyword_objs', [])
        return [legacy_keyword_obj.text for legacy_keyword_obj in legacy_keyword_objs if legacy_keyword_obj.text is not None]

    async def set_legacy_keywords(self, db, keywords: list[str]):
        self.keyword_objs = [await self._find_or_create_legacy_keyword(db, text) for text in keywords]

    # See https://gist.github.com/tachyondecay/e0fe90c074d6b6707d8f1b0b1dcc8e3a
    # @keywords.setter
    # async def set_keywords(self, db, keywords: list[str]):
    #     self._keyword_objs = [await self._find_or_create_keyword(text) for text in keywords]

    async def _find_or_create_legacy_keyword(self, db, keyword_text):
        keyword_obj = db.query(LegacyKeyword).filter(LegacyKeyword.text == keyword_text).one_or_none()
        if not keyword_obj:
            keyword_obj = LegacyKeyword(text=keyword_text)
        return keyword_obj
