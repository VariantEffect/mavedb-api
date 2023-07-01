from datetime import date
from sqlalchemy import Boolean, Column, Date, Enum, ForeignKey, Integer, String
from sqlalchemy.orm import relationship, backref
from sqlalchemy.ext.associationproxy import association_proxy
from sqlalchemy.schema import Table

from mavedb.db.base import Base
from mavedb.deps import JSONB
from mavedb.models.enums.processing_state import ProcessingState
from mavedb.models.score_set_publication_identifier import ScoreSetPublicationIdentifierAssociation
from .keyword import Keyword

# from .raw_read_identifier import SraIdentifier
from mavedb.lib.temp_urns import generate_temp_urn

# TODO Reformat code without removing dependencies whose use is not detected.

score_sets_doi_identifiers_association_table = Table(
    "scoreset_doi_identifiers",
    Base.metadata,
    Column("scoreset_id", ForeignKey("scoresets.id"), primary_key=True),
    Column("doi_identifier_id", ForeignKey("doi_identifiers.id"), primary_key=True),
)


score_sets_keywords_association_table = Table(
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


class ScoreSet(Base):
    __tablename__ = "scoresets"

    id = Column(Integer, primary_key=True, index=True)

    urn = Column(String(64), nullable=True, default=generate_temp_urn)  # index=True, nullable=True
    title = Column(String, nullable=False)
    method_text = Column(String, nullable=False)
    abstract_text = Column(String, nullable=False)
    short_description = Column(String, nullable=False)
    extra_metadata = Column(JSONB, nullable=False)
    dataset_columns = Column(JSONB, nullable=False, default={})

    normalised = Column(Boolean, nullable=False, default=False)
    private = Column(Boolean, nullable=False, default=True)
    approved = Column(Boolean, nullable=False, default=False)
    published_date = Column(Date, nullable=True)
    processing_state = Column(
        Enum(ProcessingState, create_constraint=True, length=32, native_enum=False, validate_strings=True),
        nullable=True,
    )
    data_usage_policy = Column(String, nullable=True)

    # TODO Refactor the way we track the number of variants?
    num_variants = Column(Integer, nullable=False, default=0)

    experiment_id = Column(Integer, ForeignKey("experiments.id"), nullable=False)
    experiment = relationship("Experiment", backref=backref("score_sets", cascade="all,delete-orphan"))
    # TODO Standardize on US or GB spelling for licenc/se.
    licence_id = Column(Integer, ForeignKey("licenses.id"), nullable=False)
    license = relationship("License")
    superseded_score_set_id = Column("replaces_id", Integer, ForeignKey("scoresets.id"), nullable=True)  # TODO
    superseded_score_set = relationship(
        "ScoreSet", uselist=False, remote_side=[id], backref=backref("superseding_score_set", uselist=False)
    )

    created_by_id = Column(Integer, ForeignKey("users.id"), nullable=True)
    created_by = relationship("User", foreign_keys="ScoreSet.created_by_id")
    modified_by_id = Column(Integer, ForeignKey("users.id"), nullable=True)
    modified_by = relationship("User", foreign_keys="ScoreSet.modified_by_id")
    creation_date = Column(Date, nullable=False, default=date.today)
    modification_date = Column(Date, nullable=False, default=date.today, onupdate=date.today)

    keyword_objs = relationship("Keyword", secondary=score_sets_keywords_association_table, backref="score_sets")
    doi_identifiers = relationship(
        "DoiIdentifier", secondary=score_sets_doi_identifiers_association_table, backref="score_sets"
    )
    publication_identifier_associations = relationship(
        "ScoreSetPublicationIdentifierAssociation", back_populates="score_set", cascade="all, delete-orphan"
    )
    publication_identifiers = association_proxy(
        "publication_identifier_associations",
        "publication",
        creator=lambda p: ScoreSetPublicationIdentifierAssociation(publication=p, primary=p.primary),
    )
    # sra_identifiers = relationship('SraIdentifier', secondary=score_sets_sra_identifiers_association_table, backref='score_sets')
    # raw_read_identifiers = relationship('RawReadIdentifier', secondary=score_sets_raw_read_identifiers_association_table,backref='score_sets')
    meta_analyzes_score_sets = relationship(
        "ScoreSet",
        secondary=score_sets_meta_analysis_score_sets_association_table,
        primaryjoin=(score_sets_meta_analysis_score_sets_association_table.c.meta_analysis_scoreset_id == id),
        secondaryjoin=(score_sets_meta_analysis_score_sets_association_table.c.source_scoreset_id == id),
        backref="meta_analyzed_by_score_sets",
    )

    # Unfortunately, we can't use association_proxy here, because in spite of what the documentation seems to imply, it
    # doesn't check for a pre-existing keyword with the same text.
    # keywords = association_proxy('keyword_objs', 'text', creator=lambda text: Keyword(text=text))

    # _updated_keywords: list[str] = None
    # _updated_doi_identifiers: list[str] = None

    @property
    def keywords(self) -> list[str]:
        # if self._updated_keywords:
        #     return self._updated_keywords
        # else:
        keyword_objs = self.keyword_objs or []  # getattr(self, 'keyword_objs', [])
        return list(map(lambda keyword_obj: keyword_obj.text, keyword_objs))

    async def set_keywords(self, db, keywords: list[str]):
        if keywords is None:
            self.keyword_objs = []
        else:
            self.keyword_objs = [await self._find_or_create_keyword(db, text) for text in keywords]

    # See https://gist.github.com/tachyondecay/e0fe90c074d6b6707d8f1b0b1dcc8e3a
    # @keywords.setter
    # async def set_keywords(self, db, keywords: list[str]):
    #     self._keyword_objs = [await self._find_or_create_keyword(text) for text in keywords]

    async def _find_or_create_keyword(self, db, keyword_text):
        keyword_obj = db.query(Keyword).filter(Keyword.text == keyword_text).one_or_none()
        if not keyword_obj:
            keyword_obj = Keyword(text=keyword_text)
            # object_session.add(keyword_obj)
        return keyword_obj
