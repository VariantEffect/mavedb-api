from datetime import date
from sqlalchemy import Boolean, Column, Date, Enum, ForeignKey, Integer, String
from sqlalchemy.orm import relationship
from sqlalchemy.schema import Table

from app.db.base import Base
from app.deps import JSONB
from app.models.enums.processing_state import ProcessingState
from .doi_identifier import DoiIdentifier
from .keyword import Keyword
from .pubmed_identifier import PubmedIdentifier
from .sra_identifier import SraIdentifier
from .variant import Variant
from app.lib.temp_urns import generate_temp_urn

scoresets_doi_identifiers_association_table = Table(
    'dataset_scoreset_doi_ids',
    Base.metadata,
    Column('scoreset_id', ForeignKey('dataset_scoreset.id'), primary_key=True),
    Column('doiidentifier_id', ForeignKey('metadata_doiidentifier.id'), primary_key=True)
)


scoresets_keywords_association_table = Table(
    'dataset_scoreset_keywords',
    Base.metadata,
    Column('scoreset_id', ForeignKey('dataset_scoreset.id'), primary_key=True),
    Column('keyword_id', ForeignKey('metadata_keyword.id'), primary_key=True)
)


scoresets_meta_analysis_scoresets_association_table = Table(
    'dataset_scoreset_meta_analysis_for',
    Base.metadata,
    Column('from_scoreset_id', ForeignKey('dataset_scoreset.id'), primary_key=True),
    Column('to_scoreset_id', ForeignKey('dataset_scoreset.id'), primary_key=True)
)


scoresets_pubmed_identifiers_association_table = Table(
    'dataset_scoreset_pubmed_ids',
    Base.metadata,
    Column('scoreset_id', ForeignKey('dataset_scoreset.id'), primary_key=True),
    Column('pubmedidentifier_id', ForeignKey('metadata_pubmedidentifier.id'), primary_key=True)
)


scoresets_sra_identifiers_association_table = Table(
    'dataset_scoreset_sra_ids',
    Base.metadata,
    Column('scoreset_id', ForeignKey('dataset_scoreset.id'), primary_key=True),
    Column('sraidentifier_id', ForeignKey('metadata_sraidentifier.id'), primary_key=True)
)


class Scoreset(Base):
    __tablename__ = 'dataset_scoreset'

    id = Column(Integer, primary_key=True, index=True)

    urn = Column(String(64), nullable=True, default=generate_temp_urn)  # index=True, nullable=True
    title = Column(String(250), nullable=False)
    method_text = Column(String, nullable=False)
    abstract_text = Column(String, nullable=False)
    short_description = Column(String, nullable=False)
    extra_metadata = Column(JSONB, nullable=False)
    dataset_columns = Column(JSONB, nullable=False, default={})

    normalised = Column(Boolean, nullable=False, default=False)
    private = Column(Boolean, nullable=False, default=True)
    approved = Column(Boolean, nullable=False, default=False)
    published_date = Column('publish_date', Date, nullable=True)
    processing_state = Column(
        Enum(ProcessingState, create_constraint=True, length=32, native_enum=False, validate_strings=True),
        nullable=True
    )
    data_usage_policy = Column(String, nullable=True)

    # TODO Refactor the way we track the number of variants?
    num_variants = Column('last_child_value', Integer, nullable=False, default=0)

    experiment_id = Column(Integer, ForeignKey('dataset_experiment.id'), nullable=False)
    experiment = relationship('Experiment', back_populates='scoresets')
    licence_id = Column(Integer, nullable=True)  # TODO
    replaces_id = Column(Integer, nullable=True)  # TODO

    created_by_id = Column(Integer, ForeignKey('auth_user.id'), nullable=True)
    created_by = relationship('User', foreign_keys='Scoreset.created_by_id')
    modified_by_id = Column(Integer, ForeignKey('auth_user.id'), nullable=True)
    modified_by = relationship('User', foreign_keys='Scoreset.modified_by_id')
    creation_date = Column(Date, nullable=False, default=date.today)
    modification_date = Column(Date, nullable=False, default=date.today, onupdate=date.today)

    variants = relationship('Variant', back_populates='scoreset')
    # target_gene = relationship('TargetGene', back_populates='scoreset', uselist=False)
    keyword_objs = relationship('Keyword', secondary=scoresets_keywords_association_table, backref='scoresets')
    doi_identifiers = relationship('DoiIdentifier', secondary=scoresets_doi_identifiers_association_table, backref='scoresets')
    pubmed_identifiers = relationship('PubmedIdentifier', secondary=scoresets_pubmed_identifiers_association_table, backref='scoresets')
    sra_identifiers = relationship('SraIdentifier', secondary=scoresets_sra_identifiers_association_table, backref='scoresets')
    meta_analysis_for = relationship(
        'Scoreset',
        secondary=scoresets_meta_analysis_scoresets_association_table,
        primaryjoin=(scoresets_meta_analysis_scoresets_association_table.c.from_scoreset_id == id),
        secondaryjoin=(scoresets_meta_analysis_scoresets_association_table.c.to_scoreset_id == id),
        backref='meta_analyses'
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
