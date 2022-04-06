from datetime import date
from sqlalchemy import Boolean, Column, Date, ForeignKey, Integer, String
from sqlalchemy.orm import relationship
from sqlalchemy.schema import Table

from app.db.base import Base
from app.deps import JSONB
from app.lib.urns import generate_temp_urn


experiment_sets_doi_identifiers_association_table = Table(
    'dataset_experimentset_doi_ids',
    Base.metadata,
    Column('experimentset_id', ForeignKey('dataset_experimentset.id'), primary_key=True),
    Column('doiidentifier_id', ForeignKey('metadata_doiidentifier.id'), primary_key=True)
)


experiment_sets_keywords_association_table = Table(
    'dataset_experimentset_keywords',
    Base.metadata,
    Column('experimentset_id', ForeignKey('dataset_experimentset.id'), primary_key=True),
    Column('keyword_id', ForeignKey('metadata_keyword.id'), primary_key=True)
)


experiment_sets_pubmed_identifiers_association_table = Table(
    'dataset_experimentset_pubmed_ids',
    Base.metadata,
    Column('experimentset_id', ForeignKey('dataset_experimentset.id'), primary_key=True),
    Column('pubmedidentifier_id', ForeignKey('metadata_pubmedidentifier.id'), primary_key=True)
)


experiment_sets_sra_identifiers_association_table = Table(
    'dataset_experimentset_sra_ids',
    Base.metadata,
    Column('experimentset_id', ForeignKey('dataset_experimentset.id'), primary_key=True),
    Column('sraidentifier_id', ForeignKey('metadata_sraidentifier.id'), primary_key=True)
)


class ExperimentSet(Base):
    __tablename__ = 'dataset_experimentset'

    id = Column(Integer, primary_key=True, index=True)

    urn = Column(String(64), nullable=True, default=generate_temp_urn)  # index=True, nullable=True
    title = Column(String(250), nullable=False)
    method_text = Column(String, nullable=False)
    abstract_text = Column(String, nullable=False)
    short_description = Column(String, nullable=False)
    extra_metadata = Column(JSONB, nullable=False)

    private = Column(Boolean, nullable=False, default=False)
    approved = Column(Boolean, nullable=False, default=False)
    published_date = Column('publish_date', Date, nullable=True)
    processing_state = Column(String(32), nullable=True)

    # TODO Refactor the way we handle child collections?
    num_experiments = Column('last_child_value', Integer, nullable=False, default=0)

    created_by_id = Column(Integer, ForeignKey('auth_user.id'), nullable=True)
    created_by = relationship('User', foreign_keys='ExperimentSet.created_by_id')
    modified_by_id = Column(Integer, ForeignKey('auth_user.id'), nullable=True)
    modified_by = relationship('User', foreign_keys='ExperimentSet.modified_by_id')
    creation_date = Column(Date, nullable=False, default=date.today)
    modification_date = Column(Date, nullable=False, default=date.today, onupdate=date.today)

    keyword_objs = relationship('Keyword', secondary=experiment_sets_keywords_association_table, backref='experiment_sets')
    doi_identifiers = relationship('DoiIdentifier', secondary=experiment_sets_doi_identifiers_association_table, backref='experiment_sets')
    pubmed_identifiers = relationship('PubmedIdentifier', secondary=experiment_sets_pubmed_identifiers_association_table, backref='experiment_sets')
    sra_identifiers = relationship('SraIdentifier', secondary=experiment_sets_sra_identifiers_association_table, backref='experiment_sets')
