from datetime import date

from sqlalchemy import Boolean, Column, Date, ForeignKey, Integer, String
from sqlalchemy.orm import relationship
from sqlalchemy.schema import Table

from mavedb.db.base import Base
from mavedb.deps import JSONB
from mavedb.lib.temp_urns import generate_temp_urn

experiment_sets_doi_identifiers_association_table = Table(
    "experiment_set_doi_identifiers",
    Base.metadata,
    Column("experiment_set_id", ForeignKey("experiment_sets.id"), primary_key=True),
    Column("doi_identifier_id", ForeignKey("doi_identifiers.id"), primary_key=True),
)

experiment_sets_keywords_association_table = Table(
    "experiment_set_keywords",
    Base.metadata,
    Column("experiment_set_id", ForeignKey("experiment_sets.id"), primary_key=True),
    Column("keyword_id", ForeignKey("keywords.id"), primary_key=True),
)

experiment_sets_pubmed_identifiers_association_table = Table(
    "experiment_set_pubmed_identifiers",
    Base.metadata,
    Column("experiment_set_id", ForeignKey("experiment_sets.id"), primary_key=True),
    Column("pubmed_identifier_id", ForeignKey("pubmed_identifiers.id"), primary_key=True),
)

# experiment_sets_sra_identifiers_association_table = Table(
experiment_sets_raw_read_identifiers_association_table = Table(
    "experiment_set_sra_identifiers",
    Base.metadata,
    Column("experiment_set_id", ForeignKey("experiment_sets.id"), primary_key=True),
    Column("sra_identifier_id", ForeignKey("sra_identifiers.id"), primary_key=True),
)


class ExperimentSet(Base):
    __tablename__ = "experiment_sets"

    id = Column(Integer, primary_key=True, index=True)

    urn = Column(String(64), nullable=True, default=generate_temp_urn)  # index=True, nullable=True
    title = Column(String, nullable=False)
    method_text = Column(String, nullable=False)
    abstract_text = Column(String, nullable=False)
    short_description = Column(String, nullable=False)
    extra_metadata = Column(JSONB, nullable=False)

    private = Column(Boolean, nullable=False, default=True)
    approved = Column(Boolean, nullable=False, default=False)
    published_date = Column(Date, nullable=True)
    processing_state = Column(String, nullable=True)

    # TODO Refactor the way we handle child collections?
    num_experiments = Column(Integer, nullable=False, default=0)

    created_by_id = Column(Integer, ForeignKey("users.id"), nullable=True)
    created_by = relationship("User", foreign_keys="ExperimentSet.created_by_id")
    modified_by_id = Column(Integer, ForeignKey("users.id"), nullable=True)
    modified_by = relationship("User", foreign_keys="ExperimentSet.modified_by_id")
    creation_date = Column(Date, nullable=False, default=date.today)
    modification_date = Column(Date, nullable=False, default=date.today, onupdate=date.today)

    keyword_objs = relationship(
        "Keyword", secondary=experiment_sets_keywords_association_table, backref="experiment_sets"
    )
    doi_identifiers = relationship(
        "DoiIdentifier", secondary=experiment_sets_doi_identifiers_association_table, backref="experiment_sets"
    )
    pubmed_identifiers = relationship(
        "PubmedIdentifier", secondary=experiment_sets_pubmed_identifiers_association_table, backref="experiment_sets"
    )
    # sra_identifiers = relationship('SraIdentifier', secondary=experiment_sets_sra_identifiers_association_table, backref='experiment_sets')
    raw_read_identifiers = relationship(
        "RawReadIdentifier", secondary=experiment_sets_raw_read_identifiers_association_table, backref="experiment_sets"
    )
