from datetime import date
from typing import List, TYPE_CHECKING

from sqlalchemy import Boolean, Column, Date, ForeignKey, Integer, String
from sqlalchemy.orm import relationship, Mapped
from sqlalchemy.schema import Table
from sqlalchemy.dialects.postgresql import JSONB

from mavedb.db.base import Base
from mavedb.lib.temp_urns import generate_temp_urn
from .user import User
from .keyword import Keyword
from .doi_identifier import DoiIdentifier
from .publication_identifier import PublicationIdentifier
from .raw_read_identifier import RawReadIdentifier

if TYPE_CHECKING:
    from mavedb.models.experiment import Experiment

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

experiment_sets_publication_identifiers_association_table = Table(
    "experiment_set_publication_identifiers",
    Base.metadata,
    Column("experiment_set_id", ForeignKey("experiment_sets.id"), primary_key=True),
    Column("publication_identifier_id", ForeignKey("publication_identifiers.id"), primary_key=True),
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

    urn = Column(String(64), nullable=True, default=generate_temp_urn, unique=True, index=True)
    extra_metadata = Column(JSONB, nullable=False)

    private = Column(Boolean, nullable=False, default=True)
    approved = Column(Boolean, nullable=False, default=False)
    published_date = Column(Date, nullable=True)
    processing_state = Column(String, nullable=True)

    # TODO Refactor the way we handle child collections?
    num_experiments = Column(Integer, nullable=False, default=0)
    experiments: Mapped[List["Experiment"]] = relationship(
        back_populates="experiment_set", cascade="all, delete-orphan"
    )

    created_by_id = Column(Integer, ForeignKey("users.id"), index=True, nullable=True)
    created_by: Mapped[User] = relationship("User", foreign_keys="ExperimentSet.created_by_id")
    modified_by_id = Column(Integer, ForeignKey("users.id"), nullable=True)
    modified_by: Mapped[User] = relationship("User", foreign_keys="ExperimentSet.modified_by_id")
    creation_date = Column(Date, nullable=False, default=date.today)
    modification_date = Column(Date, nullable=False, default=date.today, onupdate=date.today)

    keyword_objs: Mapped[list[Keyword]] = relationship(
        "Keyword", secondary=experiment_sets_keywords_association_table, backref="experiment_sets"
    )
    doi_identifiers: Mapped[list[DoiIdentifier]] = relationship(
        "DoiIdentifier", secondary=experiment_sets_doi_identifiers_association_table, backref="experiment_sets"
    )
    publication_identifiers: Mapped[list[PublicationIdentifier]] = relationship(
        "PublicationIdentifier",
        secondary=experiment_sets_publication_identifiers_association_table,
        backref="experiment_sets",
    )
    # sra_identifiers = relationship('SraIdentifier', secondary=experiment_sets_sra_identifiers_association_table, backref='experiment_sets')
    raw_read_identifiers: Mapped[list[RawReadIdentifier]] = relationship(
        "RawReadIdentifier", secondary=experiment_sets_raw_read_identifiers_association_table, backref="experiment_sets"
    )
