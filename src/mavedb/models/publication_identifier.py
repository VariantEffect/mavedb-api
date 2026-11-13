from datetime import date

from sqlalchemy import Column, Date, Integer, String, UniqueConstraint
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped

from mavedb.db.base import Base


class PublicationIdentifier(Base):
    __tablename__ = "publication_identifiers"
    # The natural key that citations reference (see PublicationCitation).
    __table_args__ = (UniqueConstraint("db_name", "identifier", name="uq_publication_identifiers_db_name_identifier"),)

    id = Column(Integer, primary_key=True)
    identifier: Mapped[str] = Column(String, nullable=False)
    db_name: Mapped[str] = Column(String, nullable=False)
    db_version = Column(String, nullable=True)
    title = Column(String, nullable=False)
    abstract = Column(String, nullable=True)
    authors = Column(JSONB, nullable=False)
    doi = Column(String, nullable=True)
    publication_year = Column(Integer, nullable=True)
    publication_journal = Column(String, nullable=True)
    url = Column(String, nullable=True)
    reference_html = Column(String, nullable=True)
    creation_date = Column(Date, nullable=False, default=date.today)
    modification_date = Column(Date, nullable=False, default=date.today, onupdate=date.today)
