from datetime import date

from sqlalchemy import Column, Date, Integer, String
from sqlalchemy.dialects.postgresql import JSONB

from mavedb.db.base import Base


class PublicationIdentifier(Base):
    __tablename__ = "publication_identifiers"

    id = Column(Integer, primary_key=True)
    identifier = Column(String, nullable=False)
    db_name = Column(String, nullable=False)
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
