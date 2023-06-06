from datetime import date

from sqlalchemy import Column, Date, Integer, String, ForeignKey, Boolean

from mavedb.db.base import Base


class Author(Base):
    __tablename__ = "authors"

    id = Column(Integer, primary_key=True, index=True)
    publication_identifier_id = Column(Integer, ForeignKey("publication_identifiers.id"), nullable=False)
    name = Column(String, nullable=False)
    primary_author = Column(Boolean, nullable=False)
    creation_date = Column(Date, nullable=False, default=date.today)
    modification_date = Column(Date, nullable=False, default=date.today, onupdate=date.today)
