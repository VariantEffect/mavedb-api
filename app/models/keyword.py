from datetime import date

from sqlalchemy import Column, Date, Integer, String

from app.db.base import Base


class Keyword(Base):
    # __tablename__ = 'metadata_keyword'
    __tablename__ = 'keywords'

    id = Column(Integer, primary_key=True, index=True)
    text = Column(String(250), nullable=False, unique=True)
    creation_date = Column(Date, nullable=False, default=date.today)
    modification_date = Column(Date, nullable=False, default=date.today, onupdate=date.today)
