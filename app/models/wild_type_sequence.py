from datetime import date
from sqlalchemy import Column, Date, Integer, String

from app.db.base import Base


class WildTypeSequence(Base):
    __tablename__ = 'genome_wildtypesequence'

    id = Column(Integer, primary_key=True, index=True)
    sequence_type = Column(String(32), nullable=False)
    sequence = Column(String, nullable=False)
    creation_date = Column(Date, nullable=False, default=date.today)
    modification_date = Column(Date, nullable=False, default=date.today, onupdate=date.today)
