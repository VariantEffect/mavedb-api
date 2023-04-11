from datetime import date

from sqlalchemy import Column, Date, Integer, String

from mavedb.db.base import Base


class WildTypeSequence(Base):
    __tablename__ = "wild_type_sequences"

    id = Column(Integer, primary_key=True, index=True)
    sequence_type = Column(String, nullable=False)
    sequence = Column(String, nullable=False)
    creation_date = Column(Date, nullable=False, default=date.today)
    modification_date = Column(Date, nullable=False, default=date.today, onupdate=date.today)
