from datetime import date

from sqlalchemy import Boolean, Column, Date, Integer, String, UniqueConstraint

from mavedb.db.base import Base


class ControlledKeyword(Base):
    __tablename__ = "controlled_keywords"

    id = Column(Integer, primary_key=True, index=True)
    key = Column(String, nullable=False)
    value = Column(String, nullable=False)
    vocabulary = Column(String, nullable=True)
    special = Column(Boolean, nullable=True)
    description = Column(String, nullable=True)
    creation_date = Column(Date, nullable=False, default=date.today)
    modification_date = Column(Date, nullable=False, default=date.today, onupdate=date.today)
    __table_args__ = (UniqueConstraint("key", "value", name="ix_controlled_keywords_key_value"),)
