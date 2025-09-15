from datetime import date

from sqlalchemy import Boolean, Column, Date, Integer, String, UniqueConstraint

from mavedb.db.base import Base


class ControlledKeyword(Base):
    __tablename__ = "controlled_keywords"

    id = Column(Integer, primary_key=True, index=True)
    key = Column(String, nullable=False)
    label = Column(String, nullable=False)
    system = Column(String, nullable=True)
    code = Column(String, nullable=True)
    version = Column(String, nullable=True)
    special = Column(Boolean, nullable=True)
    description = Column(String, nullable=True)
    creation_date = Column(Date, nullable=False, default=date.today)
    modification_date = Column(Date, nullable=False, default=date.today, onupdate=date.today)
    __table_args__ = (UniqueConstraint("key", "label", name="ix_controlled_keywords_key_label"),)
