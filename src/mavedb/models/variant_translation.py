from datetime import date

from sqlalchemy import Column, Date, String

from mavedb.db.base import Base


class VariantTranslation(Base):
    __tablename__ = "variant_translations"

    aa_clingen_id = Column(String, nullable=False, primary_key=True)
    nt_clingen_id = Column(String, nullable=False, primary_key=True)
    creation_date = Column(Date, nullable=False, default=date.today)
    modification_date = Column(Date, nullable=False, default=date.today, onupdate=date.today)
