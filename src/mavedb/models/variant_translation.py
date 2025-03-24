from sqlalchemy import Column, String
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()


class VariantTranslation(Base):
    __tablename__ = "variant_translations"

    aa_clingen_id = Column(String, nullable=False, primary_key=True)
    nt_clingen_id = Column(String, nullable=False, primary_key=True)
