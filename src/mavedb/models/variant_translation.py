from datetime import date

from sqlalchemy import Column, Date, String

from mavedb.db.base import Base


class VariantTranslation(Base):
    """FROZEN (serving-only). Written by the retired populate_variant_translations_for_score_set job;
    superseded by the reverse-translation allele equivalence space. Read for existing old-model data,
    never written for new score sets, dropped at read-cutover. See lib/variant_translations.py."""

    __tablename__ = "variant_translations"

    aa_clingen_id = Column(String, nullable=False, primary_key=True)
    nt_clingen_id = Column(String, nullable=False, primary_key=True)
    creation_date = Column(Date, nullable=False, default=date.today)
    modification_date = Column(Date, nullable=False, default=date.today, onupdate=date.today)
