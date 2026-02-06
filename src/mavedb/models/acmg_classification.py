"""SQLAlchemy model for ACMG classification entities."""

from datetime import date

from sqlalchemy import Column, Date, Enum, Integer

from mavedb.db.base import Base
from mavedb.models.enums.acmg_criterion import ACMGCriterion
from mavedb.models.enums.strength_of_evidence import StrengthOfEvidenceProvided


class ACMGClassification(Base):
    """ACMG classification model for storing ACMG criteria, evidence strength, and points."""

    __tablename__ = "acmg_classifications"

    id = Column(Integer, primary_key=True)

    criterion = Column(Enum(ACMGCriterion, native_enum=False, validate_strings=True, length=32), nullable=True)
    evidence_strength = Column(
        Enum(StrengthOfEvidenceProvided, native_enum=False, validate_strings=True, length=32), nullable=True
    )
    points = Column(Integer, nullable=True)

    creation_date = Column(Date, nullable=False, default=date.today)
    modification_date = Column(Date, nullable=False, default=date.today, onupdate=date.today)
