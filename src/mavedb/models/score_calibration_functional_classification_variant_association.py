"""SQLAlchemy association table for variants belonging to functional classifications."""

from sqlalchemy import Column, ForeignKey, Table

from mavedb.db.base import Base

score_calibration_functional_classification_variants_association_table = Table(
    "score_calibration_functional_classification_variants",
    Base.metadata,
    Column(
        "functional_classification_id", ForeignKey("score_calibration_functional_classifications.id"), primary_key=True
    ),
    Column("variant_id", ForeignKey("variants.id"), primary_key=True),
)
