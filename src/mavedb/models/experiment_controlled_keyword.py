from typing import TYPE_CHECKING

from sqlalchemy import Column, ForeignKey, Integer, String
from sqlalchemy.orm import Mapped, backref, relationship

from mavedb.db.base import Base
from mavedb.models.controlled_keyword import ControlledKeyword

if TYPE_CHECKING:
    from mavedb.models.experiment import Experiment


class ExperimentControlledKeywordAssociation(Base):
    __tablename__ = "experiment_controlled_keywords"

    controlled_keyword_id = Column(Integer, ForeignKey("controlled_keywords.id"), nullable=False, primary_key=True)
    controlled_keyword: Mapped[ControlledKeyword] = relationship(
        "ControlledKeyword", backref=backref("experiment_controlled_keywords", uselist=True)
    )

    experiment_id = Column(Integer, ForeignKey("experiments.id"), nullable=False, primary_key=True)
    experiment: Mapped["Experiment"] = relationship(back_populates="keyword_objs")

    description = Column(String, nullable=True)
