from datetime import date

from sqlalchemy import Boolean, Column, Date, ForeignKey, Integer, String
from sqlalchemy.ext.associationproxy import AssociationProxy, association_proxy
from sqlalchemy.orm import Mapped, relationship
from sqlalchemy.schema import Table

import mavedb.models.collection_user_association
from mavedb.db.base import Base
from mavedb.lib.temp_urns import generate_temp_urn

from .experiment import Experiment
from .score_set import ScoreSet
from .user import User

collection_experiments_association_table = Table(
    "collection_experiments",
    Base.metadata,
    Column("collection_id", ForeignKey("collections.id"), primary_key=True),
    Column("experiment_id", ForeignKey("experiments.id"), primary_key=True),
)

collection_score_sets_association_table = Table(
    "collection_score_sets",
    Base.metadata,
    Column("collection_id", ForeignKey("collections.id"), primary_key=True),
    Column("score_set_id", ForeignKey("scoresets.id"), primary_key=True),
)


class Collection(Base):
    __tablename__ = "collections"

    id = Column(Integer, primary_key=True)

    urn = Column(String(64), nullable=True, default=generate_temp_urn, unique=True, index=True)
    private = Column(Boolean, nullable=False, default=True)

    name = Column(String, nullable=False)
    badge_name = Column(String, nullable=True)
    description = Column(String, nullable=True)

    created_by_id = Column(Integer, ForeignKey("users.id"), index=True, nullable=True)
    created_by: Mapped[User] = relationship("User", foreign_keys="Collection.created_by_id")
    modified_by_id = Column(Integer, ForeignKey("users.id"), index=True, nullable=True)
    modified_by: Mapped[User] = relationship("User", foreign_keys="Collection.modified_by_id")
    creation_date = Column(Date, nullable=False, default=date.today)
    modification_date = Column(Date, nullable=False, default=date.today, onupdate=date.today)

    user_associations: Mapped[list[mavedb.models.collection_user_association.CollectionUserAssociation]] = relationship(
        "CollectionUserAssociation", back_populates="collection", cascade="all, delete-orphan"
    )
    users: AssociationProxy[list[User]] = association_proxy(
        "user_associations",
        "user",
        creator=lambda c: mavedb.models.collection_user_association.CollectionUserAssociation(
            collection=c, contribution_role=c.role
        ),
    )

    experiments: Mapped[list[Experiment]] = relationship(
        "Experiment", secondary=collection_experiments_association_table, backref="collections"
    )
    score_sets: Mapped[list[ScoreSet]] = relationship(
        "ScoreSet", secondary=collection_score_sets_association_table, backref="collections"
    )
