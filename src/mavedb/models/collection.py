from datetime import date

from sqlalchemy import Boolean, Column, Date, ForeignKey, Integer, String
from sqlalchemy.ext.associationproxy import AssociationProxy, association_proxy
from sqlalchemy.orm import Mapped, relationship

import mavedb.models.collection_user_association
from mavedb.db.base import Base
from mavedb.lib.urns import generate_collection_urn
from mavedb.models.collection_experiment_association import CollectionExperimentAssociation
from mavedb.models.collection_score_set_association import CollectionScoreSetAssociation

from .experiment import Experiment
from .score_set import ScoreSet
from .user import User


class Collection(Base):
    __tablename__ = "collections"

    id = Column(Integer, primary_key=True)

    urn = Column(
        String(64),
        nullable=True,
        default=generate_collection_urn,
        unique=True,
        index=True,
    )
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
        "CollectionUserAssociation",
        back_populates="collection",
        cascade="all, delete-orphan",
    )
    users: AssociationProxy[list[User]] = association_proxy(
        "user_associations",
        "user",
        creator=lambda u: mavedb.models.collection_user_association.CollectionUserAssociation(
            user=u, contribution_role=u.role
        ),
    )

    # Ordered association relationships
    score_set_associations: Mapped[list[CollectionScoreSetAssociation]] = relationship(
        "CollectionScoreSetAssociation",
        back_populates="collection",
        cascade="all, delete-orphan",
        order_by="CollectionScoreSetAssociation.position",
    )
    experiment_associations: Mapped[list[CollectionExperimentAssociation]] = relationship(
        "CollectionExperimentAssociation",
        back_populates="collection",
        cascade="all, delete-orphan",
        order_by="CollectionExperimentAssociation.position",
    )

    # Convenient proxies for direct access to ordered score sets and experiments
    experiments: AssociationProxy[list[Experiment]] = association_proxy(
        "experiment_associations",
        "experiment",
    )
    score_sets: AssociationProxy[list[ScoreSet]] = association_proxy(
        "score_set_associations",
        "score_set",
    )
