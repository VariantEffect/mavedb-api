# Prevent circular imports
from typing import TYPE_CHECKING

from sqlalchemy import Enum, Column, ForeignKey, Integer
from sqlalchemy.orm import Mapped, relationship

from mavedb.db.base import Base
from mavedb.models.enums.contribution_role import ContributionRole

if TYPE_CHECKING:
    from mavedb.models.user import User
    from mavedb.models.collection import Collection


class CollectionUserAssociation(Base):
    __tablename__ = "collection_user_associations"

    collection_id = Column("collection_id", Integer, ForeignKey("collections.id"), primary_key=True)
    user_id = Column("user_id", Integer, ForeignKey("users.id"), primary_key=True)
    contribution_role: Mapped["ContributionRole"] = Column(
        Enum(
            ContributionRole,
            create_constraint=True,
            length=32,
            native_enum=False,
            validate_strings=True,
        ),
        nullable=False,
    )

    collection: Mapped["Collection"] = relationship(
        "mavedb.models.collection.Collection", back_populates="user_associations"
    )
    user: Mapped["User"] = relationship("User")
