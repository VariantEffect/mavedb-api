from typing import Any

from pydantic import validator
from pydantic.utils import GetterDict

from mavedb.models.enums.contribution_role import ContributionRole


class PublicationIdentifiersGetter(GetterDict):
    """
    Custom class used in transforming PublicationAssociation SQLAlchemy model objects
    into Pydantic view model objects, with special handling of publication identifier
    association objects.

    Pydantic uses GetterDict objects to access source objects as dictionaries, which can
    then be turned into Pydantic view model objects. We need to remap the underlying
    SQLAlchemy model's AssociationList objects with information about whether a
    publication is primary or not into two separate lists with that same information.
    """

    def get(self, key: Any, default: Any = ...) -> Any:
        if key == "secondary_publication_identifiers":
            pub_assc = getattr(self._obj, "publication_identifier_associations")
            return [assc.publication for assc in pub_assc if not assc.primary]
        elif key == "primary_publication_identifiers":
            pub_assc = getattr(self._obj, "publication_identifier_associations")
            return [assc.publication for assc in pub_assc if assc.primary]
        else:
            return super().get(key, default)


class UserContributionRoleGetter(GetterDict):
    """
    Custom class used in transforming ContributionAssociation SQLAlchemy model objects
    into Pydantic view model objects, with special handling of user association objects.

    Pydantic uses GetterDict objects to access source objects as dictionaries, which can
    then be turned into Pydantic view model objects. We need to remap the underlying
    SQLAlchemy model's AssociationList objects with information about the role for a
    contributing user
    """

    def get(self, key: Any, default: Any = ...) -> Any:
        # The standard is to name properties as the plural of the enum value
        if key[:-1] in ContributionRole._member_map_:
            user_assc = getattr(self._obj, "user_associations")
            return [user.user for user in user_assc if key[:-1] == user.contribution_role.name]
        else:
            return super().get(key, default)


def record_type_validator():
    return validator("record_type", allow_reuse=True, pre=True, always=True)


def set_record_type(cls, v):
    # Record type will be set to the class name no matter the input.
    return cls.__name__
