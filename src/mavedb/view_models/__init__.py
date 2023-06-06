from pydantic.utils import GetterDict
from typing import Any


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

    def get(self, key: str, default: Any) -> Any:
        if key == "secondary_publication_identifiers":
            pub_assc = getattr(self._obj, "publication_identifier_associations")
            return [assc.publication for assc in pub_assc if not assc.primary]
        elif key == "primary_publication_identifiers":
            pub_assc = getattr(self._obj, "publication_identifier_associations")
            return [assc.publication for assc in pub_assc if assc.primary]
        else:
            return super().get(key, default)


class AuthorsGetter(GetterDict):
    """
    Custom class used in transforming Author SQLAlchemy model objects
    into Pydantic view model objects, for special handling of primary and secondary
    authors.

    Pydantic uses GetterDict objects to access source objects as dictionaries, which can
    then be turned into Pydantic view model objects. We need to remap the underlying
    SQLAlchemy model's Association objects with information about whether an
    author is primary or not into two separate lists with that same information.
    """

    def get(self, key: str, default: Any) -> Any:
        if key == "secondary_authors":
            pub_authors = getattr(self._obj, "authors")
            return tuple([author for author in pub_authors if not author.primary_author])
        elif key == "first_author":
            pub_authors = getattr(self._obj, "authors")
            return [author for author in pub_authors if author.primary_author][0]
        else:
            return super().get(key, default)
