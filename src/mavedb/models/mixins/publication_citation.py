"""A record's citation of a publication, by the publication's natural key."""

from typing import TYPE_CHECKING, Any, NamedTuple

from sqlalchemy import ForeignKeyConstraint, Index, String
from sqlalchemy.orm import Mapped, declared_attr, mapped_column, relationship

if TYPE_CHECKING:
    from mavedb.models.publication_identifier import PublicationIdentifier


class PublicationKey(NamedTuple):
    """A publication's natural key, which citations reference."""

    db_name: str
    identifier: str


class PublicationCitation:
    """A citation of a publication by its natural key, ``(db_name, identifier)``.

    Publications are shared rows a caller can see only through a citation they can see (#808), so a citation never
    reads the publication to learn its id; foreign key checks ignore row security. ``publication`` is read-only for
    the same reason: write the publication with ``lib.publications.ensure_publications`` and cite it by key.
    """

    __tablename__: str

    db_name: Mapped[str] = mapped_column(String, primary_key=True)
    identifier: Mapped[str] = mapped_column(String, primary_key=True)

    @property
    def key(self) -> PublicationKey:
        return PublicationKey(self.db_name, self.identifier)

    @declared_attr
    def publication(cls) -> Mapped["PublicationIdentifier"]:
        return relationship("PublicationIdentifier", viewonly=True)

    @declared_attr.directive
    def __table_args__(cls) -> tuple[Any, ...]:
        return (
            ForeignKeyConstraint(
                ["db_name", "identifier"],
                ["publication_identifiers.db_name", "publication_identifiers.identifier"],
                name=f"fk_{cls.__tablename__}_publication",
            ),
            Index(f"ix_{cls.__tablename__}_publication", "db_name", "identifier"),
        )
