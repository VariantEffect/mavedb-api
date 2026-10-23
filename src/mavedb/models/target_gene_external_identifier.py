from typing import TYPE_CHECKING

from sqlalchemy import CheckConstraint, Column, ForeignKey, Integer, String
from sqlalchemy.orm import Mapped, relationship

from mavedb.db.base import Base
from mavedb.lib.external_identifier_urls import external_gene_identifier_url
from mavedb.lib.validation.constants.identifier import valid_dbnames
from mavedb.models.mixins.external_identifier import ExternalIdentifier

if TYPE_CHECKING:
    from mavedb.models.target_gene import TargetGene


class TargetGeneExternalIdentifier(ExternalIdentifier, Base):
    """A target gene's identifier in one external database, and the offset of the target within it.

    A target gene has at most one identifier per database.
    """

    __tablename__ = "target_gene_external_identifiers"
    __table_args__ = (
        CheckConstraint(
            "db_name IN ({})".format(", ".join(f"'{db_name}'" for db_name in valid_dbnames)),
            name="ck_target_gene_external_identifiers_db_name_valid",
        ),
    )

    target_gene_id = Column(Integer, ForeignKey("target_genes.id", ondelete="CASCADE"), primary_key=True)
    target_gene: Mapped["TargetGene"] = relationship(back_populates="external_identifiers")
    db_name: Mapped[str] = Column(String, primary_key=True)
    identifier: Mapped[str] = Column(String, nullable=False)
    offset: Mapped[int] = Column(Integer, nullable=False)

    @property
    def url(self) -> str:
        return external_gene_identifier_url(self.db_name, self.identifier)
