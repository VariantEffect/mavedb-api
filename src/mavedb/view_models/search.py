from pydantic.types import Optional

from mavedb.view_models.base.base import BaseModel


class ExperimentsSearch(BaseModel):
    published: Optional[bool]
    authors: Optional[list[str]]
    databases: Optional[list[str]]
    journals: Optional[list[str]]
    publication_identifiers: Optional[list[str]]
    keywords: Optional[list[str]]
    text: Optional[str]


class ScoreSetsSearch(BaseModel):
    published: Optional[bool]
    targets: Optional[list[str]]
    target_organism_names: Optional[list[str]]
    target_types: Optional[list[str]]
    target_accessions: Optional[list[str]]
    authors: Optional[list[str]]
    databases: Optional[list[str]]
    journals: Optional[list[str]]
    publication_identifiers: Optional[list[str]]
    keywords: Optional[list[str]]
    text: Optional[str]


class TextSearch(BaseModel):
    text: Optional[str]
