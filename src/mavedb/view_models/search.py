from typing import Optional

from mavedb.view_models.base.base import BaseModel


class ExperimentsSearch(BaseModel):
    published: Optional[bool] = None
    authors: Optional[list[str]] = None
    databases: Optional[list[str]] = None
    journals: Optional[list[str]] = None
    publication_identifiers: Optional[list[str]] = None
    keywords: Optional[list[str]] = None
    text: Optional[str] = None
    meta_analysis: Optional[bool] = None


class ScoreSetsSearch(BaseModel):
    published: Optional[bool] = None
    targets: Optional[list[str]] = None
    target_organism_names: Optional[list[str]] = None
    target_types: Optional[list[str]] = None
    target_accessions: Optional[list[str]] = None
    authors: Optional[list[str]] = None
    databases: Optional[list[str]] = None
    journals: Optional[list[str]] = None
    publication_identifiers: Optional[list[str]] = None
    keywords: Optional[list[str]] = None
    text: Optional[str] = None


class TextSearch(BaseModel):
    text: Optional[str] = None
