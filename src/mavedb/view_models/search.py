from typing import Optional

from mavedb.view_models.base.base import BaseModel
from mavedb.view_models.score_set import ShortScoreSet


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
    include_experiment_score_set_urns_and_count: Optional[bool] = True
    offset: Optional[int] = None
    limit: Optional[int] = None


class ScoreSetsSearchResponse(BaseModel):
    score_sets: list[ShortScoreSet]
    num_score_sets: int

    class Config:
        from_attributes = True


class ScoreSetsSearchFilterOption(BaseModel):
    value: str
    count: int

    class Config:
        from_attributes = True


class ScoreSetsSearchFilterOptionsResponse(BaseModel):
    target_gene_categories: list[ScoreSetsSearchFilterOption]
    target_gene_names: list[ScoreSetsSearchFilterOption]
    target_organism_names: list[ScoreSetsSearchFilterOption]
    target_accessions: list[ScoreSetsSearchFilterOption]
    publication_author_names: list[ScoreSetsSearchFilterOption]
    publication_db_names: list[ScoreSetsSearchFilterOption]
    publication_journals: list[ScoreSetsSearchFilterOption]

    class Config:
        from_attributes = True


class TextSearch(BaseModel):
    text: Optional[str] = None
