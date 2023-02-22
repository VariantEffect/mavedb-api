# See https://pydantic-docs.helpmanual.io/usage/postponed_annotations/#self-referencing-models
from __future__ import annotations
from datetime import date
from typing import Dict, Optional

from src.view_models.base.base import BaseModel
from src.view_models.doi_identifier import DoiIdentifier, DoiIdentifierCreate, SavedDoiIdentifier
from src.view_models.experiment import Experiment, SavedExperiment
from src.view_models.pubmed_identifier import PubmedIdentifier, PubmedIdentifierCreate, SavedPubmedIdentifier
from src.view_models.target_gene import SavedTargetGene, ShortTargetGene, TargetGene, TargetGeneCreate
from src.view_models.user import SavedUser, User
from src.view_models.variant import VariantInDbBase


class ScoresetBase(BaseModel):
    """Base class for score set view models."""

    title: str
    method_text: str
    abstract_text: str
    short_description: str
    extra_metadata: Dict
    data_usage_policy: Optional[str]
    licence_id: Optional[int]
    keywords: Optional[list[str]]


class ScoresetCreate(ScoresetBase):
    """View model for creating a new score set."""

    experiment_urn: str
    superseded_scoreset_urn: Optional[str]
    meta_analysis_source_scoreset_urns: Optional[list[str]]
    target_gene: TargetGeneCreate
    doi_identifiers: Optional[list[DoiIdentifierCreate]]
    pubmed_identifiers: Optional[list[PubmedIdentifierCreate]]


class ScoresetUpdate(ScoresetBase):
    """View model for updating a score set."""

    doi_identifiers: list[DoiIdentifierCreate]
    pubmed_identifiers: list[PubmedIdentifierCreate]
    target_gene: TargetGeneCreate


class ShortScoreset(BaseModel):
    """
    Target gene view model containing a smaller set of properties to return in list contexts.

    Notice that this is not derived from ScoresetBase.
    """

    urn: str
    title: str
    short_description: str
    published_date: Optional[date]
    replaces_id: Optional[int]
    num_variants: int
    experiment: Experiment
    creation_date: date
    modification_date: date
    target_gene: ShortTargetGene
    private: bool

    class Config:
        orm_mode = True
        arbitrary_types_allowed = True


class SavedScoreset(ScoresetBase):
    """Base class for score set view models representing saved records."""

    urn: str
    num_variants: int
    experiment: SavedExperiment
    superseded_scoreset: Optional[ShortScoreset]
    superseding_scoreset: Optional[SavedScoreset]
    meta_analysis_source_scoresets: list[ShortScoreset]
    meta_analyses: list[ShortScoreset]
    doi_identifiers: list[SavedDoiIdentifier]
    pubmed_identifiers: list[SavedPubmedIdentifier]
    published_date: Optional[date]
    creation_date: date
    modification_date: date
    created_by: Optional[SavedUser]
    modified_by: Optional[SavedUser]
    target_gene: SavedTargetGene
    dataset_columns: Dict

    class Config:
        orm_mode = True
        arbitrary_types_allowed = True


class Scoreset(SavedScoreset):
    """Score set view model containing most properties visible to non-admin users, but no variant data."""

    experiment: Experiment
    superseded_scoreset: Optional[ShortScoreset]
    superseding_scoreset: Optional[Scoreset]
    meta_analysis_source_scoresets: list[ShortScoreset]
    meta_analyses: list[ShortScoreset]
    doi_identifiers: list[DoiIdentifier]
    pubmed_identifiers: list[PubmedIdentifier]
    created_by: Optional[User]
    modified_by: Optional[User]
    target_gene: TargetGene
    num_variants: int
    private: bool
    # processing_state: Optional[str]


class ScoresetWithVariants(Scoreset):
    """
    Score set view model containing a complete set of properties visible to non-admin users, for contexts where variants
    are requested.
    """

    variants: list[VariantInDbBase]


class AdminScoreset(Scoreset):
    """Score set view model containing properties to return to admin clients."""

    normalised: bool
    approved: bool
