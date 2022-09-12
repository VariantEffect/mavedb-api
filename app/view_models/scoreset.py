# See https://pydantic-docs.helpmanual.io/usage/postponed_annotations/#self-referencing-models
from __future__ import annotations
from datetime import date
from typing import Dict, Optional

from app.view_models.base.base import BaseModel
from app.view_models.doi_identifier import DoiIdentifier, DoiIdentifierCreate, SavedDoiIdentifier
from app.view_models.experiment import Experiment, SavedExperiment
from app.view_models.pubmed_identifier import PubmedIdentifier, PubmedIdentifierCreate, SavedPubmedIdentifier
from app.view_models.target_gene import SavedTargetGene, ShortTargetGene, TargetGene, TargetGeneCreate
from app.view_models.user import SavedUser, User
from app.view_models.variant import VariantInDbBase


class ScoresetBase(BaseModel):
    """Base class for score set view models."""

    title: str
    method_text: str
    abstract_text: str
    short_description: str
    extra_metadata: Dict
    data_usage_policy: Optional[str]
    licence_id: Optional[int]
    replaces_id: Optional[int]
    keywords: Optional[list[str]]


class ScoresetCreate(ScoresetBase):
    """View model for creating a new score set."""

    experiment_urn: str
    meta_analysis_source_scoreset_urns: Optional[list[str]]
    target_gene: TargetGeneCreate
    doi_identifiers: Optional[list[DoiIdentifierCreate]]
    pubmed_identifiers: Optional[list[PubmedIdentifierCreate]]


class ScoresetUpdate(ScoresetBase):
    """View model for updating a score set."""

    doi_identifiers: list[DoiIdentifierCreate]
    pubmed_identifiers: list[PubmedIdentifierCreate]
    target_gene: TargetGeneCreate


class SavedScoreset(ScoresetBase):
    """Base class for score set view models representing saved records."""

    urn: str
    num_variants: int
    experiment: SavedExperiment
    meta_analysis_source_scoresets: list[SavedScoreset]
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
    meta_analysis_source_scoresets: list[Scoreset]
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


class AdminScoreset(Scoreset):
    """Score set view model containing properties to return to admin clients."""

    normalised: bool
    approved: bool
