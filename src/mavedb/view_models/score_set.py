# See https://pydantic-docs.helpmanual.io/usage/postponed_annotations/#self-referencing-models
from __future__ import annotations

from datetime import date
from typing import Collection, Dict, Optional

from pydantic import Field

from mavedb.lib.validation.exceptions import ValidationError
from mavedb.lib.validation import keywords, urn_re
from mavedb.view_models import PublicationIdentifiersGetter
from mavedb.view_models.base.base import BaseModel, validator
from mavedb.view_models.doi_identifier import (
    DoiIdentifier,
    DoiIdentifierCreate,
    SavedDoiIdentifier,
)
from mavedb.view_models.experiment import Experiment, SavedExperiment
from mavedb.view_models.license import License, SavedLicense, ShortLicense
from mavedb.view_models.publication_identifier import (
    PublicationIdentifier,
    PublicationIdentifierCreate,
    SavedPublicationIdentifier,
)
from mavedb.view_models.target_gene import (
    SavedTargetGene,
    ShortTargetGene,
    TargetGene,
    TargetGeneCreate,
)
from mavedb.view_models.user import SavedUser, User
from mavedb.view_models.variant import VariantInDbBase


class ScoreSetBase(BaseModel):
    """Base class for score set view models."""

    title: str
    method_text: Optional[str]
    abstract_text: Optional[str]
    short_description: str
    extra_metadata: Dict
    data_usage_policy: Optional[str]
    keywords: Optional[list[str]]


class ScoreSetModify(ScoreSetBase):
    @validator("keywords")
    def validate_keywords(cls, v):
        keywords.validate_keywords(v)
        return v


class ScoreSetCreate(ScoreSetModify):
    """View model for creating a new score set."""

    experiment_urn: str
    license_id: int
    superseded_score_set_urn: Optional[str]
    meta_analysis_source_score_set_urns: Optional[list[str]]
    target_gene: TargetGeneCreate
    doi_identifiers: Optional[list[DoiIdentifierCreate]]
    primary_publication_identifiers: Optional[list[PublicationIdentifierCreate]] = Field(..., min_items=0, max_items=1)
    publication_identifiers: Optional[list[PublicationIdentifierCreate]]

    @validator("superseded_score_set_urn")
    def validate_superseded_score_set_urn(cls, v):
        if v is None:
            pass
        else:
            if urn_re.MAVEDB_SCORE_SET_URN_RE.fullmatch(v) is None:
                if urn_re.MAVEDB_TMP_URN_RE.fullmatch(v) is None:
                    raise ValueError(f"'{v}' is not a valid score set URN")
                else:
                    raise ValueError("cannot supersede a private score set - please edit it instead")
        return v

    @validator("meta_analysis_source_score_set_urns")
    def validate_score_set_urn(cls, v):
        if v is None:
            pass
        else:
            for s in v:
                if urn_re.MAVEDB_SCORE_SET_URN_RE.fullmatch(s) is None and urn_re.MAVEDB_TMP_URN_RE.fullmatch(s) is None:
                    raise ValueError(f"'{s}' is not a valid score set URN")
        return v

    @validator("experiment_urn")
    def validate_experiment_urn(cls, v):
        urn.validate_mavedb_urn_experiment(v)
        return v

class ScoreSetUpdate(ScoreSetModify):
    """View model for updating a score set."""

    license_id: Optional[int]
    doi_identifiers: list[DoiIdentifierCreate]
    primary_publication_identifiers: Optional[list[PublicationIdentifierCreate]] = Field(..., min_items=0, max_items=1)
    publication_identifiers: list[PublicationIdentifierCreate]
    target_gene: TargetGeneCreate


class ShortScoreSet(BaseModel):
    """
    Score set view model containing a smaller set of properties to return in list contexts.

    Notice that this is not derived from ScoreSetBase.
    """

    urn: str
    title: str
    short_description: str
    published_date: Optional[date]
    replaces_id: Optional[int]
    num_variants: int
    experiment: Experiment
    license: ShortLicense
    creation_date: date
    modification_date: date
    target_gene: ShortTargetGene
    private: bool

    class Config:
        orm_mode = True
        arbitrary_types_allowed = True


class SavedScoreSet(ScoreSetBase):
    """Base class for score set view models representing saved records."""

    urn: str
    num_variants: int
    experiment: SavedExperiment
    license: ShortLicense
    superseded_score_set: Optional[ShortScoreSet]
    superseding_score_set: Optional[SavedScoreSet]
    meta_analysis_source_score_sets: list[ShortScoreSet]
    meta_analyses: list[ShortScoreSet]
    doi_identifiers: list[SavedDoiIdentifier]
    primary_publication_identifiers: list[SavedPublicationIdentifier]
    secondary_publication_identifiers: list[SavedPublicationIdentifier]
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
        getter_dict = PublicationIdentifiersGetter

    # Association proxy objects return an untyped _AssocitionList object.
    # Recast it into something more generic.
    @validator("secondary_publication_identifiers", "primary_publication_identifiers", pre=True)
    def publication_identifiers_validator(cls, value) -> list[PublicationIdentifier]:
        assert isinstance(value, Collection), "`publication_identifiers` must be a collection"
        return list(value)  # Re-cast into proper list-like type


class ScoreSet(SavedScoreSet):
    """Score set view model containing most properties visible to non-admin users, but no variant data."""

    experiment: Experiment
    license: ShortLicense
    superseded_score_set: Optional[ShortScoreSet]
    superseding_score_set: Optional[ScoreSet]
    meta_analysis_source_score_sets: list[ShortScoreSet]
    meta_analyses: list[ShortScoreSet]
    doi_identifiers: list[DoiIdentifier]
    primary_publication_identifiers: list[PublicationIdentifier]
    secondary_publication_identifiers: list[PublicationIdentifier]
    created_by: Optional[User]
    modified_by: Optional[User]
    target_gene: TargetGene
    num_variants: int
    private: bool
    # processing_state: Optional[str]


class ScoreSetWithVariants(ScoreSet):
    """
    Score set view model containing a complete set of properties visible to non-admin users, for contexts where variants
    are requested.
    """

    variants: list[VariantInDbBase]


class AdminScoreSet(ScoreSet):
    """Score set view model containing properties to return to admin clients."""

    normalised: bool
    approved: bool
