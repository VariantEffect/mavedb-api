# See https://pydantic-docs.helpmanual.io/usage/postponed_annotations/#self-referencing-models
from __future__ import annotations

from datetime import date
from pydantic import root_validator
from typing import Collection, Dict, Optional, Any, Sequence
from humps import camelize

from mavedb.lib.validation import keywords, urn_re
from mavedb.lib.validation.exceptions import ValidationError
from mavedb.models.enums.processing_state import ProcessingState
from mavedb.models.target_sequence import TargetSequence
from mavedb.view_models import PublicationIdentifiersGetter
from mavedb.view_models.base.base import BaseModel, validator
from mavedb.view_models.contributor import Contributor, ContributorCreate
from mavedb.view_models.doi_identifier import (
    DoiIdentifier,
    DoiIdentifierCreate,
    SavedDoiIdentifier,
)
from mavedb.view_models.experiment import Experiment, SavedExperiment
from mavedb.view_models.license import ShortLicense
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


class ScoreSetGetter(PublicationIdentifiersGetter):
    def get(self, key: Any, default: Any = ...) -> Any:
        if key == "meta_analyzes_score_set_urns":
            meta_analyzes_score_sets = getattr(self._obj, "meta_analyzes_score_sets") or []
            return sorted([score_set.urn for score_set in meta_analyzes_score_sets])
        elif key == "meta_analyzed_by_score_set_urns":
            meta_analyzed_by_score_sets = getattr(self._obj, "meta_analyzed_by_score_sets") or []
            return sorted([score_set.urn for score_set in meta_analyzed_by_score_sets])
        else:
            return super().get(key, default)


class ScoreSetBase(BaseModel):
    """Base class for score set view models."""

    title: str
    method_text: str
    abstract_text: str
    short_description: str
    extra_metadata: Optional[dict]
    data_usage_policy: Optional[str]


class ScoreSetModify(ScoreSetBase):
    contributors: Optional[list[ContributorCreate]]
    keywords: Optional[list[str]]
    primary_publication_identifiers: Optional[list[PublicationIdentifierCreate]]
    secondary_publication_identifiers: Optional[list[PublicationIdentifierCreate]]
    doi_identifiers: Optional[list[DoiIdentifierCreate]]
    target_genes: list[TargetGeneCreate]

    @validator("primary_publication_identifiers")
    def max_one_primary_publication_identifier(cls, v):
        if isinstance(v, list):
            if len(v) > 1:
                raise ValidationError("multiple primary publication identifiers are not allowed")
        return v

    # Validate nested label field within target sequence if there are multiple target genes
    @validator("target_genes")
    def targets_need_labels_when_multiple_targets_exist(cls, field_value, values):
        if len(field_value) > 1:
            for idx, target in enumerate(field_value):
                if target.target_sequence and target.target_sequence.label is None:
                    raise ValidationError(
                        "Target sequence labels cannot be empty when multiple targets are defined.",
                        custom_loc=["body", "targetGene", idx, "targetSequence", "label"],
                    )

        return field_value

    # Validate nested label fields are not identical
    @validator("target_genes")
    def target_labels_are_unique(cls, field_value, values):
        # Labels are only used on target sequence instances.
        if len(field_value) > 1 and all([isinstance(target, TargetSequence) for target in field_value]):
            labels = [target.target_sequence.label for target in field_value]
            dup_indices = [idx for idx, item in enumerate(labels) if item in labels[:idx]]
            if dup_indices:
                # TODO: surface the error for the each duplicated index. the way these pydantic validators are
                # implemented would require additional customization to surface each duplicate, so surfacing
                # just one for now seems fine.
                raise ValidationError(
                    "Target sequence labels cannot be duplicated.",
                    custom_loc=["body", "targetGene", dup_indices[-1], "targetSequence", "label"],
                )

        return field_value

    @validator("keywords")
    def validate_keywords(cls, v):
        keywords.validate_keywords(v)
        return v


class ScoreSetCreate(ScoreSetModify):
    """View model for creating a new score set."""

    experiment_urn: Optional[str]
    license_id: int
    superseded_score_set_urn: Optional[str]
    meta_analyzes_score_set_urns: Optional[list[str]]

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

    @validator("meta_analyzes_score_set_urns")
    def validate_score_set_urn(cls, v):
        if v is not None:
            for s in v:
                if (
                    urn_re.MAVEDB_SCORE_SET_URN_RE.fullmatch(s) is None
                    and urn_re.MAVEDB_TMP_URN_RE.fullmatch(s) is None
                ):
                    raise ValueError(f"'{s}' is not a valid score set URN")
        return v

    @validator("experiment_urn")
    def validate_experiment_urn(cls, v):
        if urn_re.MAVEDB_EXPERIMENT_URN_RE.fullmatch(v) is None and urn_re.MAVEDB_TMP_URN_RE.fullmatch(v) is None:
            raise ValueError(f"'{v}' is not a valid experiment URN")
        return v

    @root_validator
    def validate_experiment_urn_required_except_for_meta_analyses(cls, values):
        experiment_urn = values.get("experiment_urn")
        meta_analyzes_score_set_urns = values.get("meta_analyzes_score_set_urns")
        is_meta_analysis = not (meta_analyzes_score_set_urns is None or len(meta_analyzes_score_set_urns) == 0)
        if experiment_urn is None and not is_meta_analysis:
            raise ValidationError("experiment URN is required unless your score set is a meta-analysis")
        if experiment_urn is not None and is_meta_analysis:
            raise ValidationError("experiment URN should not be supplied when your score set is a meta-analysis")
        return values


class ScoreSetUpdate(ScoreSetModify):
    """View model for updating a score set."""

    license_id: Optional[int]


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
    primary_publication_identifiers: list[SavedPublicationIdentifier]
    secondary_publication_identifiers: list[SavedPublicationIdentifier]
    license: ShortLicense
    creation_date: date
    modification_date: date
    target_genes: list[ShortTargetGene]
    private: bool

    class Config:
        orm_mode = True
        arbitrary_types_allowed = True
        getter_dict = ScoreSetGetter


class SavedScoreSet(ScoreSetBase):
    """Base class for score set view models representing saved records."""

    urn: str
    num_variants: int
    experiment: SavedExperiment
    license: ShortLicense
    superseded_score_set_urn: Optional[str]
    superseding_score_set_urn: Optional[str]
    meta_analyzes_score_set_urns: list[str]
    meta_analyzed_by_score_set_urns: list[str]
    doi_identifiers: Sequence[SavedDoiIdentifier]
    primary_publication_identifiers: Sequence[SavedPublicationIdentifier]
    secondary_publication_identifiers: Sequence[SavedPublicationIdentifier]
    published_date: Optional[date]
    creation_date: date
    modification_date: date
    created_by: Optional[SavedUser]
    modified_by: Optional[SavedUser]
    target_genes: Sequence[SavedTargetGene]
    dataset_columns: Dict
    contributors: Optional[list[Contributor]]
    keywords: list[str]

    class Config:
        orm_mode = True
        arbitrary_types_allowed = True
        getter_dict = ScoreSetGetter

    # Association proxy objects return an untyped _AssocitionList object.
    # Recast it into something more generic.
    @validator("secondary_publication_identifiers", "primary_publication_identifiers", pre=True)
    def publication_identifiers_validator(cls, value) -> list[PublicationIdentifier]:
        assert isinstance(value, Collection), "Publication identifier lists must be a collection"
        return list(value)  # Re-cast into proper list-like type

    @validator("dataset_columns")
    def camelize_dataset_columns_keys(cls, value) -> dict:
        return camelize(value)


class ScoreSet(SavedScoreSet):
    """Score set view model containing most properties visible to non-admin users, but no variant data."""

    experiment: Experiment
    doi_identifiers: Sequence[DoiIdentifier]
    primary_publication_identifiers: Sequence[PublicationIdentifier]
    secondary_publication_identifiers: Sequence[PublicationIdentifier]
    created_by: Optional[User]
    modified_by: Optional[User]
    target_genes: Sequence[TargetGene]
    private: bool
    processing_state: Optional[ProcessingState]
    processing_errors: Optional[dict]


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
