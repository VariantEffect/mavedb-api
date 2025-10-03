# See https://pydantic-docs.helpmanual.io/usage/postponed_annotations/#self-referencing-models
from __future__ import annotations

from datetime import date
from typing import Any, Collection, Optional, Sequence, Union
from typing_extensions import Self

from humps import camelize
from pydantic import field_validator, model_validator

from mavedb.lib.validation import urn_re
from mavedb.lib.validation.exceptions import ValidationError
from mavedb.lib.validation.utilities import is_null
from mavedb.models.enums.mapping_state import MappingState
from mavedb.models.enums.processing_state import ProcessingState
from mavedb.view_models import record_type_validator, set_record_type
from mavedb.view_models.base.base import BaseModel
from mavedb.view_models.contributor import Contributor, ContributorCreate
from mavedb.lib.validation.transform import (
    transform_score_set_list_to_urn_list,
    transform_publication_identifiers_to_primary_and_secondary,
)
from mavedb.view_models.doi_identifier import (
    DoiIdentifier,
    DoiIdentifierCreate,
    SavedDoiIdentifier,
)
from mavedb.view_models.license import ShortLicense
from mavedb.view_models.publication_identifier import (
    PublicationIdentifier,
    PublicationIdentifierCreate,
    SavedPublicationIdentifier,
)
from mavedb.view_models.score_range import SavedScoreSetRanges, ScoreSetRangesCreate, ScoreSetRanges
from mavedb.view_models.target_gene import (
    SavedTargetGene,
    ShortTargetGene,
    TargetGene,
    TargetGeneCreate,
)
from mavedb.view_models.user import SavedUser, User

import logging
logger = logging.getLogger(__name__)

UnboundedRange = tuple[Union[float, None], Union[float, None]]


class ExternalLink(BaseModel):
    url: Optional[str] = None


class OfficialCollection(BaseModel):
    badge_name: str
    name: str
    urn: str

    class Config:
        arbitrary_types_allowed = True
        from_attributes = True


class ScoreSetBase(BaseModel):
    """Base class for score set view models."""

    title: str
    method_text: str
    abstract_text: str
    short_description: str
    extra_metadata: Optional[dict] = None
    data_usage_policy: Optional[str] = None


class ScoreSetModify(ScoreSetBase):
    contributors: Optional[list[ContributorCreate]] = None
    primary_publication_identifiers: Optional[list[PublicationIdentifierCreate]] = None
    secondary_publication_identifiers: Optional[list[PublicationIdentifierCreate]] = None
    doi_identifiers: Optional[list[DoiIdentifierCreate]] = None
    target_genes: list[TargetGeneCreate]
    score_ranges: Optional[ScoreSetRangesCreate] = None

    @field_validator("title", "short_description", "abstract_text", "method_text")
    def validate_field_is_non_empty(cls, v: str) -> str:
        if is_null(v):
            raise ValidationError("This field is required and cannot be empty.")
        return v.strip()

    @field_validator("primary_publication_identifiers")
    def max_one_primary_publication_identifier(
        cls, v: list[PublicationIdentifierCreate]
    ) -> list[PublicationIdentifierCreate]:
        if len(v) > 1:
            raise ValidationError("Multiple primary publication identifiers are not allowed.")
        return v

    # Validate nested label field within target sequence if there are multiple target genes
    @model_validator(mode="after")
    def targets_need_labels_when_multiple_targets_exist(self) -> Self:
        if len(self.target_genes) > 1:
            for idx, target in enumerate(self.target_genes):
                if target.target_sequence and target.target_sequence.label is None:
                    raise ValidationError(
                        "Target sequence labels cannot be empty when multiple targets are defined.",
                        custom_loc=[
                            "body",
                            "targetGene",
                            idx,
                            "targetSequence",
                            "label",
                        ],
                    )

        return self

    # Validate nested label fields are not identical
    @model_validator(mode="after")
    def target_labels_are_unique(self) -> Self:
        # Labels are only used on target sequence instances.
        if len(self.target_genes) > 1 and all([target.target_sequence is not None for target in self.target_genes]):
            # Labels have already been sanitized by the TargetSequence validator.
            labels = [target.target_sequence.label for target in self.target_genes]  # type: ignore
            dup_indices = [idx for idx, item in enumerate(labels) if item in labels[:idx]]
            if dup_indices:
                # TODO: surface the error for the each duplicated index. the way these pydantic validators are
                # implemented would require additional customization to surface each duplicate, so surfacing
                # just one for now seems fine.
                raise ValidationError(
                    "Target sequence labels cannot be duplicated.",
                    custom_loc=[
                        "body",
                        "targetGene",
                        dup_indices[-1],
                        "targetSequence",
                        "label",
                    ],
                )

        return self

    # Validate that this score set contains at least one target attached to it
    @field_validator("target_genes")
    def at_least_one_target_gene_exists(cls, v: list[TargetGeneCreate]):
        if len(v) < 1:
            raise ValidationError("Score sets should define at least one target.")
        return v

    # Validate nested label fields are not identical
    @field_validator("target_genes")
    def target_accession_base_editor_targets_are_consistent(cls, field_value, values):
        # Only target accessions can have base editor data.
        if len(field_value) > 1 and all([target.target_accession is not None for target in field_value]):
            if len(set(target.target_accession.is_base_editor for target in field_value)) > 1:
                # Throw the error for the first target, since it necessarily has an inconsistent base editor value.
                raise ValidationError(
                    "All target accessions must be of the same base editor type.",
                    custom_loc=[
                        "body",
                        "targetGene",
                        0,
                        "targetAccession",
                        "isBaseEditor",
                    ],
                )

        return field_value

    @model_validator(mode="after")
    def validate_score_range_sources_exist_in_publication_identifiers(self):
        def _check_source_in_score_set(source: Any) -> bool:
            # It looks like you could just do values.get("primary_publication_identifiers", []), but the value of the Pydantic
            # field is not guaranteed to be a list and could be None, so we need to check if it exists and only then add the list
            # as the default value.
            primary_publication_identifiers = self.primary_publication_identifiers or []
            secondary_publication_identifiers = self.secondary_publication_identifiers or []

            if source not in primary_publication_identifiers and source not in secondary_publication_identifiers:
                return False

            return True

        score_ranges = self.score_ranges
        if not score_ranges:
            return self

        # Use the model_fields_set attribute to iterate over the defined containers in score_ranges.
        # This allows us to validate each range definition within the range containers.
        for range_name in score_ranges.model_fields_set:
            range_definition = getattr(score_ranges, range_name)
            if not range_definition:
                continue

            # investigator_provided score ranges can have an odds path source as well.
            if range_name == "investigator_provided" and range_definition.odds_path_source is not None:
                for idx, pub in enumerate(range_definition.odds_path_source):
                    odds_path_source_exists = _check_source_in_score_set(pub)

                    if not odds_path_source_exists:
                        raise ValidationError(
                            f"Odds path source publication at index {idx} is not defined in score set publications. "
                            "To use a publication identifier in the odds path source, it must be defined in the primary or secondary publication identifiers for this score set.",
                            custom_loc=["body", "scoreRanges", range_name, "oddsPathSource", idx],
                        )

            if not range_definition.source:
                continue

            for idx, pub in enumerate(range_definition.source):
                source_exists = _check_source_in_score_set(pub)

                if not source_exists:
                    raise ValidationError(
                        f"Score range source publication at index {idx} is not defined in score set publications. "
                        "To use a publication identifier in the score range source, it must be defined in the primary or secondary publication identifiers for this score set.",
                        custom_loc=["body", "scoreRanges", range_name, "source", idx],
                    )

        return self


class ScoreSetCreate(ScoreSetModify):
    """View model for creating a new score set."""

    experiment_urn: Optional[str] = None
    license_id: int
    superseded_score_set_urn: Optional[str] = None
    meta_analyzes_score_set_urns: Optional[list[str]] = None

    @field_validator("superseded_score_set_urn")
    def validate_superseded_score_set_urn(cls, v: Optional[str]) -> Optional[str]:
        if not v:
            return None

        if urn_re.MAVEDB_SCORE_SET_URN_RE.fullmatch(v) is None:
            if urn_re.MAVEDB_TMP_URN_RE.fullmatch(v) is None:
                raise ValueError(f"'{v}' is not a valid score set URN")
            else:
                raise ValueError("cannot supersede a private score set - please edit it instead")

        return v

    @field_validator("meta_analyzes_score_set_urns")
    def validate_score_set_urn(cls, v: Optional[list[str]]) -> Optional[list[str]]:
        if not v:
            return None

        for s in v:
            if urn_re.MAVEDB_SCORE_SET_URN_RE.fullmatch(s) is None and urn_re.MAVEDB_TMP_URN_RE.fullmatch(s) is None:
                raise ValueError(f"'{s}' is not a valid score set URN")

        return v

    @field_validator("experiment_urn")
    def validate_experiment_urn(cls, v: Optional[str]) -> Optional[str]:
        if not v:
            return None

        if urn_re.MAVEDB_EXPERIMENT_URN_RE.fullmatch(v) is None and urn_re.MAVEDB_TMP_URN_RE.fullmatch(v) is None:
            raise ValueError(f"'{v}' is not a valid experiment URN")

        return v

    @model_validator(mode="after")
    def validate_experiment_urn_required_except_for_meta_analyses(self) -> Self:
        is_meta_analysis = not (
            self.meta_analyzes_score_set_urns is None or len(self.meta_analyzes_score_set_urns) == 0
        )
        if self.experiment_urn is None and not is_meta_analysis:
            raise ValidationError("experiment URN is required unless your score set is a meta-analysis")
        if self.experiment_urn is not None and is_meta_analysis:
            raise ValidationError("experiment URN should not be supplied when your score set is a meta-analysis")
        return self


class ScoreSetUpdate(ScoreSetModify):
    """View model for updating a score set."""

    license_id: Optional[int] = None

class DatasetColumnMetadata(BaseModel):
    """Metadata for individual dataset columns."""

    description: str
    details: Optional[str] = None

class DatasetColumns(BaseModel):
    """Dataset columns view model representing the dataset columns property of a score set."""

    score_columns: Optional[list[str]] = None
    count_columns: Optional[list[str]] = None
    score_columns_metadata: Optional[dict[str, DatasetColumnMetadata]] = None
    count_columns_metadata: Optional[dict[str, DatasetColumnMetadata]] = None

    @field_validator("score_columns_metadata", "count_columns_metadata")
    def validate_dataset_columns_metadata(cls, v: Optional[dict[str, DatasetColumnMetadata]]) -> Optional[dict[str, DatasetColumnMetadata]]:
        if not v:
            return None
        DatasetColumnMetadata.model_validate(v)
        return v

    @model_validator(mode="after")
    def validate_dataset_columns_metadata_keys(self) -> Self:
        if self.score_columns_metadata is not None and self.score_columns is None:
            raise ValidationError("Score columns metadata cannot be provided without score columns.")
        elif self.score_columns_metadata is not None and self.score_columns is not None:
            for key in self.score_columns_metadata.keys():
                if key not in self.score_columns:
                    raise ValidationError(f"Score column metadata key '{key}' does not exist in score_columns list.")

        if self.count_columns_metadata is not None and self.count_columns is None:
            raise ValidationError("Count columns metadata cannot be provided without count columns.")
        elif self.count_columns_metadata is not None and self.count_columns is not None:
            for key in self.count_columns_metadata.keys():
                if key not in self.count_columns:
                    raise ValidationError(f"Count column metadata key '{key}' does not exist in count_columns list.")
        return self

class ShortScoreSet(BaseModel):
    """
    Score set view model containing a smaller set of properties to return in list contexts.

    Notice that this is not derived from ScoreSetBase.
    """

    urn: str
    title: str
    short_description: str
    published_date: Optional[date] = None
    replaces_id: Optional[int] = None
    num_variants: int
    experiment: "Experiment"
    primary_publication_identifiers: list[SavedPublicationIdentifier]
    secondary_publication_identifiers: list[SavedPublicationIdentifier]
    license: ShortLicense
    creation_date: date
    modification_date: date
    target_genes: list[ShortTargetGene]
    private: bool
    record_type: str = None  # type: ignore

    _record_type_factory = record_type_validator()(set_record_type)

    class Config:
        from_attributes = True
        arbitrary_types_allowed = True

    # These 'synthetic' fields are generated from other model properties. Transform data from other properties as needed, setting
    # the appropriate field on the model itself. Then, proceed with Pydantic ingestion once fields are created.
    @model_validator(mode="before")
    def generate_primary_and_secondary_publications(cls, data: Any):
        if not hasattr(data, "primary_publication_identifiers") or not hasattr(data, "primary_publication_identifiers"):
            try:
                publication_identifiers = transform_publication_identifiers_to_primary_and_secondary(
                    data.publication_identifier_associations
                )
                data.__setattr__(
                    "primary_publication_identifiers", publication_identifiers["primary_publication_identifiers"]
                )
                data.__setattr__(
                    "secondary_publication_identifiers", publication_identifiers["secondary_publication_identifiers"]
                )
            except AttributeError as exc:
                raise ValidationError(
                    f"Unable to create {cls.__name__} without attribute: {exc}."  # type: ignore
                )
        return data


class ShorterScoreSet(BaseModel):
    urn: str
    record_type: str = None  # type: ignore

    _record_type_factory = record_type_validator()(set_record_type)

    class Config:
        from_attributes = True
        arbitrary_types_allowed = True


class SavedScoreSet(ScoreSetBase):
    """Base class for score set view models representing saved records."""

    record_type: str = None  # type: ignore
    urn: str
    num_variants: int
    license: ShortLicense
    superseded_score_set: Optional[ShorterScoreSet] = None
    superseding_score_set: Optional[ShorterScoreSet] = None
    meta_analyzes_score_set_urns: list[str]
    meta_analyzed_by_score_set_urns: list[str]
    doi_identifiers: Sequence[SavedDoiIdentifier]
    primary_publication_identifiers: Sequence[SavedPublicationIdentifier]
    secondary_publication_identifiers: Sequence[SavedPublicationIdentifier]
    published_date: Optional[date] = None
    creation_date: date
    modification_date: date
    created_by: Optional[SavedUser] = None
    modified_by: Optional[SavedUser] = None
    target_genes: Sequence[SavedTargetGene]
    dataset_columns: DatasetColumns
    external_links: dict[str, ExternalLink]
    contributors: Sequence[Contributor]
    score_ranges: Optional[SavedScoreSetRanges] = None

    _record_type_factory = record_type_validator()(set_record_type)

    class Config:
        from_attributes = True
        arbitrary_types_allowed = True

    # Association proxy objects return an untyped _AssocitionList object.
    # Recast it into something more generic.
    @field_validator("secondary_publication_identifiers", "primary_publication_identifiers", mode="before")
    def publication_identifiers_validator(cls, value: Any) -> list[PublicationIdentifier]:
        assert isinstance(value, Collection), "Publication identifier lists must be a collection"
        return list(value)  # Re-cast into proper list-like type

    @field_validator("dataset_columns")
    def camelize_dataset_columns_keys(cls, value: dict) -> dict:
        return camelize(value)

    # These 'synthetic' fields are generated from other model properties. Transform data from other properties as needed, setting
    # the appropriate field on the model itself. Then, proceed with Pydantic ingestion once fields are created.
    @model_validator(mode="before")
    def generate_primary_and_secondary_publications(cls, data: Any):
        if not hasattr(data, "primary_publication_identifiers") or not hasattr(data, "primary_publication_identifiers"):
            try:
                publication_identifiers = transform_publication_identifiers_to_primary_and_secondary(
                    data.publication_identifier_associations
                )
                data.__setattr__(
                    "primary_publication_identifiers", publication_identifiers["primary_publication_identifiers"]
                )
                data.__setattr__(
                    "secondary_publication_identifiers", publication_identifiers["secondary_publication_identifiers"]
                )
            except AttributeError as exc:
                raise ValidationError(
                    f"Unable to create {cls.__name__} without attribute: {exc}."  # type: ignore
                )
        return data

    @model_validator(mode="before")
    def transform_meta_analysis_objects_to_urns(cls, data: Any):
        if not hasattr(data, "meta_analyzes_score_set_urns"):
            try:
                data.__setattr__(
                    "meta_analyzes_score_set_urns", transform_score_set_list_to_urn_list(data.meta_analyzes_score_sets)
                )
            except AttributeError as exc:
                raise ValidationError(f"Unable to create {cls.__name__} without attribute: {exc}.")  # type: ignore
        return data

    @model_validator(mode="before")
    def transform_meta_analyzed_objects_to_urns(cls, data: Any):
        if not hasattr(data, "meta_analyzed_by_score_set_urns"):
            try:
                data.__setattr__(
                    "meta_analyzed_by_score_set_urns",
                    transform_score_set_list_to_urn_list(data.meta_analyzed_by_score_sets),
                )
            except AttributeError as exc:
                raise ValidationError(f"Unable to create {cls.__name__} without attribute: {exc}.")  # type: ignore
        return data


class ScoreSet(SavedScoreSet):
    """Score set view model containing most properties visible to non-admin users, but no variant data."""

    experiment: "Experiment"
    doi_identifiers: Sequence[DoiIdentifier]
    primary_publication_identifiers: Sequence[PublicationIdentifier]
    secondary_publication_identifiers: Sequence[PublicationIdentifier]
    official_collections: Sequence[OfficialCollection]
    created_by: Optional[User] = None
    modified_by: Optional[User] = None
    target_genes: Sequence[TargetGene]
    private: bool
    processing_state: Optional[ProcessingState] = None
    processing_errors: Optional[dict] = None
    mapping_state: Optional[MappingState] = None
    mapping_errors: Optional[dict] = None
    score_ranges: Optional[ScoreSetRanges] = None  # type: ignore[assignment]


class ScoreSetWithVariants(ScoreSet):
    """
    Score set view model containing a complete set of properties visible to non-admin users, for contexts where variants
    are requested.
    """

    variants: list[SavedVariantEffectMeasurement]


class AdminScoreSet(ScoreSet):
    """Score set view model containing properties to return to admin clients."""

    normalised: bool
    approved: bool


class ScoreSetPublicDump(SavedScoreSet):
    """Score set view model containing properties to include in a dump of all published data."""

    doi_identifiers: Sequence[DoiIdentifier]
    primary_publication_identifiers: Sequence[PublicationIdentifier]
    secondary_publication_identifiers: Sequence[PublicationIdentifier]
    created_by: Optional[User] = None
    modified_by: Optional[User] = None
    target_genes: Sequence[TargetGene]
    private: bool
    contributors: Sequence[Contributor]
    processing_state: Optional[ProcessingState] = None
    processing_errors: Optional[dict] = None
    mapping_state: Optional[MappingState] = None
    mapping_errors: Optional[dict] = None
    score_ranges: Optional[ScoreSetRanges] = None  # type: ignore[assignment]


# ruff: noqa: E402
from mavedb.view_models.experiment import Experiment
from mavedb.view_models.variant import SavedVariantEffectMeasurement

ScoreSetWithVariants.model_rebuild()
ShortScoreSet.model_rebuild()
ScoreSet.model_rebuild()
ScoreSetWithVariants.model_rebuild()
