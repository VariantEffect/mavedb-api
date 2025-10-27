# See https://pydantic-docs.helpmanual.io/usage/postponed_annotations/#self-referencing-models
from __future__ import annotations

import json
from copy import deepcopy
from datetime import date
from typing import Any, Callable, Collection, Optional, Sequence, Type, TypeVar, Union

from pydantic import create_model, field_validator, model_validator
from pydantic.fields import FieldInfo
from typing_extensions import Self

from mavedb.lib.validation import urn_re
from mavedb.lib.validation.exceptions import ValidationError
from mavedb.lib.validation.transform import (
    transform_publication_identifiers_to_primary_and_secondary,
    transform_score_set_list_to_urn_list,
)
from mavedb.lib.validation.utilities import is_null
from mavedb.models.enums.mapping_state import MappingState
from mavedb.models.enums.processing_state import ProcessingState
from mavedb.view_models import record_type_validator, set_record_type
from mavedb.view_models.base.base import BaseModel
from mavedb.view_models.contributor import Contributor, ContributorCreate
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
from mavedb.view_models.score_range import SavedScoreSetRanges, ScoreSetRanges, ScoreSetRangesCreate
from mavedb.view_models.score_set_dataset_columns import DatasetColumns, SavedDatasetColumns
from mavedb.view_models.target_gene import (
    SavedTargetGene,
    ShortTargetGene,
    TargetGene,
    TargetGeneCreate,
)
from mavedb.view_models.user import SavedUser, User

UnboundedRange = tuple[Union[float, None], Union[float, None]]

Model = TypeVar("Model", bound=BaseModel)


def all_fields_optional_model() -> Callable[[Type[Model]], Type[Model]]:
    """A decorator that create a partial model.

    Args:
        model (Type[BaseModel]): BaseModel model.

    Returns:
        Type[BaseModel]: ModelBase partial model.
    """

    def wrapper(model: Type[Model]) -> Type[Model]:
        def make_field_optional(field: FieldInfo, default: Any = None) -> tuple[Any, FieldInfo]:
            new = deepcopy(field)
            new.default = default
            new.annotation = Optional[field.annotation]  # type: ignore[assignment]
            return new.annotation, new

        return create_model(
            model.__name__,
            __base__=model,
            __module__=model.__module__,
            **{field_name: make_field_optional(field_info) for field_name, field_info in model.model_fields.items()},
        )  # type: ignore[call-overload]

    return wrapper


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


class ScoreSetModifyBase(ScoreSetBase):
    contributors: Optional[list[ContributorCreate]] = None
    primary_publication_identifiers: Optional[list[PublicationIdentifierCreate]] = None
    secondary_publication_identifiers: Optional[list[PublicationIdentifierCreate]] = None
    doi_identifiers: Optional[list[DoiIdentifierCreate]] = None
    target_genes: list[TargetGeneCreate]
    score_ranges: Optional[ScoreSetRangesCreate] = None


class ScoreSetModify(ScoreSetModifyBase):
    """View model that adds custom validators to ScoreSetModifyBase."""

    @field_validator("title", "short_description", "abstract_text", "method_text")
    def validate_field_is_non_empty(cls, v: str) -> str:
        if is_null(v):
            raise ValidationError("This field is required and cannot be empty.")
        return v.strip()

    @field_validator("primary_publication_identifiers")
    def max_one_primary_publication_identifier(
        cls, v: list[PublicationIdentifierCreate]
    ) -> list[PublicationIdentifierCreate]:
        if v is not None and len(v) > 1:
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


class ScoreSetUpdateBase(ScoreSetModifyBase):
    """View model for updating a score set with no custom validators."""

    license_id: Optional[int] = None


class ScoreSetUpdate(ScoreSetModify):
    """View model for updating a score set that includes custom validators."""

    license_id: Optional[int] = None


@all_fields_optional_model()
class ScoreSetUpdateAllOptional(ScoreSetUpdateBase):
    @classmethod
    def as_form(cls, **kwargs: Any) -> "ScoreSetUpdateAllOptional":
        """Create ScoreSetUpdateAllOptional from form data."""

        # Define which fields need special JSON parsing
        json_fields = {
            "contributors": lambda data: [ContributorCreate.model_validate(c) for c in data] if data else None,
            "primary_publication_identifiers": lambda data: [
                PublicationIdentifierCreate.model_validate(p) for p in data
            ]
            if data
            else None,
            "secondary_publication_identifiers": lambda data: [
                PublicationIdentifierCreate.model_validate(s) for s in data
            ]
            if data
            else None,
            "doi_identifiers": lambda data: [DoiIdentifierCreate.model_validate(d) for d in data] if data else None,
            "target_genes": lambda data: [TargetGeneCreate.model_validate(t) for t in data] if data else None,
            "score_ranges": lambda data: ScoreSetRangesCreate.model_validate(data) if data else None,
            "extra_metadata": lambda data: data,
        }

        # Process all fields dynamically
        processed_kwargs = {}

        for field_name, value in kwargs.items():
            if field_name in json_fields and value is not None and isinstance(value, str):
                parsed_value = json.loads(value)
                processed_kwargs[field_name] = json_fields[field_name](parsed_value)
            else:
                # All other fields pass through as-is
                processed_kwargs[field_name] = value

        return cls(**processed_kwargs)


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
    dataset_columns: Optional[SavedDatasetColumns] = None
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
    dataset_columns: Optional[DatasetColumns] = None  # type: ignore[assignment]


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
