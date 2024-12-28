# See https://pydantic-docs.helpmanual.io/usage/postponed_annotations/#self-referencing-models
from __future__ import annotations

from datetime import date
from typing import Any, Collection, Literal, Optional, Sequence, Union
from typing_extensions import Self

from humps import camelize
from pydantic import field_validator, model_validator

from mavedb.lib.validation import urn_re
from mavedb.lib.validation.exceptions import ValidationError
from mavedb.lib.validation.transform import (
    transform_score_set_list_to_urn_list,
    transform_publication_identifiers_to_primary_and_secondary,
)
from mavedb.lib.validation.utilities import inf_or_float, is_null
from mavedb.models.enums.mapping_state import MappingState
from mavedb.models.enums.processing_state import ProcessingState
from mavedb.view_models import record_type_validator, set_record_type
from mavedb.view_models.base.base import BaseModel
from mavedb.view_models.calibration import Calibration
from mavedb.view_models.contributor import Contributor, SavedContributor, ContributorCreate
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
from mavedb.view_models.target_gene import (
    SavedTargetGene,
    ShortTargetGene,
    TargetGene,
    TargetGeneCreate,
)
from mavedb.view_models.user import SavedUser, User
from mavedb.view_models.variant import SavedVariant


UnboundedRange = tuple[Union[float, None], Union[float, None]]


class ExternalLink(BaseModel):
    url: Optional[str] = None


class OfficialCollection(BaseModel):
    badge_name: str
    name: str
    urn: str

    class Config:
        orm_mode = True
        arbitrary_types_allowed = True


class ScoreRange(BaseModel):
    label: str
    description: Optional[str] = None
    classification: Literal["normal", "abnormal", "not_specified"]
    # Purposefully vague type hint because of some odd JSON Schema generation behavior.
    # Typing this as tuple[Union[float, None], Union[float, None]] will generate an invalid
    # jsonschema, and fail all tests that access the schema. This may be fixed in pydantic v2,
    # but it's unclear. Even just typing it as Tuple[Any, Any] will generate an invalid schema!
    range: UnboundedRange  # list[Any]

    @field_validator("range")
    def ranges_are_not_backwards(cls, value: UnboundedRange) -> UnboundedRange:
        lower_bound = inf_or_float(value[0], True) if value[0] is not None else None
        upper_bound = inf_or_float(value[1], False) if value[1] is not None else None

        if inf_or_float(lower_bound, True) > inf_or_float(upper_bound, False):
            raise ValidationError("The lower bound of the score range may not be larger than the upper bound.")
        elif inf_or_float(lower_bound, True) == inf_or_float(upper_bound, False):
            raise ValidationError("The lower and upper bound of the score range may not be the same.")

        return (lower_bound, upper_bound)


class ScoreRanges(BaseModel):
    wt_score: Optional[float]
    ranges: list[ScoreRange]  # type: ignore


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
    score_ranges: Optional[ScoreRanges] = None

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

    @field_validator("score_ranges")
    def score_range_labels_must_be_unique(cls, field_value: Optional[ScoreRanges]):
        if field_value is None:
            return None

        existing_labels = []
        for i, range_model in enumerate(field_value.ranges):
            range_model.label = range_model.label.strip()

            if range_model.label in existing_labels:
                raise ValidationError(
                    f"Detected repeated label: `{range_model.label}`. Range labels must be unique.",
                    custom_loc=["body", "scoreRanges", "ranges", i, "label"],
                )

            existing_labels.append(range_model.label)

        return field_value

    @field_validator("score_ranges")
    def score_range_normal_classification_exists_if_wild_type_score_provided(cls, field_value: ScoreRanges):
        if field_value is None:
            return None

        if field_value.wt_score is not None:
            if not any([range_model.classification == "normal" for range_model in field_value.ranges]):
                raise ValidationError(
                    "A wild type score has been provided, but no normal classification range exists.",
                    custom_loc=["body", "scoreRanges", "wtScore"],
                )

    @field_validator("score_ranges")
    def ranges_do_not_overlap(cls, field_value: ScoreRanges) -> ScoreRanges:
        def test_overlap(tp1, tp2) -> bool:
            # Always check the tuple with the lowest lower bound. If we do not check
            # overlaps in this manner, checking the overlap of (0,1) and (1,2) will
            # yield different results depending on the ordering of tuples.
            if min(inf_or_float(tp1[0], True), inf_or_float(tp2[0], True)) == inf_or_float(tp1[0], True):
                tp_with_min_value = tp1
                tp_with_non_min_value = tp2
            else:
                tp_with_min_value = tp2
                tp_with_non_min_value = tp1

            if inf_or_float(tp_with_min_value[1], False) > inf_or_float(
                tp_with_non_min_value[0], True
            ) and inf_or_float(tp_with_min_value[0], True) <= inf_or_float(tp_with_non_min_value[1], False):
                return True

            return False

        for i, range_test in enumerate(field_value.ranges):
            for range_check in list(field_value.ranges)[i + 1 :]:
                if test_overlap(range_test.range, range_check.range):
                    raise ValidationError(
                        f"Score ranges may not overlap; `{range_test.label}` overlaps with `{range_check.label}`",
                        custom_loc=["body", "scoreRanges", "ranges", i, "range"],
                    )

        return field_value

    @field_validator("score_ranges")
    def wild_type_score_in_normal_range(cls, field_value: ScoreRanges) -> ScoreRanges:
        normal_ranges = [
            range_model.range for range_model in field_value.ranges if range_model.classification == "normal"
        ]

        if normal_ranges and field_value.wt_score is None:
            raise ValidationError(
                "A normal range has been provided, but no wild type score has been provided.",
                custom_loc=["body", "scoreRanges", "wtScore"],
            )

        if field_value.wt_score is None:
            return field_value

        for range in normal_ranges:
            if field_value.wt_score >= inf_or_float(range[0], lower=True) and field_value.wt_score < inf_or_float(
                range[1], lower=False
            ):
                return field_value

        raise ValidationError(
            f"The provided wild type score of {field_value.wt_score} is not within any of the provided normal ranges. This score should be within a normal range.",
            custom_loc=["body", "scoreRanges", "wtScore"],
        )


class ScoreSetCreate(ScoreSetModify):
    """View model for creating a new score set."""

    experiment_urn: Optional[str] = None
    license_id: int
    superseded_score_set_urn: Optional[str] = None
    meta_analyzes_score_set_urns: Optional[list[str]] = None

    @field_validator("superseded_score_set_urn")
    def validate_superseded_score_set_urn(cls, v: str) -> str:
        if urn_re.MAVEDB_SCORE_SET_URN_RE.fullmatch(v) is None:
            if urn_re.MAVEDB_TMP_URN_RE.fullmatch(v) is None:
                raise ValueError(f"'{v}' is not a valid score set URN")
            else:
                raise ValueError("cannot supersede a private score set - please edit it instead")
        return v

    @field_validator("meta_analyzes_score_set_urns")
    def validate_score_set_urn(cls, v: str) -> str:
        for s in v:
            if urn_re.MAVEDB_SCORE_SET_URN_RE.fullmatch(s) is None and urn_re.MAVEDB_TMP_URN_RE.fullmatch(s) is None:
                raise ValueError(f"'{s}' is not a valid score set URN")
        return v

    @field_validator("experiment_urn")
    def validate_experiment_urn(cls, v: str) -> str:
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
    dataset_columns: dict
    external_links: dict[str, ExternalLink]
    contributors: Sequence[SavedContributor]
    score_ranges: Optional[ScoreRanges] = None
    score_calibrations: Optional[dict[str, Calibration]] = None

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
        try:
            data.__setattr__(
                "meta_analyzes_score_set_urns", transform_score_set_list_to_urn_list(data.meta_analyzes_score_sets)
            )
        except AttributeError as exc:
            raise ValidationError(f"Unable to create {cls.__name__} without attribute: {exc}.")  # type: ignore
        return data

    @model_validator(mode="before")
    def transform_meta_analyzed_objects_to_urns(cls, data: Any):
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
    contributors: Sequence[Contributor]
    processing_state: Optional[ProcessingState] = None
    processing_errors: Optional[dict] = None
    mapping_state: Optional[MappingState] = None
    mapping_errors: Optional[dict] = None


class ScoreSetWithVariants(ScoreSet):
    """
    Score set view model containing a complete set of properties visible to non-admin users, for contexts where variants
    are requested.
    """

    variants: list[SavedVariant]


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
    processing_errors: Optional[dict]
    mapping_state: Optional[MappingState]
    mapping_errors: Optional[dict]


# ruff: noqa: E402
from mavedb.view_models.experiment import Experiment

ShortScoreSet.model_rebuild()
ScoreSet.model_rebuild()
