# See https://pydantic-docs.helpmanual.io/usage/postponed_annotations/#self-referencing-models
from __future__ import annotations

from datetime import date
from typing import Any, Collection, Dict, Optional, Sequence

from humps import camelize
from pydantic import root_validator

from mavedb.lib.validation import urn_re
from mavedb.lib.validation.constants.score_set import default_ranges
from mavedb.lib.validation.exceptions import ValidationError
from mavedb.lib.validation.utilities import inf_or_float, is_null
from mavedb.models.enums.mapping_state import MappingState
from mavedb.models.enums.processing_state import ProcessingState
from mavedb.view_models import PublicationIdentifiersGetter, record_type_validator, set_record_type
from mavedb.view_models.base.base import BaseModel, validator
from mavedb.view_models.calibration import Calibration
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
from mavedb.view_models.target_gene import (
    SavedTargetGene,
    ShortTargetGene,
    TargetGene,
    TargetGeneCreate,
)
from mavedb.view_models.user import SavedUser, User
from mavedb.view_models.variant import VariantInDbBase


class ExternalLink(BaseModel):
    url: Optional[str]


class OfficialCollection(BaseModel):
    badge_name: str
    name: str
    urn: str

    class Config:
        orm_mode = True
        arbitrary_types_allowed = True


class ScoreRange(BaseModel):
    label: str
    description: Optional[str]
    classification: str
    # Purposefully vague type hint because of some odd JSON Schema generation behavior.
    # Typing this as tuple[Union[float, None], Union[float, None]] will generate an invalid
    # jsonschema, and fail all tests that access the schema. This may be fixed in pydantic v2,
    # but it's unclear. Even just typing it as Tuple[Any, Any] will generate an invalid schema!
    range: list[Any]  # really: tuple[Union[float, None], Union[float, None]]

    @validator("classification")
    def range_classification_value_is_accepted(cls, field_value: str):
        classification = field_value.strip().lower()
        if classification not in default_ranges:
            raise ValidationError(
                f"Unexpected classification value(s): {classification}. Permitted values: {default_ranges}"
            )

        return classification

    @validator("range")
    def ranges_are_not_backwards(cls, field_value: tuple[Any]):
        if len(field_value) != 2:
            raise ValidationError("Only a lower and upper bound are allowed.")

        field_value[0] = inf_or_float(field_value[0], True) if field_value[0] is not None else None
        field_value[1] = inf_or_float(field_value[1], False) if field_value[1] is not None else None

        if inf_or_float(field_value[0], True) > inf_or_float(field_value[1], False):
            raise ValidationError("The lower bound of the score range may not be larger than the upper bound.")
        elif inf_or_float(field_value[0], True) == inf_or_float(field_value[1], False):
            raise ValidationError("The lower and upper bound of the score range may not be the same.")

        return field_value


class ScoreRanges(BaseModel):
    wt_score: float
    ranges: list[ScoreRange]  # type: ignore


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
    primary_publication_identifiers: Optional[list[PublicationIdentifierCreate]]
    secondary_publication_identifiers: Optional[list[PublicationIdentifierCreate]]
    doi_identifiers: Optional[list[DoiIdentifierCreate]]
    target_genes: list[TargetGeneCreate]
    score_ranges: Optional[ScoreRanges]

    @validator("title", "short_description", "abstract_text", "method_text")
    def validate_field_is_non_empty(cls, v):
        if is_null(v) or not isinstance(v, str):
            raise ValidationError("This field is required and cannot be empty.")
        return v.strip()

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
                        custom_loc=[
                            "body",
                            "targetGene",
                            idx,
                            "targetSequence",
                            "label",
                        ],
                    )

        return field_value

    # Validate nested label fields are not identical
    @validator("target_genes")
    def target_labels_are_unique(cls, field_value, values):
        # Labels are only used on target sequence instances.
        if len(field_value) > 1 and all([target.target_sequence is not None for target in field_value]):
            # Labels have already been sanitized by the TargetSequence validator.
            labels = [target.target_sequence.label for target in field_value]
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

        return field_value

    # Validate that this score set contains at least one target attached to it
    @validator("target_genes")
    def at_least_one_target_gene_exists(cls, field_value, values):
        if len(field_value) < 1:
            raise ValidationError("Score sets should define at least one target.")

        return field_value

    @validator("score_ranges")
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

    @validator("score_ranges")
    def ranges_contain_normal_and_abnormal(cls, field_value: Optional[ScoreRanges]):
        if field_value is None:
            return None

        ranges = set([range_model.classification for range_model in field_value.ranges])
        if not set(default_ranges).issubset(ranges):
            raise ValidationError(
                "Both `normal` and `abnormal` ranges must be provided.",
                # Raise this error inside the first classification provided by the model.
                custom_loc=["body", "scoreRanges", "ranges", 0, "classification"],
            )

        return field_value

    @validator("score_ranges")
    def ranges_do_not_overlap(cls, field_value: Optional[ScoreRanges]):
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

        if field_value is None:
            return None

        for i, range_test in enumerate(field_value.ranges):
            for range_check in list(field_value.ranges)[i + 1 :]:
                if test_overlap(range_test.range, range_check.range):
                    raise ValidationError(
                        f"Score ranges may not overlap; `{range_test.label}` overlaps with `{range_check.label}`",
                        custom_loc=["body", "scoreRanges", "ranges", i, "range"],
                    )

        return field_value

    @validator("score_ranges")
    def wild_type_score_in_normal_range(cls, field_value: Optional[ScoreRanges]):
        if field_value is None:
            return None

        normal_ranges = [
            range_model.range for range_model in field_value.ranges if range_model.classification == "normal"
        ]
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
        orm_mode = True
        arbitrary_types_allowed = True
        getter_dict = ScoreSetGetter


class ShorterScoreSet(BaseModel):
    urn: str
    record_type: str = None  # type: ignore

    _record_type_factory = record_type_validator()(set_record_type)

    class Config:
        orm_mode = True
        arbitrary_types_allowed = True
        getter_dict = ScoreSetGetter


class SavedScoreSet(ScoreSetBase):
    """Base class for score set view models representing saved records."""

    record_type: str = None  # type: ignore
    urn: str
    num_variants: int
    license: ShortLicense
    superseded_score_set: Optional[ShorterScoreSet]
    superseding_score_set: Optional[ShorterScoreSet]
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
    external_links: Dict[str, ExternalLink]
    contributors: list[Contributor]
    score_ranges: Optional[ScoreRanges]
    score_calibrations: Optional[dict[str, Calibration]]

    _record_type_factory = record_type_validator()(set_record_type)

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

    experiment: "Experiment"
    doi_identifiers: Sequence[DoiIdentifier]
    primary_publication_identifiers: Sequence[PublicationIdentifier]
    secondary_publication_identifiers: Sequence[PublicationIdentifier]
    official_collections: Sequence[OfficialCollection]
    created_by: Optional[User]
    modified_by: Optional[User]
    target_genes: Sequence[TargetGene]
    private: bool
    processing_state: Optional[ProcessingState]
    processing_errors: Optional[dict]
    mapping_state: Optional[MappingState]
    mapping_errors: Optional[dict]


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


class ScoreSetPublicDump(SavedScoreSet):
    """Score set view model containing properties to include in a dump of all published data."""

    doi_identifiers: Sequence[DoiIdentifier]
    primary_publication_identifiers: Sequence[PublicationIdentifier]
    secondary_publication_identifiers: Sequence[PublicationIdentifier]
    created_by: Optional[User]
    modified_by: Optional[User]
    target_genes: Sequence[TargetGene]
    private: bool
    processing_state: Optional[ProcessingState]
    processing_errors: Optional[Dict]
    mapping_state: Optional[MappingState]
    mapping_errors: Optional[Dict]


# ruff: noqa: E402
from mavedb.view_models.experiment import Experiment

ShortScoreSet.update_forward_refs()
ScoreSet.update_forward_refs()
