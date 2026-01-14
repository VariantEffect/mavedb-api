from datetime import date
from typing import TYPE_CHECKING, Any, Collection, Optional, Sequence

from pydantic import ValidationInfo, field_validator, model_validator

from mavedb.lib.validation import urn_re
from mavedb.lib.validation.exceptions import ValidationError
from mavedb.lib.validation.transform import (
    transform_experiment_set_to_urn,
    transform_record_publication_identifiers,
    transform_score_set_list_to_urn_list,
)
from mavedb.lib.validation.utilities import is_null
from mavedb.view_models import record_type_validator, set_record_type
from mavedb.view_models.base.base import BaseModel
from mavedb.view_models.contributor import Contributor, ContributorCreate
from mavedb.view_models.doi_identifier import (
    DoiIdentifier,
    DoiIdentifierCreate,
    SavedDoiIdentifier,
)
from mavedb.view_models.experiment_controlled_keyword import (
    ExperimentControlledKeyword,
    ExperimentControlledKeywordCreate,
    SavedExperimentControlledKeyword,
)
from mavedb.view_models.publication_identifier import (
    PublicationIdentifier,
    PublicationIdentifierCreate,
    SavedPublicationIdentifier,
)
from mavedb.view_models.raw_read_identifier import (
    RawReadIdentifier,
    RawReadIdentifierCreate,
    SavedRawReadIdentifier,
)
from mavedb.view_models.user import SavedUser, User

if TYPE_CHECKING:
    from mavedb.view_models.score_set import ScoreSetPublicDump


class OfficialCollection(BaseModel):
    badge_name: str
    name: str
    urn: str

    class Config:
        arbitrary_types_allowed = True
        from_attributes = True


class ExperimentBase(BaseModel):
    title: str
    short_description: str
    abstract_text: Optional[str] = None
    method_text: Optional[str] = None
    extra_metadata: Optional[dict] = None


class ExperimentModify(ExperimentBase):
    abstract_text: str
    method_text: str
    contributors: Optional[list[ContributorCreate]] = None
    keywords: Optional[list[ExperimentControlledKeywordCreate]] = None
    doi_identifiers: Optional[list[DoiIdentifierCreate]] = None
    primary_publication_identifiers: Optional[list[PublicationIdentifierCreate]] = None
    secondary_publication_identifiers: Optional[list[PublicationIdentifierCreate]] = None
    raw_read_identifiers: Optional[list[RawReadIdentifierCreate]] = None

    @field_validator("primary_publication_identifiers")
    def max_one_primary_publication_identifier(
        cls, v: list[PublicationIdentifierCreate]
    ) -> list[PublicationIdentifierCreate]:
        if len(v) > 1:
            raise ValidationError("Multiple primary publication identifiers are not allowed.")
        return v

    @field_validator("title", "short_description", "abstract_text", "method_text")
    def validate_field_is_non_empty(cls, v: str) -> str:
        if is_null(v):
            raise ValidationError("This field is required and cannot be empty.")
        return v.strip()


class ExperimentCreate(ExperimentModify):
    experiment_set_urn: Optional[str] = None

    @field_validator("experiment_set_urn")
    def validate_experiment_urn(cls, v: Optional[str]) -> Optional[str]:
        if (
            v is not None
            and (urn_re.MAVEDB_EXPERIMENT_SET_URN_RE.fullmatch(v) is None)
            and (urn_re.MAVEDB_TMP_URN_RE.fullmatch(v) is None)
        ):
            raise ValueError(f"'{v}' is not a valid experiment set URN")
        return v


class ExperimentUpdate(ExperimentModify):
    pass


# Properties shared by models stored in DB
class SavedExperiment(ExperimentBase):
    record_type: str = None  # type: ignore
    urn: str
    created_by: SavedUser
    modified_by: SavedUser
    creation_date: date
    modification_date: date
    published_date: Optional[date] = None
    experiment_set_urn: str
    doi_identifiers: Sequence[SavedDoiIdentifier]
    primary_publication_identifiers: Sequence[SavedPublicationIdentifier]
    secondary_publication_identifiers: Sequence[SavedPublicationIdentifier]
    raw_read_identifiers: Sequence[SavedRawReadIdentifier]
    contributors: list[Contributor]
    keywords: Sequence[SavedExperimentControlledKeyword]
    score_set_urns: list[str]

    _record_type_factory = record_type_validator()(set_record_type)

    class Config:
        from_attributes = True

    # Association proxy objects return an untyped _AssociationList object.
    # Recast it into something more generic.
    @field_validator("secondary_publication_identifiers", "primary_publication_identifiers", mode="before")
    def publication_identifiers_validator(cls, v: Any, info: ValidationInfo) -> list[PublicationIdentifier]:
        assert isinstance(v, Collection), f"{info.field_name} must be a collection, not {type(v)}"
        return list(v)  # Re-cast into proper list-like type

    # These 'synthetic' fields are generated from other model properties. Transform data from other properties as needed, setting
    # the appropriate field on the model itself. Then, proceed with Pydantic ingestion once fields are created.
    @model_validator(mode="before")
    def generate_primary_and_secondary_publications(cls, data: Any):
        if not hasattr(data, "primary_publication_identifiers") or not hasattr(
            data, "secondary_publication_identifiers"
        ):
            try:
                publication_identifiers = transform_record_publication_identifiers(
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
    def generate_score_set_urn_list(cls, data: Any):
        if not hasattr(data, "score_set_urns"):
            try:
                data.__setattr__("score_set_urns", transform_score_set_list_to_urn_list(data.score_sets))
            except AttributeError as exc:
                raise ValidationError(f"Unable to create {cls.__name__} without attribute: {exc}.")  # type: ignore
        return data

    @model_validator(mode="before")
    def generate_experiment_set_urn(cls, data: Any):
        if not hasattr(data, "experiment_set_urn"):
            try:
                data.__setattr__("experiment_set_urn", transform_experiment_set_to_urn(data.experiment_set))
            except AttributeError as exc:
                raise ValidationError(f"Unable to create {cls.__name__} without attribute: {exc}.")  # type: ignore
        return data


# Properties to return to non-admin clients
class Experiment(SavedExperiment):
    num_score_sets: Optional[int] = None
    score_set_urns: list[str]
    processing_state: Optional[str] = None
    doi_identifiers: Sequence[DoiIdentifier]
    primary_publication_identifiers: Sequence[PublicationIdentifier]
    secondary_publication_identifiers: Sequence[PublicationIdentifier]
    official_collections: Sequence[OfficialCollection]
    keywords: Sequence[ExperimentControlledKeyword]
    raw_read_identifiers: Sequence[RawReadIdentifier]
    created_by: User
    modified_by: User


class ShortExperiment(SavedExperiment):
    processing_state: Optional[str] = None


# Properties to return to admin clients
class AdminExperiment(Experiment):
    processing_state: Optional[str] = None
    approved: bool


# Properties to include in a dump of all published data.
class ExperimentPublicDump(SavedExperiment):
    score_sets: "Sequence[ScoreSetPublicDump]"
