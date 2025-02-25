from datetime import date
from typing import Any, Collection, Optional, Sequence

from mavedb.lib.validation.exceptions import ValidationError
from mavedb.lib.validation.utilities import is_null
from mavedb.view_models import PublicationIdentifiersGetter, record_type_validator, set_record_type
from mavedb.view_models.base.base import BaseModel, validator
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


class OfficialCollection(BaseModel):
    badge_name: str
    name: str
    urn: str

    class Config:
        orm_mode = True
        arbitrary_types_allowed = True


class ExperimentGetter(PublicationIdentifiersGetter):
    def get(self, key: Any, default: Any = ...) -> Any:
        if key == "score_set_urns":
            score_sets = getattr(self._obj, "score_sets") or []
            return sorted([score_set.urn for score_set in score_sets if score_set.superseding_score_set is None])
        elif key == "experiment_set_urn":
            experiment_set = getattr(self._obj, "experiment_set")
            return experiment_set.urn if experiment_set is not None else None
        else:
            return super().get(key, default)


class ExperimentBase(BaseModel):
    title: str
    short_description: str
    abstract_text: Optional[str]
    method_text: Optional[str]
    extra_metadata: Optional[dict]

    @classmethod
    def from_orm(cls, obj: Any):
        try:
            obj.experiment_set_urn = obj.experiment_set.urn
        except AttributeError:
            obj.experiment_set_urn = None
        return super().from_orm(obj)


class ExperimentModify(ExperimentBase):
    abstract_text: str
    method_text: str
    contributors: Optional[list[ContributorCreate]]
    keywords: Optional[list[ExperimentControlledKeywordCreate]]
    doi_identifiers: Optional[list[DoiIdentifierCreate]]
    primary_publication_identifiers: Optional[list[PublicationIdentifierCreate]]
    secondary_publication_identifiers: Optional[list[PublicationIdentifierCreate]]
    raw_read_identifiers: Optional[list[RawReadIdentifierCreate]]

    @validator("primary_publication_identifiers")
    def max_one_primary_publication_identifier(cls, v):
        if isinstance(v, list):
            if len(v) > 1:
                raise ValidationError("multiple primary publication identifiers are not allowed")
        return v

    @validator("title", "short_description", "abstract_text", "method_text")
    def validate_field_is_non_empty(cls, v):
        if is_null(v) or not isinstance(v, str):
            raise ValidationError("This field is required and cannot be empty.")
        return v.strip()


class ExperimentCreate(ExperimentModify):
    experiment_set_urn: Optional[str]


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
    published_date: Optional[date]
    experiment_set_urn: str
    doi_identifiers: Sequence[SavedDoiIdentifier]
    primary_publication_identifiers: Sequence[SavedPublicationIdentifier]
    secondary_publication_identifiers: Sequence[SavedPublicationIdentifier]
    raw_read_identifiers: Sequence[SavedRawReadIdentifier]
    contributors: list[Contributor]
    keywords: Sequence[SavedExperimentControlledKeyword]

    _record_type_factory = record_type_validator()(set_record_type)

    class Config:
        orm_mode = True
        getter_dict = ExperimentGetter

    # Association proxy objects return an untyped _AssocitionList object.
    # Recast it into something more generic.
    @validator("secondary_publication_identifiers", "primary_publication_identifiers", pre=True)
    def publication_identifiers_validator(cls, value, values, field) -> list[PublicationIdentifier]:
        assert isinstance(value, Collection), f"{field.name} must be a collection, not {type(value)}"
        return list(value)  # Re-cast into proper list-like type


# Properties to return to non-admin clients
class Experiment(SavedExperiment):
    score_set_urns: list[str]
    processing_state: Optional[str]
    doi_identifiers: Sequence[DoiIdentifier]
    primary_publication_identifiers: Sequence[PublicationIdentifier]
    secondary_publication_identifiers: Sequence[PublicationIdentifier]
    official_collections: Sequence[OfficialCollection]
    keywords: Sequence[ExperimentControlledKeyword]
    raw_read_identifiers: Sequence[RawReadIdentifier]
    created_by: User
    modified_by: User


class ShortExperiment(SavedExperiment):
    score_set_urns: list[str]
    processing_state: Optional[str]


# Properties to return to admin clients
class AdminExperiment(Experiment):
    score_set_urns: list[str]
    processing_state: Optional[str]
    approved: bool


# Properties to include in a dump of all published data.
class ExperimentPublicDump(SavedExperiment):
    score_sets: "Sequence[ScoreSetPublicDump]"


# ruff: noqa: E402
from mavedb.view_models.score_set import ScoreSetPublicDump

ExperimentPublicDump.update_forward_refs()
