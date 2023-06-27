from datetime import date
from typing import Any, Collection, Optional

from mavedb.lib.validation import keywords
from mavedb.lib.validation.exceptions import ValidationError
from mavedb.view_models import PublicationIdentifiersGetter
from mavedb.view_models.base.base import BaseModel, validator
from mavedb.view_models.doi_identifier import (
    DoiIdentifier,
    DoiIdentifierCreate,
    SavedDoiIdentifier,
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


class ExperimentBase(BaseModel):
    title: str
    short_description: str
    abstract_text: str
    method_text: str
    extra_metadata: Optional[dict]

    @classmethod
    def from_orm(cls, obj: Any):
        try:
            obj.experiment_set_urn = obj.experiment_set.urn
        except AttributeError:
            obj.experiment_set_urn = None
        return super().from_orm(obj)


class ExperimentModify(ExperimentBase):
    keywords: Optional[list[str]]
    doi_identifiers: Optional[list[DoiIdentifierCreate]]
    primary_publication_identifiers: Optional[list[PublicationIdentifierCreate]]
    publication_identifiers: Optional[list[PublicationIdentifierCreate]]
    raw_read_identifiers: Optional[list[RawReadIdentifierCreate]]

    @validator("primary_publication_identifiers")
    def max_one_primary_publication_identifier(cls, v):
        if isinstance(v, list):
            if len(v) > 1:
                raise ValidationError("multiple primary publication identifiers are not allowed")
        return v

    @validator("keywords")
    def validate_keywords(cls, v):
        keywords.validate_keywords(v)
        return v


class ExperimentCreate(ExperimentModify):
    experiment_set_urn: Optional[str]


class ExperimentUpdate(ExperimentModify):
    pass


# Properties shared by models stored in DB
class SavedExperiment(ExperimentBase):
    urn: str
    created_by: SavedUser
    modified_by: SavedUser
    creation_date: date
    modification_date: date
    published_date: Optional[date]
    experiment_set_urn: str
    score_set_urns: list[str]
    doi_identifiers: list[SavedDoiIdentifier]
    primary_publication_identifiers: list[SavedPublicationIdentifier]
    secondary_publication_identifiers: list[SavedPublicationIdentifier]
    raw_read_identifiers: list[SavedRawReadIdentifier]
    processing_state: Optional[str]
    keywords: list[str]

    class Config:
        orm_mode = True
        getter_dict = PublicationIdentifiersGetter

    # Association proxy objects return an untyped _AssocitionList object.
    # Recast it into something more generic.
    @validator("secondary_publication_identifiers", "primary_publication_identifiers", pre=True)
    def publication_identifiers_validator(cls, value) -> list[PublicationIdentifier]:
        assert isinstance(value, Collection), "`publication_identifiers` must be a collection"
        return list(value)  # Re-cast into proper list-like type


# Properties to return to non-admin clients
class Experiment(SavedExperiment):
    doi_identifiers: list[DoiIdentifier]
    primary_publication_identifiers: list[PublicationIdentifier]
    secondary_publication_identifiers: list[PublicationIdentifier]
    raw_read_identifiers: list[RawReadIdentifier]
    created_by: User
    modified_by: User


class ShortExperiment(SavedExperiment):
    pass


# Properties to return to admin clients
class AdminExperiment(SavedExperiment):
    approved: bool
