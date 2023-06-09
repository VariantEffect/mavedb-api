from datetime import date
from typing import Any, Collection, Dict, Optional

from pydantic import Field

from mavedb.lib.validation import keywords
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
    short_description: Optional[str]
    abstract_text: Optional[str]
    method_text: Optional[str]
    extra_metadata: Dict
    keywords: Optional[list[str]]

    @classmethod
    def from_orm(cls, obj: Any):
        try:
            obj.experiment_set_urn = obj.experiment_set.urn
        except AttributeError:
            obj.experiment_set_urn = None
        return super().from_orm(obj)

    # @validator('urn')
    # def name_must_contain_space(cls, v):
    #    if ' ' not in v:
    #        raise ValueError('must contain a space')
    #    return v.title()


class ExperimentModify(ExperimentBase):
    @validator("keywords")
    def validate_keywords(cls, v):
        keywords.validate_keywords(v)
        return v


class ExperimentCreate(ExperimentModify):
    experiment_set_urn: Optional[str]
    short_description: str
    abstract_text: str
    method_text: str
    doi_identifiers: Optional[list[DoiIdentifierCreate]]
    primary_publication_identifiers: Optional[list[PublicationIdentifierCreate]] = Field(..., min_items=0, max_items=1)
    publication_identifiers: Optional[list[PublicationIdentifierCreate]]
    raw_read_identifiers: Optional[list[RawReadIdentifierCreate]]


class ExperimentUpdate(ExperimentModify):
    short_description: str
    abstract_text: str
    method_text: str
    doi_identifiers: Optional[list[DoiIdentifierCreate]]
    primary_publication_identifiers: Optional[list[PublicationIdentifierCreate]] = Field(..., min_items=0, max_items=1)
    publication_identifiers: Optional[list[PublicationIdentifierCreate]]
    raw_read_identifiers: Optional[list[RawReadIdentifierCreate]]


# Properties shared by models stored in DB
class SavedExperiment(ExperimentBase):
    urn: str
    num_scoresets: int
    created_by: Optional[SavedUser]
    modified_by: Optional[SavedUser]
    creation_date: date
    modification_date: date
    published_date: Optional[date]
    experiment_set_urn: Optional[str]
    doi_identifiers: list[SavedDoiIdentifier]
    primary_publication_identifiers: list[SavedPublicationIdentifier]
    secondary_publication_identifiers: list[SavedPublicationIdentifier]
    raw_read_identifiers: list[SavedRawReadIdentifier]
    processing_state: Optional[str]

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
    created_by: Optional[User]
    modified_by: Optional[User]


class ShortExperiment(Experiment):
    pass


# Properties to return to admin clients
class AdminExperiment(Experiment):
    approved: bool
