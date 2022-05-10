from datetime import date
from typing import Any, Dict, Optional

from app.view_models.base.base import BaseModel
from app.view_models.doi_identifier import DoiIdentifierCreate, SavedDoiIdentifier, DoiIdentifier
from app.view_models.pubmed_identifier import PubmedIdentifierCreate, SavedPubmedIdentifier, PubmedIdentifier
from app.view_models.user import SavedUser, User


class ExperimentBase(BaseModel):
    title: str
    short_description: str
    abstract_text: str
    method_text: str
    extra_metadata: Dict
    keywords: Optional[list[str]]

    @classmethod
    def from_orm(cls, obj: Any) -> 'Order':
        try:
            obj.experiment_set_urn = obj.experiment_set.urn
        except AttributeError:
            obj.experiment_set_urn = None
        return super().from_orm(obj)

    #@validator('urn')
    #def name_must_contain_space(cls, v):
    #    if ' ' not in v:
    #        raise ValueError('must contain a space')
    #    return v.title()


class ExperimentCreate(ExperimentBase):
    doi_identifiers: Optional[list[DoiIdentifierCreate]]
    pubmed_identifiers: Optional[list[PubmedIdentifierCreate]]


class ExperimentUpdate(ExperimentBase):
    doi_identifiers: Optional[list[DoiIdentifierCreate]]
    pubmed_identifiers: Optional[list[PubmedIdentifierCreate]]


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
    pubmed_identifiers: list[SavedPubmedIdentifier]
    num_scoresets: int
    processing_state: Optional[str]

    class Config:
        orm_mode = True


# Properties to return to non-admin clients
class Experiment(SavedExperiment):
    doi_identifiers: list[DoiIdentifier]
    pubmed_identifiers: list[PubmedIdentifier]
    created_by: Optional[User]
    modified_by: Optional[User]

class ShortExperiment(Experiment):
    pass

# Properties to return to admin clients
class AdminExperiment(Experiment):
    approved: bool
