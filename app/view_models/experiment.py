from datetime import date
from pydantic import validator
from pydantic.types import Optional
from typing import Any, Dict

from app.view_models.base.base import BaseModel
from app.view_models.user import SavedUser, User


class ExperimentBase(BaseModel):
    urn: Optional[str]
    title: str
    method_text: str
    abstract_text: str
    short_description: str
    extra_metadata: Dict

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
    pass


class ExperimentUpdate(ExperimentBase):
    pass


# Properties shared by models stored in DB
class SavedExperiment(ExperimentBase):
    # id: int
    num_scoresets: int
    created_by: Optional[SavedUser]
    modified_by: Optional[SavedUser]
    creation_date: date
    modification_date: date
    published_date: Optional[date]
    experiment_set_urn: Optional[str]

    class Config:
        orm_mode = True


# Properties to return to non-admin clients
class Experiment(SavedExperiment):
    created_by: Optional[User]
    modified_by: Optional[User]


# Properties to return to admin clients
class AdminExperiment(SavedExperiment):
    approved: bool
    processing_state: Optional[str]
    created_by: Optional[User]
    modified_by: Optional[User]
