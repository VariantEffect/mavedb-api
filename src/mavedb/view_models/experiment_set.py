from datetime import date
from typing import List, Sequence

from pydantic.types import Optional

from mavedb.view_models.base.base import BaseModel
from mavedb.view_models.experiment import Experiment, ExperimentPublicDump, SavedExperiment
from mavedb.view_models.user import SavedUser, User


class ExperimentSetBase(BaseModel):
    urn: str
    published_date: Optional[date]


class ExperimentSetCreate(ExperimentSetBase):
    pass


class ExperimentSetUpdate(ExperimentSetBase):
    pass


# Properties shared by models stored in DB
class SavedExperimentSet(ExperimentSetBase):
    id: int
    experiments: Sequence[SavedExperiment]
    created_by: Optional[SavedUser]
    modified_by: Optional[SavedUser]
    creation_date: date
    modification_date: date

    class Config:
        orm_mode = True


# Properties to return to non-admin clients
class ExperimentSet(SavedExperimentSet):
    created_by: Optional[User]
    modified_by: Optional[User]
    experiments: Sequence[Experiment]


# Properties to return to admin clients
class AdminExperimentSet(SavedExperimentSet):
    private: bool
    approved: bool
    processing_state: Optional[str]
    created_by: Optional[User]
    modified_by: Optional[User]
    experiments: Sequence[Experiment]


# Properties to include in a dump of all published data.
class ExperimentSetPublicDump(SavedExperimentSet):
    experiments: Sequence[ExperimentPublicDump]
    created_by: Optional[User]
    modified_by: Optional[User]
