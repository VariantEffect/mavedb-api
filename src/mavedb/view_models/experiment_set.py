from datetime import date
from typing import Sequence

from pydantic.types import Optional

from mavedb.view_models import record_type_validator, set_record_type
from mavedb.view_models.base.base import BaseModel
from mavedb.view_models.contributor import Contributor
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
    record_type: str = None  # type: ignore
    experiments: Sequence[SavedExperiment]
    created_by: Optional[SavedUser]
    modified_by: Optional[SavedUser]
    creation_date: date
    modification_date: date
    contributors: list[Contributor]

    _record_type_factory = record_type_validator()(set_record_type)

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
