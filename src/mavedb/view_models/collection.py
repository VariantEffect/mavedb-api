from datetime import date
from typing import Any

from pydantic.types import Optional

from mavedb.view_models import UserContributionRoleGetter
from mavedb.view_models.base.base import BaseModel
from mavedb.view_models.user import SavedUser, User


class CollectionGetter(UserContributionRoleGetter):
    def get(self, key: Any, default: Any = ...) -> Any:
        if key == "score_set_urns":
            score_sets = getattr(self._obj, "score_sets") or []
            return sorted([score_set.urn for score_set in score_sets if score_set.superseding_score_set is None])
        elif key == "experiment_urns":
            experiments = getattr(self._obj, "experiment")
            return [experiment.urn for experiment in experiments]
        else:
            return super().get(key, default)


class CollectionBase(BaseModel):
    private: bool
    name: str
    description: Optional[str]
    badge_name: Optional[str]


class CollectionModify(CollectionBase):
    pass


class CollectionCreate(CollectionModify):
    experiment_urns: Optional[list[str]]
    score_set_urns: Optional[list[str]]

    viewers: Optional[list[User]]
    editors: Optional[list[User]]
    admins: Optional[list[User]]


# Properties shared by models stored in DB
class SavedCollection(CollectionBase):
    id: int
    urn: str
    created_by: Optional[SavedUser]
    modified_by: Optional[SavedUser]
    creation_date: date
    modification_date: date

    class Config:
        orm_mode = True
        getter_dict = CollectionGetter


# Properties to return to non-admin clients
class Collection(SavedCollection):
    experiment_urns: list[str]
    score_set_urns: list[str]


# Properties to return to admin clients
# NOTE: Coupled to ContributionRole enum
class AdminCollection(Collection):
    viewers: list[User]
    editors: list[User]
    admins: list[User]
