from datetime import date
from typing import Any, Sequence

from pydantic import Field
from pydantic.types import Optional

from mavedb.view_models import UserContributionRoleGetter, record_type_validator, set_record_type
from mavedb.view_models.base.base import BaseModel
from mavedb.view_models.contributor import ContributorCreate
from mavedb.view_models.user import SavedUser, User


class CollectionGetter(UserContributionRoleGetter):
    def get(self, key: Any, default: Any = ...) -> Any:
        if key == "score_set_urns":
            score_sets = getattr(self._obj, "score_sets") or []
            return sorted([score_set.urn for score_set in score_sets if score_set.superseding_score_set is None])
        elif key == "experiment_urns":
            experiments = getattr(self._obj, "experiments") or []
            return sorted([experiment.urn for experiment in experiments])
        else:
            return super().get(key, default)


class CollectionBase(BaseModel):
    private: bool = Field(
        description="Whether the collection is visible to all MaveDB users. If set during collection update, input ignored unless requesting user is collection admin."
    )
    name: str
    description: Optional[str]
    badge_name: Optional[str] = Field(
        description="Badge name. Input ignored unless requesting user has MaveDB admin privileges."
    )


class CollectionModify(BaseModel):
    # all fields should be optional, because the client should specify only the fields they want to update
    private: Optional[bool] = Field(
        description="Whether the collection is visible to all MaveDB users. If set during collection update, input ignored unless requesting user is collection admin."
    )
    name: Optional[str]
    description: Optional[str]
    badge_name: Optional[str] = Field(
        description="Badge name. Input ignored unless requesting user has MaveDB admin privileges."
    )


class CollectionCreate(CollectionBase):
    experiment_urns: Optional[list[str]]
    score_set_urns: Optional[list[str]]

    viewers: Optional[list[ContributorCreate]]
    editors: Optional[list[ContributorCreate]]
    admins: Optional[list[ContributorCreate]]


class AddScoreSetToCollectionRequest(BaseModel):
    score_set_urn: str


class AddExperimentToCollectionRequest(BaseModel):
    experiment_urn: str


class AddUserToCollectionRoleRequest(BaseModel):
    orcid_id: str


# Properties shared by models stored in DB
class SavedCollection(CollectionBase):
    record_type: str = None  # type: ignore
    urn: str

    created_by: Optional[SavedUser]
    modified_by: Optional[SavedUser]

    experiment_urns: list[str]
    score_set_urns: list[str]

    admins: Sequence[SavedUser]
    viewers: Sequence[SavedUser]
    editors: Sequence[SavedUser]

    creation_date: date
    modification_date: date

    _record_type_factory = record_type_validator()(set_record_type)

    class Config:
        orm_mode = True
        getter_dict = CollectionGetter


# Properties to return to non-admin clients
# NOTE: Coupled to ContributionRole enum
class Collection(SavedCollection):
    created_by: Optional[User]
    modified_by: Optional[User]

    admins: Sequence[User]
    viewers: Sequence[User]
    editors: Sequence[User]


# Properties to return to admin clients or non-admin clients who are admins of the returned collection
# NOTE: Coupled to ContributionRole enum
class AdminCollection(Collection):
    pass
