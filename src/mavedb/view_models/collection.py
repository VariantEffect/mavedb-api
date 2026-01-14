from datetime import date
from typing import Any, Optional, Sequence

from pydantic import Field, model_validator

from mavedb.lib.validation.exceptions import ValidationError
from mavedb.lib.validation.transform import (
    transform_contribution_role_associations_to_roles,
    transform_experiment_list_to_urn_list,
    transform_score_set_list_to_urn_list,
)
from mavedb.view_models import record_type_validator, set_record_type
from mavedb.view_models.base.base import BaseModel
from mavedb.view_models.contributor import ContributorCreate
from mavedb.view_models.user import SavedUser, User


class CollectionBase(BaseModel):
    private: bool = Field(
        description="Whether the collection is visible to all MaveDB users. If set during collection update, input ignored unless requesting user is collection admin.",
    )
    name: str
    description: Optional[str] = None
    badge_name: Optional[str] = Field(
        description="Badge name. Input ignored unless requesting user has MaveDB admin privileges.", default=None
    )


class CollectionModify(BaseModel):
    # all fields should be optional, because the client should specify only the fields they want to update
    private: Optional[bool] = Field(
        description="Whether the collection is visible to all MaveDB users. If set during collection update, input ignored unless requesting user is collection admin.",
        default=None,
    )
    name: Optional[str] = None
    description: Optional[str] = None
    badge_name: Optional[str] = Field(
        description="Badge name. Input ignored unless requesting user has MaveDB admin privileges.", default=None
    )


class CollectionCreate(CollectionBase):
    experiment_urns: Optional[list[str]] = []
    score_set_urns: Optional[list[str]] = []

    viewers: Optional[list[ContributorCreate]] = []
    editors: Optional[list[ContributorCreate]] = []
    admins: Optional[list[ContributorCreate]] = []


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

    created_by: Optional[SavedUser] = None
    modified_by: Optional[SavedUser] = None

    experiment_urns: list[str]
    score_set_urns: list[str]

    admins: Sequence[SavedUser]
    viewers: Sequence[SavedUser]
    editors: Sequence[SavedUser]

    creation_date: date
    modification_date: date

    _record_type_factory = record_type_validator()(set_record_type)

    class Config:
        from_attributes = True

    # These 'synthetic' fields are generated from other model properties. Transform data from other properties as needed, setting
    # the appropriate field on the model itself. Then, proceed with Pydantic ingestion once fields are created.
    @model_validator(mode="before")
    def generate_contribution_role_user_relationships(cls, data: Any):
        try:
            user_associations = transform_contribution_role_associations_to_roles(data.user_associations)
            for k, v in user_associations.items():
                data.__setattr__(k, v)

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
    def generate_experiment_urn_list(cls, data: Any):
        if not hasattr(data, "experiment_urns"):
            try:
                data.__setattr__("experiment_urns", transform_experiment_list_to_urn_list(data.experiments))
            except AttributeError as exc:
                raise ValidationError(f"Unable to create {cls.__name__} without attribute: {exc}.")  # type: ignore
        return data


# Properties to return to non-admin clients
# NOTE: Coupled to ContributionRole enum
class Collection(SavedCollection):
    created_by: Optional[User] = None
    modified_by: Optional[User] = None

    admins: Sequence[User]
    viewers: Sequence[User]
    editors: Sequence[User]


# Properties to return to admin clients or non-admin clients who are admins of the returned collection
# NOTE: Coupled to ContributionRole enum
class AdminCollection(Collection):
    pass


# Properties to return for official collections
class OfficialCollection(BaseModel):
    badge_name: str
    name: str
    urn: str

    class Config:
        arbitrary_types_allowed = True
        from_attributes = True
