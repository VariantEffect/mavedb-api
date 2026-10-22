from datetime import date, datetime
from typing import Optional

from mavedb.models.enums.user_role import UserRole
from mavedb.view_models import record_type_validator, set_record_type
from mavedb.view_models.base.base import BaseModel


class AccessKeyBase(BaseModel):
    key_hint: Optional[str] = None
    name: Optional[str] = None
    expiration_date: Optional[date] = None
    creation_time: Optional[datetime] = None


# Properties shared by models stored in DB
class SavedAccessKey(AccessKeyBase):
    id: int
    record_type: str = None  # type: ignore
    role: Optional[UserRole] = None

    _record_type_factory = record_type_validator()(set_record_type)

    class Config:
        from_attributes = True


# Properties to return to non-admin clients
class AccessKey(SavedAccessKey):
    pass


# Properties to return when an access key has just been created. This is the only response that carries the key.
class NewAccessKey(SavedAccessKey):
    key_id: str
    private_key: str
