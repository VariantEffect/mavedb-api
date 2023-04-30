from mavedb.view_models.base.base import BaseModel


class ApiVersion(BaseModel):
    name: str
    version: str
