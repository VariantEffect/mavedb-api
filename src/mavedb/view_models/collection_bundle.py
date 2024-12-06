from mavedb.view_models.base.base import BaseModel
from mavedb.view_models.collection import Collection


class CollectionBundle(BaseModel):
    admin: list[Collection]
    editor: list[Collection]
    viewer: list[Collection]
