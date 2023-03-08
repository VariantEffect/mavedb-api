from pydantic.types import Optional

from mavedb.view_models.base.base import BaseModel


class ExperimentsSearch(BaseModel):
    published: Optional[bool]
    text: Optional[str]


class ScoresetsSearch(BaseModel):
    published: Optional[bool]
    targets: Optional[list[str]]
    target_organism_names: Optional[list[str]]
    target_types: Optional[list[str]]
    text: Optional[str]


class TextSearch(BaseModel):
    text: Optional[str]
