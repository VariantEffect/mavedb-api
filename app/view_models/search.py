from pydantic.types import Optional

from app.view_models.base.base import BaseModel


class ScoresetsSearch(BaseModel):
    targets: Optional[list[str]]
    target_organism_names: Optional[list[str]]
    target_types: Optional[list[str]]
    text: Optional[str]


class TextSearch(BaseModel):
    text: Optional[str]
