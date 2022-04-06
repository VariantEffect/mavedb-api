from pydantic.types import Optional

from .base.base import BaseModel


class Search(BaseModel):
    targets: Optional[list[str]]
    target_organism_names: Optional[list[str]]
    target_types: Optional[list[str]]
    text: Optional[str]
