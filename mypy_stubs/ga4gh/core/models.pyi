from enum import Enum

from pydantic import BaseModel, RootModel
from typing import Annotated, Any

class Relation(str, Enum):
    CLOSE_MATCH = "closeMatch"
    EXACT_MATCH = "exactMatch"
    BROAD_MATCH = "broadMatch"
    NARROW_MATCH = "narrowMatch"
    RELATED_MATCH = "relatedMatch"

class Element(BaseModel):
    id: str | None = None
    extensions: list[Extension] | None = None

class Code(RootModel):
    root: Annotated[str, None]

class Coding(BaseModel):
    label: str | None = None
    system: str
    systemVersion: str | None = None
    code: Code

class Extension(Element):
    name: str
    value: float | str | bool | dict[str, Any] | list[Any] | None = None
    description: str | None = None

class MappableConcept(BaseModel):
    name: str | None = None
    conceptType: str | None = None
    primaryCoding: Coding | None = None
    mappings: list[ConceptMapping] | None = None

    def require_name_or_primary_coding(cls, v): ...

class Entity(BaseModel):
    id: str | None = None
    type: str
    name: str | None = None
    description: str | None = None
    aliases: list[str] | None = None
    extensions: list[Extension] | None = None

class iriReference(RootModel):  # noqa: N801
    def __hash__(self) -> int: ...
    def ga4gh_serialize(self) -> str: ...

    root: str

class ConceptMapping(Element):
    coding: Coding
    relation: Relation
