from pydantic import BaseModel, RootModel
from typing import Annotated, Any

class Code(RootModel):
    root: Annotated[str, None]

class Coding(BaseModel):
    label: str | None = None
    system: str
    systemVersion: str | None = None
    code: Code

class Extension(BaseModel):
    name: str
    value: float | str | bool | dict[str, Any] | list[Any] | None = None
    description: str | None = None

class MappableConcept(BaseModel):
    name: str | None = None
    conceptType: str | None = None
    primaryCoding: Coding
    alternativeCoding: list[Coding] | None = None
    description: str | None = None
    extensions: list[Extension] | None = None
