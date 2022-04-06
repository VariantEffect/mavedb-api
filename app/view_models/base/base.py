from humps.camel import case
from pydantic import BaseModel as PydanticBaseModel


class BaseModel(PydanticBaseModel):
    class Config:
        alias_generator = case
        allow_population_by_field_name = True
