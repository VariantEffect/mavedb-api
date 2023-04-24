from humps.camel import case
from pydantic import BaseModel as PydanticBaseModel, validator


class BaseModel(PydanticBaseModel):
    @validator("*", pre=True)
    def empty_str_to_none(cls, x):
        """
        Convert empty strings to None. This is applied to all string-valued attributes before other validators run.

        :param x: The attribute value
        :return: None if x was the empty string, otherwise x
        """
        if x == "":
            return None
        return x

    class Config:
        alias_generator = case
        allow_population_by_field_name = True
