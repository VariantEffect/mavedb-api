from humps import camelize

from pydantic import BaseModel as PydanticBaseModel
from pydantic import field_validator


class BaseModel(PydanticBaseModel):
    @field_validator("*", mode="before")
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
        alias_generator = camelize
        populate_by_name = True
