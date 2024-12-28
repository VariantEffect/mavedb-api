from pydantic import model_validator


def record_type_validator():
    return model_validator(mode="after")


def set_record_type(cls, data):
    data.record_type = cls.__name__
    return data
