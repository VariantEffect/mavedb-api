from pydantic import model_validator


def record_type_validator():
    return model_validator(mode="after")


def set_record_type(cls, data):
    if data is None:
        return None

    data.record_type = cls.__name__
    return data
