from copy import deepcopy
from typing import Any, Callable, Optional, Type, TypeVar

from pydantic import create_model
from pydantic.fields import FieldInfo

from mavedb.view_models.base.base import BaseModel

Model = TypeVar("Model", bound=BaseModel)


def all_fields_optional_model() -> Callable[[Type[Model]], Type[Model]]:
    """A decorator that create a partial model.

    Args:
        model (Type[BaseModel]): BaseModel model.

    Returns:
        Type[BaseModel]: ModelBase partial model.
    """

    def wrapper(model: Type[Model]) -> Type[Model]:
        def make_field_optional(field: FieldInfo, default: Any = None) -> tuple[Any, FieldInfo]:
            new = deepcopy(field)
            new.default = default
            new.annotation = Optional[field.annotation]  # type: ignore[assignment]
            return new.annotation, new

        return create_model(
            model.__name__,
            __base__=model,
            __module__=model.__module__,
            **{field_name: make_field_optional(field_info) for field_name, field_info in model.model_fields.items()},
        )  # type: ignore[call-overload]

    return wrapper
