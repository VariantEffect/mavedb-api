from typing import Any, Dict, Union

from sqlalchemy.orm import as_declarative, declared_attr

class_registry: Dict = {}


@as_declarative(class_registry=class_registry)
class Base:
    id: Any
    __name__: str

    # Generate __tablename__ automatically
    # Declared in this odd way to provide correct type hint for mypy
    __tablename__: Union[declared_attr[Any], str] = declared_attr(lambda cls: cls.__name__.lower())
