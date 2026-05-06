"""Type stubs for aiocache library.

Provides type hints for the aiocache caching library functionality used in MaveDB.
"""

from typing import Any, Awaitable, Callable, Optional, Type, TypeVar, Union

from .base import BaseCache

# Type variables for decorator
F = TypeVar("F", bound=Callable[..., Awaitable[Any]])
T = TypeVar("T")

class Cache:
    """Cache factory class for creating cache instances."""

    # Cache backend constants
    REDIS: Type[BaseCache]
    MEMORY: Type[BaseCache]

    def __init__(
        self,
        cache_class: Type[BaseCache],
        *,
        endpoint: Optional[str] = None,
        port: Optional[int] = None,
        ssl: bool = False,
        namespace: Optional[str] = None,
        serializer: Optional[Any] = None,
        plugins: Optional[Any] = None,
        **kwargs: Any,
    ) -> None: ...
    async def get(self, key: str) -> Any: ...
    async def set(self, key: str, value: Any, ttl: Optional[int] = None) -> bool: ...
    async def delete(self, key: str) -> bool: ...
    async def clear(self, namespace: Optional[str] = None) -> bool: ...
    async def close(self) -> None: ...

def cached(
    ttl: Optional[int] = None,
    key: Optional[str] = None,
    key_builder: Optional[Callable[..., str]] = None,
    cache: Union[Type[BaseCache], BaseCache, None] = None,
    serializer: Optional[Any] = None,
    plugins: Optional[Any] = None,
    alias: Optional[str] = None,
    namespace: Optional[str] = None,
    noself: bool = False,
    skip_cache_func: Optional[Callable[[Any], bool]] = None,
    **kwargs: Any,
) -> Callable[[F], F]: ...

__all__ = ["Cache", "cached"]
