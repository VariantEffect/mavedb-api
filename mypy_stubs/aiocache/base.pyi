"""Type stubs for aiocache.base module.

Provides type hints for the base cache class used by aiocache backends.
"""

from typing import Any, Optional

class BaseCache:
    """Base class for cache backends."""

    def __init__(
        self,
        *,
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

__all__ = ["BaseCache"]
