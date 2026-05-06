import pytest_asyncio
from aiocache import Cache

from mavedb.lib.clingen.cache import CACHE_CLASS, CACHE_CONFIG


@pytest_asyncio.fixture
async def clear_cache():
    """Clear the aiocache cache before and after each test.

    This ensures test isolation when testing caching behavior for ClinGen API calls.
    Uses the module-level cache configuration which is set to memory backend via
    environment variable in tests/conftest.py.

    Note: ClinVar TSV files use file-based caching, not aiocache, so they are not
    affected by this fixture. ClinVar tests should use tmp_path fixture instead.
    """
    cache = Cache(CACHE_CLASS, **CACHE_CONFIG)
    await cache.clear()

    yield

    await cache.clear()
    await cache.close()
