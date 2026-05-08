"""Cache configuration for ClinGen API requests.

This module provides centralized cache configuration for ClinGen API calls that works
from both worker and API contexts. The cache backend is configurable via environment
variables, enabling different backends for dev/test/prod environments.

The caching layer significantly reduces redundant API calls to ClinGen's Allele
Registry when refreshing ClinVar controls across multiple months/years. With a
24-hour TTL, subsequent jobs within the cache window experience 100% cache hit
rates, eliminating unnecessary API load.

Note: Configuration is evaluated at module import time (when decorators are applied).
For testing purposes, use get_cache_configuration() to retrieve config with different
environment variables.
"""

import logging
import os

from aiocache import Cache

logger = logging.getLogger(__name__)

# Cache constants
CACHE_KEY_PREFIX = "mavedb:clingen"
CACHE_KEY_VERSION = "v1"
CACHE_TTL_SECONDS = 86400  # 24 hours
# aiocache default is 5s, which times out under connection pool contention when
# concurrent annotation jobs all hit Redis simultaneously.
CACHE_TIMEOUT_SECONDS = 30


def get_cache_configuration(backend=None, redis_host=None, redis_port=None, redis_ssl=None):
    """Get cache configuration based on environment variables or provided parameters.

    This function is provided for testing purposes, allowing configuration to be
    retrieved with custom parameters. In production, module-level CACHE_CLASS and
    CACHE_CONFIG are used (evaluated at import time).

    Args:
        backend: Cache backend ('redis' or 'memory'). If None, reads from CLINGEN_CACHE_BACKEND env var.
        redis_host: Redis host. If None, reads from CLINGEN_REDIS_HOST env var.
        redis_port: Redis port. If None, reads from CLINGEN_REDIS_PORT env var.
        redis_ssl: Redis SSL enabled. If None, reads from CLINGEN_REDIS_SSL env var.

    Returns:
        tuple: (cache_class, cache_config_dict)

    Raises:
        ValueError: If backend is not 'redis' or 'memory'
    """
    cache_backend = backend or os.getenv("CLINGEN_CACHE_BACKEND", "redis")

    if cache_backend == "redis":
        host = redis_host or os.getenv("CLINGEN_REDIS_HOST", "redis")
        port = redis_port or int(os.getenv("CLINGEN_REDIS_PORT", "6379"))
        ssl = redis_ssl if redis_ssl is not None else os.getenv("CLINGEN_REDIS_SSL", "false").lower() == "true"

        cache_class = Cache.REDIS
        cache_config = {
            "endpoint": host,
            "port": port,
            "ssl": ssl,
            "namespace": CACHE_KEY_PREFIX,
            "timeout": CACHE_TIMEOUT_SECONDS,
        }
        return cache_class, cache_config

    elif cache_backend == "memory":
        cache_class = Cache.MEMORY
        cache_config = {
            "namespace": CACHE_KEY_PREFIX,
            "timeout": CACHE_TIMEOUT_SECONDS,
        }
        return cache_class, cache_config

    else:
        raise ValueError(f"Unsupported cache backend: {cache_backend}. Valid options are 'redis' or 'memory'.")


# Module-level configuration (evaluated at import time for decorator usage)
# The @cached decorators in allele_registry.py use these at function definition time
CACHE_CLASS, CACHE_CONFIG = get_cache_configuration()

# Log the configuration that was selected
backend_name = "memory" if CACHE_CLASS == Cache.MEMORY else CACHE_CONFIG.get("endpoint") or "unknown"
logger.info(f"ClinGen cache initialized: backend={backend_name}, TTL={CACHE_TTL_SECONDS}s, prefix={CACHE_KEY_PREFIX}")


def clingen_cache_key_builder(func, *args, **kwargs):
    """Build cache key for ClinGen API functions.

    The key includes a version prefix to enable cache invalidation if the
    response format changes in the future. Different ClinGen API functions
    (get_canonical_pa_ids, get_matching_registered_ca_ids, get_associated_clinvar_allele_id)
    are cached separately as they return different data for the same allele ID.

    Cache key format: v1:{function_name}:{allele_id}
    The namespace prefix (mavedb:clingen) is added by aiocache automatically.

    Full Redis key example: mavedb:clingen:v1:get_associated_clinvar_allele_id:CA123456

    Args:
        func: The decorated function being cached
        *args: Positional arguments (first arg is always the allele_id for ClinGen functions)
        **kwargs: Keyword arguments (may contain clingen_allele_id or clingen_pa_id)

    Returns:
        Cache key string in format: v1:{function_name}:{allele_id}
    """
    function_name = func.__name__

    # First positional arg is always the allele ID for ClinGen API functions
    # Fallback to kwargs for flexibility (though not currently used)
    allele_id = args[0] if args else kwargs.get("clingen_allele_id") or kwargs.get("clingen_pa_id")

    if not allele_id:
        raise ValueError(f"Cannot build cache key for {function_name}: allele_id is required")

    return f"{CACHE_KEY_VERSION}:{function_name}:{allele_id}"
