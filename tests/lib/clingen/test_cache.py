# ruff: noqa: E402
"""Tests for ClinGen cache configuration."""

import pytest

pytest.importorskip("aiocache", reason="aiocache is required to test caching behavior of ClinGen API functions")

import inspect

from aiocache import Cache

from mavedb.lib.clingen.allele_registry import (
    get_associated_clinvar_allele_id,
    get_canonical_pa_ids,
    get_matching_registered_ca_ids,
)
from mavedb.lib.clingen.cache import (
    CACHE_CLASS,
    CACHE_CONFIG,
    CACHE_KEY_PREFIX,
    CACHE_KEY_VERSION,
    CACHE_TTL_SECONDS,
    clingen_cache_key_builder,
    get_cache_configuration,
)


@pytest.mark.unit
class TestCacheConfiguration:
    """Test cache configuration constants and key builder."""

    def test_cache_constants(self):
        """Verify cache constants are properly defined."""
        assert CACHE_KEY_PREFIX == "mavedb:clingen"
        assert CACHE_KEY_VERSION == "v1"
        assert CACHE_TTL_SECONDS == 86400  # 24 hours

    def test_cache_key_builder_with_positional_arg(self):
        """Verify cache key builder generates correct keys with positional args."""

        def mock_func():
            pass

        mock_func.__name__ = "get_associated_clinvar_allele_id"

        key = clingen_cache_key_builder(mock_func, "CA123456")
        assert key == "v1:get_associated_clinvar_allele_id:CA123456"

    def test_cache_key_builder_with_kwargs(self):
        """Verify cache key builder generates correct keys with kwargs."""

        def mock_func():
            pass

        mock_func.__name__ = "get_canonical_pa_ids"

        # Test with clingen_allele_id kwarg
        key = clingen_cache_key_builder(mock_func, clingen_allele_id="CA654321")
        assert key == "v1:get_canonical_pa_ids:CA654321"

        # Test with clingen_pa_id kwarg
        mock_func.__name__ = "get_matching_registered_ca_ids"
        key = clingen_cache_key_builder(mock_func, clingen_pa_id="PA987654")
        assert key == "v1:get_matching_registered_ca_ids:PA987654"

    def test_cache_key_builder_includes_function_name(self):
        """Verify cache keys are isolated by function name."""

        def func1():
            pass

        def func2():
            pass

        func1.__name__ = "get_canonical_pa_ids"
        func2.__name__ = "get_associated_clinvar_allele_id"

        key1 = clingen_cache_key_builder(func1, "CA123")
        key2 = clingen_cache_key_builder(func2, "CA123")

        # Same allele ID, different functions = different cache keys
        assert key1 == "v1:get_canonical_pa_ids:CA123"
        assert key2 == "v1:get_associated_clinvar_allele_id:CA123"
        assert key1 != key2

    def test_cache_key_builder_raises_on_missing_id(self):
        """Verify cache key builder raises error when allele_id is missing."""

        def mock_func():
            pass

        mock_func.__name__ = "test_function"

        with pytest.raises(ValueError, match="allele_id is required"):
            clingen_cache_key_builder(mock_func)

    def test_functions_are_async_with_cached_decorator(self):
        """Verify all ClinGen API functions are async (required for aiocache)."""
        assert inspect.iscoroutinefunction(get_canonical_pa_ids)
        assert inspect.iscoroutinefunction(get_matching_registered_ca_ids)
        assert inspect.iscoroutinefunction(get_associated_clinvar_allele_id)


@pytest.mark.unit
class TestCacheBackendConfiguration:
    """Test cache backend configuration logic."""

    def test_get_cache_configuration_redis_backend(self):
        """Verify get_cache_configuration returns correct Redis config."""
        cache_class, cache_config = get_cache_configuration(
            backend="redis", redis_host="test-host", redis_port=1234, redis_ssl=True
        )

        assert cache_class == Cache.REDIS
        assert cache_config["endpoint"] == "test-host"
        assert cache_config["port"] == 1234
        assert cache_config["ssl"] is True
        assert cache_config["namespace"] == CACHE_KEY_PREFIX

    def test_get_cache_configuration_memory_backend(self):
        """Verify get_cache_configuration returns correct memory config."""
        cache_class, cache_config = get_cache_configuration(backend="memory")

        assert cache_class == Cache.MEMORY
        assert cache_config["namespace"] == CACHE_KEY_PREFIX
        # Memory backend should not have Redis-specific config
        assert "endpoint" not in cache_config
        assert "port" not in cache_config
        assert "ssl" not in cache_config

    def test_get_cache_configuration_invalid_backend(self):
        """Verify get_cache_configuration raises error for invalid backend."""
        with pytest.raises(ValueError, match="Unsupported cache backend: invalid"):
            get_cache_configuration(backend="invalid")

    def test_get_cache_configuration_defaults_from_env(self, monkeypatch):
        """Verify get_cache_configuration reads from environment variables."""
        monkeypatch.setenv("CLINGEN_CACHE_BACKEND", "memory")

        cache_class, cache_config = get_cache_configuration()

        assert cache_class == Cache.MEMORY

    def test_get_cache_configuration_redis_defaults(self):
        """Verify get_cache_configuration uses correct defaults for Redis."""
        cache_class, cache_config = get_cache_configuration(backend="redis")

        assert cache_class == Cache.REDIS
        assert cache_config["endpoint"] == "redis"
        assert cache_config["port"] == 6379
        assert cache_config["ssl"] is False

    def test_get_cache_configuration_redis_ssl_parsing(self):
        """Verify SSL boolean is parsed correctly from string."""
        # Test True
        _, config_true = get_cache_configuration(backend="redis", redis_ssl=True)
        assert config_true["ssl"] is True

        # Test False
        _, config_false = get_cache_configuration(backend="redis", redis_ssl=False)
        assert config_false["ssl"] is False

    def test_module_level_cache_config_initialized(self):
        """Verify module-level CACHE_CLASS and CACHE_CONFIG are initialized."""
        # Should be initialized (either Redis or Memory depending on env)
        assert CACHE_CLASS is not None
        assert CACHE_CONFIG is not None
        assert isinstance(CACHE_CONFIG, dict)
        assert "namespace" in CACHE_CONFIG

    def test_cache_backend_is_memory_in_tests(self):
        """Verify cache backend is configured to use memory in test environment."""
        # In test environment, CLINGEN_CACHE_BACKEND env var is set to "memory" in tests/conftest.py
        assert CACHE_CLASS == Cache.MEMORY
        assert CACHE_CONFIG["namespace"] == CACHE_KEY_PREFIX
        # Memory backend should not have Redis-specific config
        assert "endpoint" not in CACHE_CONFIG
        assert "port" not in CACHE_CONFIG
        assert "ssl" not in CACHE_CONFIG
