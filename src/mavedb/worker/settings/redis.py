"""Redis connection settings for ARQ worker.

This module provides Redis connection configuration using environment
variables with appropriate defaults. The settings are compatible with
ARQ's RedisSettings class and handle SSL connections.
"""

from arq.connections import RedisSettings

from mavedb.worker.settings.constants import REDIS_IP, REDIS_PORT, REDIS_SSL

RedisWorkerSettings = RedisSettings(host=REDIS_IP, port=REDIS_PORT, ssl=REDIS_SSL)
