"""Environment configuration constants for worker settings.

This module centralizes all environment variable handling for the worker,
providing sensible defaults and type conversion for configuration values.
All worker-related environment variables should be defined here.
"""

import os

REDIS_IP = os.getenv("REDIS_IP") or "localhost"
REDIS_PORT = int(os.getenv("REDIS_PORT") or 6379)
REDIS_SSL = (os.getenv("REDIS_SSL") or "false").lower() == "true"
