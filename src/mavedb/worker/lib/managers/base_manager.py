"""Base manager class providing common database transaction handling.

This module provides the BaseManager class that encapsulates common database
session management patterns used across all manager classes.
"""

import logging
from abc import ABC
from typing import Optional

from arq import ArqRedis
from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)


class BaseManager(ABC):
    """Base class for all manager classes providing common interface.

    Provides standardized pattern for initializing a manager with database
    and Redis connections.

    Features:
    - Common initialization pattern

    Attributes:
        db: SQLAlchemy database session for queries and transactions
        redis: ARQ Redis client for job queue operations
    """

    def __init__(self, db: Session, redis: Optional[ArqRedis]):
        """Initialize base manager with database and Redis connections.

        Args:
            db: SQLAlchemy database session for job and pipeline queries
            redis(Optional[ArqRedis]): ARQ Redis client for job queue operations

        Raises:
            DatabaseConnectionError: Cannot connect to database
        """
        self.db = db
        self.redis = redis
