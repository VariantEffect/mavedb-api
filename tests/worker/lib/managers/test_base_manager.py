# ruff: noqa: E402
import pytest

pytest.importorskip("arq")

from mavedb.worker.lib.managers.base_manager import BaseManager


@pytest.mark.integration
class TestInitialization:
    """Tests for BaseManager initialization."""

    def test_initialization(self, session, arq_redis):
        """Test that BaseManager initializes with db and redis attributes."""

        manager = BaseManager(db=session, redis=arq_redis)

        assert manager.db == session
        assert manager.redis == arq_redis
