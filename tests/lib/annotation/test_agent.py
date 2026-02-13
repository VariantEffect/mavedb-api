from unittest.mock import Mock

import pytest

from mavedb import __version__
from mavedb.lib.annotation.agent import mavedb_api_agent, mavedb_user_agent, mavedb_vrs_agent
from mavedb.models.user import User


@pytest.mark.unit
class TestMavedbApiAgentUnit:
    """Unit tests for mavedb_api_agent factory function."""

    def test_creates_correct_structure(self):
        """Test that API agent has correct properties and structure."""
        agent = mavedb_api_agent()

        assert agent.name == "MaveDB API"
        assert agent.agentType == "Software"
        assert agent.description == f"MaveDB API agent, version {__version__}"
        assert len(agent.extensions) == 1
        assert agent.extensions[0].name == "mavedbApiVersion"
        assert agent.extensions[0].value == __version__

    def test_uses_current_version(self):
        """Test that agent uses the current package version."""
        agent = mavedb_api_agent()

        # Ensure it's using the actual package version
        assert agent.extensions[0].value == __version__
        assert __version__ in agent.description

    def test_returns_agent_type(self):
        """Test that function returns proper Agent object."""
        from ga4gh.va_spec.base.core import Agent

        agent = mavedb_api_agent()
        assert isinstance(agent, Agent)


@pytest.mark.unit
class TestMavedbVrsAgentUnit:
    """Unit tests for mavedb_vrs_agent factory function."""

    @pytest.mark.parametrize("version", ["1.0.0", "2.1.3-beta", "latest", "dev-branch-123"])
    def test_creates_agent_with_various_versions(self, version):
        """Test VRS agent creation with different version strings."""
        agent = mavedb_vrs_agent(version)

        assert agent.name == "MaveDB VRS Mapping Agent"
        assert agent.agentType == "Software"
        assert agent.description == f"MaveDB VRS mapping agent, version {version}"
        assert len(agent.extensions) == 1
        assert agent.extensions[0].name == "mavedbVrsVersion"
        assert agent.extensions[0].value == version

    def test_handles_empty_version(self):
        """Test VRS agent handles empty version string."""
        agent = mavedb_vrs_agent("")

        assert agent.extensions[0].value == ""
        assert "version " in agent.description

    def test_raises_on_none_version(self):
        """Test VRS agent handles None version."""
        with pytest.raises(ValueError):
            mavedb_vrs_agent(None)

    def test_returns_agent_type(self):
        """Test that function returns proper Agent object."""
        from ga4gh.va_spec.base.core import Agent

        agent = mavedb_vrs_agent("1.0.0")
        assert isinstance(agent, Agent)


@pytest.mark.unit
class TestMavedbUserAgentUnit:
    """Unit tests for mavedb_user_agent factory function."""

    def test_creates_agent_from_user_object(self):
        """Test user agent creation from User object."""
        mock_user = Mock(spec=User)
        mock_user.username = "test-orcid-0000-0000-0000-0001"

        agent = mavedb_user_agent(mock_user)

        assert agent.name == "test-orcid-0000-0000-0000-0001"
        assert agent.agentType == "Person"
        assert agent.description == "MaveDB ORCid authenticated user test-orcid-0000-0000-0000-0001"
        # User agent has no extensions by design
        assert agent.extensions is None or len(agent.extensions) == 0

    @pytest.mark.parametrize(
        "username",
        [
            "short",
            "very-long-username-with-dashes-and-numbers-123",
            "user@domain.com",
            "0000-0000-0000-0000",  # ORCID format
        ],
    )
    def test_handles_various_username_formats(self, username):
        """Test user agent with different username formats."""
        mock_user = Mock(spec=User)
        mock_user.username = username

        agent = mavedb_user_agent(mock_user)

        assert agent.name == username
        assert username in agent.description

    def test_handles_empty_username(self):
        """Test user agent handles empty username."""
        mock_user = Mock(spec=User)
        mock_user.username = ""

        agent = mavedb_user_agent(mock_user)

        assert agent.name == ""
        assert "user " in agent.description

    def test_raises_on_none_user(self):
        """Test user agent handles None user."""
        with pytest.raises(AttributeError):
            mavedb_user_agent(None)

    def test_returns_agent_type(self):
        """Test that function returns proper Agent object."""
        from ga4gh.va_spec.base.core import Agent

        mock_user = Mock(spec=User, username="test")
        agent = mavedb_user_agent(mock_user)
        assert isinstance(agent, Agent)
