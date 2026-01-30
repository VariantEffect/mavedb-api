from unittest import mock

from mavedb import __version__
from mavedb.lib.annotation.agent import (
    excalibr_calibration_agent,
    mavedb_api_agent,
    mavedb_user_agent,
    mavedb_vrs_agent,
)
from mavedb.models.user import User
from tests.helpers.constants import TEST_USER


def test_mavedb_api_agent():
    agent = mavedb_api_agent()

    assert agent.name == "MaveDB API"
    assert agent.agentType == "Software"
    assert agent.description == f"MaveDB API agent, version {__version__}"
    assert len(agent.extensions) == 1
    assert agent.extensions[0].name == "mavedbApiVersion"
    assert agent.extensions[0].value == __version__


def test_mavedb_vrs_agent():
    version = "test.1.0"
    agent = mavedb_vrs_agent(version)

    assert agent.name == "MaveDB VRS Mapping Agent"
    assert agent.agentType == "Software"
    assert agent.description == f"MaveDB VRS mapping agent, version {version}"
    assert len(agent.extensions) == 1
    assert agent.extensions[0].name == "mavedbVrsVersion"
    assert agent.extensions[0].value == version


def test_mavedb_user_agent():
    user = mock.Mock(spec=User)
    user.username = TEST_USER["username"]
    agent = mavedb_user_agent(user)

    assert agent.name == TEST_USER["username"]
    assert agent.description == f"MaveDB ORCid authenticated user {TEST_USER['username']}"
    assert agent.agentType == "Person"


def test_excalibr_calibration_agent():
    agent = excalibr_calibration_agent()

    assert agent.name == "ExCALIBR Variant Calibrator"
    assert agent.description == "ExCALIBR variant calibrator, see https://github.com/Dzeiberg/mave_calibration"
    assert agent.agentType == "Software"
