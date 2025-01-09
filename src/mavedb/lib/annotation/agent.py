import logging

from ga4gh.core.entity_models import Agent, Extension, AgentSubtype

from mavedb import __version__
from mavedb.models.user import User

logger = logging.getLogger(__name__)


def mavedb_api_agent():
    """
    Create a [VA Agent](https://va-ga4gh.readthedocs.io/en/latest/core-information-model/entities/agent.html)
    object for the current MaveDB API version.
    """
    version_at_time_of_generation = Extension(
        name="mavedbApiVersion",
        value=__version__,
    )

    return Agent(
        subtype=AgentSubtype.SOFTWARE,
        label="MaveDB API",
        description=f"MaveDB API agent, version {__version__}",
        extensions=[version_at_time_of_generation],
    )


def mavedb_vrs_agent(version: str):
    """
    Create a [VA Agent](https://va-ga4gh.readthedocs.io/en/latest/core-information-model/entities/agent.html)
    object for the passed MaveDB VRS mapping version.
    """
    version_at_time_of_variant_generation = Extension(
        name="mavedbVrsVersion",
        value=version,
    )

    return Agent(
        subtype=AgentSubtype.SOFTWARE,
        label="MaveDB VRS mapper",
        description=f"MaveDB VRS mapping agent, version {version_at_time_of_variant_generation.value}",
        extensions=[version_at_time_of_variant_generation],
    )


def mavedb_user_agent(user: User) -> Agent:
    """
    Create a [VA Agent](https://va-ga4gh.readthedocs.io/en/latest/core-information-model/entities/agent.html)
    object for the passed MaveDB user.
    """
    return Agent(
        id=user.username,
        subtype=AgentSubtype.PERSON,
        label="MaveDB ORCid authenticated user",
    )


# TODO: Ideally, this becomes versioned software.
def pillar_project_calibration_agent():
    """
    Create a [VA Agent](https://va-ga4gh.readthedocs.io/en/latest/core-information-model/entities/agent.html)
    object for the pillar project calibration software.
    """
    return Agent(
        subtype=AgentSubtype.SOFTWARE,
        label="Pillar project variant calibrator",
        reportedIn="https://github.com/Dzeiberg/mave_calibration",
    )
