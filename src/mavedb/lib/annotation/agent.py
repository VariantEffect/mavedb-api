import logging

from ga4gh.core.models import Extension
from ga4gh.va_spec.base.core import Agent

from mavedb import __version__
from mavedb.models.user import User

logger = logging.getLogger(__name__)


def mavedb_api_agent() -> Agent:
    """
    Create a [VA Agent](https://va-ga4gh.readthedocs.io/en/latest/core-information-model/entities/agent.html)
    object for the current MaveDB API version.
    """
    version_at_time_of_generation = Extension(
        name="mavedbApiVersion",
        value=__version__,
    )

    return Agent(
        name="MaveDB API",
        agentType="Software",
        description=f"MaveDB API agent, version {__version__}",
        extensions=[version_at_time_of_generation],
    )


def mavedb_vrs_agent(version: str) -> Agent:
    """
    Create a [VA Agent](https://va-ga4gh.readthedocs.io/en/latest/core-information-model/entities/agent.html)
    object for the passed MaveDB VRS mapping version.
    """
    version_at_time_of_variant_generation = Extension(
        name="mavedbVrsVersion",
        value=version,
    )

    return Agent(
        name="MaveDB VRS Mapping Agent",
        agentType="Software",
        description=f"MaveDB VRS mapping agent, version {version_at_time_of_variant_generation.value}",
        extensions=[version_at_time_of_variant_generation],
    )


def mavedb_user_agent(user: User) -> Agent:
    """
    Create a [VA Agent](https://va-ga4gh.readthedocs.io/en/latest/core-information-model/entities/agent.html)
    object for the passed MaveDB user.
    """
    return Agent(
        name=user.username,
        agentType="Person",
        description=f"MaveDB ORCid authenticated user {user.username}",
    )


# XXX: Ideally, this becomes versioned software.
def pillar_project_calibration_agent() -> Agent:
    """
    Create a [VA Agent](https://va-ga4gh.readthedocs.io/en/latest/core-information-model/entities/agent.html)
    object for the pillar project calibration software.
    """
    return Agent(
        name="Pillar Project Variant Calibrator",
        agentType="Software",
        # XXX - version?
        description="Pillar project variant calibrator, see https://github.com/Dzeiberg/mave_calibration",
    )
