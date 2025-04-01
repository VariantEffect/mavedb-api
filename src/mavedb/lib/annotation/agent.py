import logging

from ga4gh.core.models import Extension, MappableConcept
from ga4gh.va_spec.base.core import Agent

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
        subtype=MappableConcept(name="Software"),  # TODO
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
        subtype=MappableConcept(name="Software"),  # TODO
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
        subtype=MappableConcept(name="Person"),  # TODO
        description=f"MaveDB ORCid authenticated user {user.username}",
    )


# XXX: Ideally, this becomes versioned software.
def pillar_project_calibration_agent():
    """
    Create a [VA Agent](https://va-ga4gh.readthedocs.io/en/latest/core-information-model/entities/agent.html)
    object for the pillar project calibration software.
    """
    return Agent(
        subtype=MappableConcept(name="Software"),
        # XXX - version?
        description="Pillar project variant calibrator, see https://github.com/Dzeiberg/mave_calibration",
    )
