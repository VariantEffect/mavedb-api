import logging
from datetime import datetime

from ga4gh.core.models import Extension
from ga4gh.va_spec.base.core import Contribution

from mavedb.models.mapped_variant import MappedVariant
from mavedb.models.user import User
from mavedb.lib.annotation.agent import (
    mavedb_api_agent,
    mavedb_vrs_agent,
    mavedb_user_agent,
    pillar_project_calibration_agent,
)
from mavedb.lib.types.annotation import ResourceWithCreationModificationDates

logger = logging.getLogger(__name__)


def mavedb_api_contribution() -> Contribution:
    """
    Create a [VA Contribution](https://va-ga4gh.readthedocs.io/en/latest/core-information-model/entities/activities/contribution.html#contribution)
    object for an arbitary contribution from the MaveDB API/software distribution.
    """
    return Contribution(
        name="MaveDB API",
        description="Contribution from the MaveDB API",
        contributor=mavedb_api_agent(),
        date=datetime.today().strftime("%Y-%m-%d"),
        activityType="software application programming interface",
    )


def mavedb_vrs_contribution(mapped_variant: MappedVariant) -> Contribution:
    """
    Create a [VA Contribution](https://va-ga4gh.readthedocs.io/en/latest/core-information-model/entities/activities/contribution.html#contribution)
    object from the provided mapped variant.
    """
    return Contribution(
        name="MaveDB VRS Mapper",
        description="Contribution from the MaveDB VRS mapping software",
        # Guaranteed to be a str via DB constraints.
        contributor=mavedb_vrs_agent(mapped_variant.mapping_api_version),  # type: ignore
        date=datetime.strftime(mapped_variant.mapped_date, "%Y-%m-%d"),  # type: ignore
        activityType="human genome sequence mapping process",
    )


def pillar_project_calibration_contribution() -> Contribution:
    """
    Create a [VA Contribution](https://va-ga4gh.readthedocs.io/en/latest/core-information-model/entities/activities/contribution.html#contribution)
    object for a sofware agent which performs calibrations on an arbitrary data set.
    """
    return Contribution(
        name="MaveDB Pillar Project Calibration",
        description="Contribution from the MaveDB Pillar Project Calibration software",
        contributor=pillar_project_calibration_agent(),
        activityType="variant specific calibration software",
    )


def mavedb_creator_contribution(created_resource: ResourceWithCreationModificationDates, creator: User) -> Contribution:
    """
    Create a [VA Contribution](https://va-ga4gh.readthedocs.io/en/latest/core-information-model/entities/activities/contribution.html#contribution)
    object from the provided createable resource (a resource with both a creation date and creator).
    """
    return Contribution(
        name="MaveDB Dataset Creator",
        description="When this resource was first submitted, and by whom.",
        contributor=mavedb_user_agent(creator),
        # Guaranteed to be a str via DB constraints.
        date=datetime.strftime(created_resource.creation_date, "%Y-%m-%d"),  # type: ignore
        activityType="http://purl.obolibrary.org/obo/CRO_0000105",
        extensions=[Extension(name="resourceType", value=created_resource.__class__.__name__)],
    )


def mavedb_modifier_contribution(
    modified_resource: ResourceWithCreationModificationDates, modifier: User
) -> Contribution:
    """
    Create a [VA Contribution](https://va-ga4gh.readthedocs.io/en/latest/core-information-model/entities/activities/contribution.html#contribution)
    object from the provided modifiable resource (a resource with both a modification date and modifier).
    """
    return Contribution(
        name="MaveDB Dataset Modifier",
        description="When this resource was last modified, and by whom.",
        contributor=mavedb_user_agent(modifier),
        # Guaranteed to be a str via DB constraints.
        date=datetime.strftime(modified_resource.modification_date, "%Y-%m-%d"),  # type: ignore
        activityType="http://purl.obolibrary.org/obo/CRO_0000103",
        extensions=[Extension(name="resourceType", value=modified_resource.__class__.__name__)],
    )


# TODO: Although we would ideally provide a contribution object for the publisher of the data set, we don't
#       save which user actually published it. We could proxy this by just using the creator, but this is
#       not always strictly accurate.
#
# ResourceWithPublicationDate = Union[ExperimentSet, Experiment, ScoreSet]
# def mavedb_publisher_contribution(published_resource: ResourceWithCreatorModifier, publisher: User) -> Contribution:
#     return Contribution(
#         contributor=[mavedb_user_agent(publisher)],
#         date=datetime.strftime(published_resource.publication_date, "%Y-%m-%d"),
#         label="Resource First Published",
#         activityType=Coding(label="author role", system="http://purl.obolibrary.org/obo/cro.owl", code="CRO_0000001", systemVersion="v2019-08-16"),
#         extensions=[Extension(name="resourceType", value=published_resource.__class__)],
#     )
