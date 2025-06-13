import logging
from datetime import datetime
from typing import Union

from ga4gh.core.models import Coding, Extension, MappableConcept
from ga4gh.va_spec.base.core import Contribution

from mavedb.models.experiment_set import ExperimentSet
from mavedb.models.experiment import Experiment
from mavedb.models.score_set import ScoreSet
from mavedb.models.mapped_variant import MappedVariant
from mavedb.models.variant import Variant
from mavedb.models.user import User
from mavedb.lib.annotation.agent import (
    mavedb_api_agent,
    mavedb_vrs_agent,
    mavedb_user_agent,
    pillar_project_calibration_agent,
)

logger = logging.getLogger(__name__)

# Non-exhaustive
ResourceWithCreationModificationDates = Union[ExperimentSet, Experiment, ScoreSet, MappedVariant, Variant]


def mavedb_api_contribution() -> Contribution:
    """
    Create a [VA Contribution](https://va-ga4gh.readthedocs.io/en/latest/core-information-model/entities/activities/contribution.html#contribution)
    object for an arbitary contribution from the MaveDB API/software distribution.
    """
    return Contribution(
        name="mavedb-api",
        description="Contribution from the MaveDB API",
        contributor=mavedb_api_agent(),
        date=datetime.today().strftime("%Y-%m-%d"),
        activityType=MappableConcept(
            name="placeholder",
            conceptType="placeholder",
            primaryCoding=Coding(
                name="application programming interface",
                system="http://purl.obolibrary.org/obo/swo.owl",
                systemVersion="2023-03-05",
                code="SWO_9000054",
            ),
        ),
    )


def mavedb_vrs_contribution(mapped_variant: MappedVariant) -> Contribution:
    """
    Create a [VA Contribution](https://va-ga4gh.readthedocs.io/en/latest/core-information-model/entities/activities/contribution.html#contribution)
    object from the provided mapped variant.
    """
    return Contribution(
        name="mavedb-vrs",
        description="Contribution from the MaveDB VRS mapping software",
        # Guaranteed to be a str via DB constraints.
        contributor=mavedb_vrs_agent(mapped_variant.mapping_api_version),  # type: ignore
        date=datetime.strftime(mapped_variant.mapped_date, "%Y-%m-%d"),  # type: ignore
        activityType=MappableConcept(
            name="placeholder",
            conceptType="placeholder",
            primaryCoding=Coding(
                name="planned process",
                system="http://purl.obolibrary.org/obo/swo.owl",
                systemVersion="2023-03-05",
                code="OBI_0000011",
            ),
        ),
    )


def pillar_project_calibration_contribution() -> Contribution:
    """
    Create a [VA Contribution](https://va-ga4gh.readthedocs.io/en/latest/core-information-model/entities/activities/contribution.html#contribution)
    object for a sofware agent which performs calibrations on an arbitrary data set.
    """
    return Contribution(
        name="pillar-project-calibration",
        description="Contribution from the pillar project calibration software",
        contributor=pillar_project_calibration_agent(),
        activityType=MappableConcept(
            name="placeholder",
            conceptType="placeholder",
            primaryCoding=Coding(
                name="planned process",
                system="http://purl.obolibrary.org/obo/swo.owl",
                systemVersion="2023-03-05",
                code="OBI_0000011",
            ),
        ),
    )


def mavedb_creator_contribution(created_resource: ResourceWithCreationModificationDates, creator: User) -> Contribution:
    """
    Create a [VA Contribution](https://va-ga4gh.readthedocs.io/en/latest/core-information-model/entities/activities/contribution.html#contribution)
    object from the provided createable resource (a resource with both a creation date and creator).
    """
    return Contribution(
        name="mavedb-creator",
        description="When this resource was first submitted, and by whom.",
        contributor=mavedb_user_agent(creator),
        # Guaranteed to be a str via DB constraints.
        date=datetime.strftime(created_resource.creation_date, "%Y-%m-%d"),  # type: ignore
        activityType=MappableConcept(
            name="placeholder",
            conceptType="placeholder",
            primaryCoding=Coding(
                name="submitter role",
                system="http://purl.obolibrary.org/obo/cro.owl",
                code="CRO_0000105",
                systemVersion="v2019-08-16",
            ),
        ),
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
        name="mavedb-modifier",
        description="When this resource was last modified, and by whom.",
        contributor=mavedb_user_agent(modifier),
        # Guaranteed to be a str via DB constraints.
        date=datetime.strftime(modified_resource.modification_date, "%Y-%m-%d"),  # type: ignore
        activityType=MappableConcept(
            name="placeholder",
            conceptType="placeholder",
            primaryCoding=Coding(
                name="modifier role",
                system="http://purl.obolibrary.org/obo/cro.owl",
                code="CRO_0000103",
                systemVersion="v2019-08-16",
            ),
        ),
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
