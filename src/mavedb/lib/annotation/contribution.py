import logging

from ga4gh.core.models import Extension
from ga4gh.va_spec.base.core import Contribution

from mavedb.lib.annotation.agent import (
    mavedb_api_agent,
    mavedb_user_agent,
    mavedb_vrs_agent,
)
from mavedb.lib.annotation.context import VariantAnnotationContext
from mavedb.lib.types.annotation import ResourceWithCreationModificationDates
from mavedb.models.score_calibration import ScoreCalibration
from mavedb.models.user import User

logger = logging.getLogger(__name__)


def mavedb_api_contribution() -> Contribution:
    """
    Create a [VA Contribution](https://va-ga4gh.readthedocs.io/en/latest/core-information-model/entities/activities/contribution.html#contribution)
    object for an arbitary contribution from the MaveDB API/software distribution.

    Carries no ``date``. What this contribution asserts is *which software* produced the annotation, and
    that is the agent's version — see :func:`mavedb_api_agent`, which stamps ``__version__``. The only
    date available at this point is the instant of serialization, which is not provenance: it describes
    when a view was rendered, not when anything was contributed. The sibling builders in this module all
    carry a real stored date instead.

    Stamping wall-clock time here also made the object unique per call, so identical provenance never
    deduplicated and ``va.ndjson`` changed bytes on every run of unchanged data — defeating checksum
    manifests and incremental sync for consumers of the public dump. When an artifact needs a generation
    time it belongs on that artifact's metadata, once, not on every nested object inside it.
    """
    return Contribution(
        name="MaveDB API",
        description="Contribution from the MaveDB API",
        contributor=mavedb_api_agent(),
        activityType="software application programming interface",
    )


def mavedb_vrs_contribution(context: VariantAnnotationContext) -> Contribution:
    """
    Create a [VA Contribution](https://va-ga4gh.readthedocs.io/en/latest/core-information-model/entities/activities/contribution.html#contribution)
    object from the variant's live mapping record (the VRS mapper's provenance).
    """
    return Contribution(
        name="MaveDB VRS Mapper",
        description="Contribution from the MaveDB VRS mapping software",
        # Both guaranteed non-null via DB constraints on MappingRecord.
        contributor=mavedb_vrs_agent(context.record.mapping_api_version),  # type: ignore
        date=context.record.mapped_date,  # type: ignore
        activityType="human genome sequence mapping process",
    )


def mavedb_score_calibration_contribution(score_calibration: ScoreCalibration) -> Contribution:
    """
    Create a [VA Contribution](https://va-ga4gh.readthedocs.io/en/latest/core-information-model/entities/activities/contribution.html#contribution)
    object from the provided score calibration.
    """
    return Contribution(
        id=score_calibration.urn,
        name=score_calibration.title,
        description="Contribution from a score calibration.",
        contributor=mavedb_user_agent(score_calibration.created_by),
        date=score_calibration.creation_date,  # type: ignore
        activityType="variant specific calibration",
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
        date=created_resource.creation_date,  # type: ignore
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
        date=modified_resource.modification_date,  # type: ignore
        activityType="http://purl.obolibrary.org/obo/CRO_0000103",
        extensions=[Extension(name="resourceType", value=modified_resource.__class__.__name__)],
    )
