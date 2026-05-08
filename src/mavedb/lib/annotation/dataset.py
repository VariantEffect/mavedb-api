import logging
from typing import Optional

from ga4gh.core.models import Coding, MappableConcept
from ga4gh.va_spec.base.core import DataSet
from ga4gh.va_spec.base.core import iriReference as IRI

from mavedb.lib.annotation.document import score_set_as_iri
from mavedb.models.score_set import ScoreSet

logger = logging.getLogger(__name__)


SPDX_LICENSE_SYSTEM = "https://spdx.org/licenses/"


def _license_spdx_code(score_set: ScoreSet) -> str:
    """Derive a SPDX-style license identifier from a score set license."""
    if not score_set.license.short_name:
        raise ValueError(
            f"Score set {score_set.urn} does not have a license short_name. "
            "Please ensure the score set has a valid license with a short_name "
            "before attempting to derive SPDX code."
        )

    short_name = score_set.license.short_name.strip().replace(" ", "-")
    version = score_set.license.version

    upper_short_name = short_name.upper()
    if upper_short_name.startswith("CC-BY") and (version and version not in short_name):
        return f"{short_name}-{version}"
    if upper_short_name == "CC0" and version:
        return f"CC0-{version}"

    return short_name


def score_set_license_to_mappable_concept(score_set: ScoreSet) -> Optional[MappableConcept]:
    """Build a mappable concept for a score set's license when a link is available."""
    if not score_set.license.link:
        return None

    return MappableConcept(
        name=score_set.license.long_name,
        primaryCoding=Coding(
            system=SPDX_LICENSE_SYSTEM,
            code=_license_spdx_code(score_set),
            systemVersion=score_set.license.version,
            iris=[IRI(root=score_set.license.link)],
        ),
    )


def score_set_to_data_set(score_set: ScoreSet) -> DataSet:
    """
    Create a [VA Data Set](https://va-ga4gh.readthedocs.io/en/latest/core-information-model/entities/information-entities/dataset.html#data-set)
    object from the provided MaveDB score set.
    """
    return DataSet(
        id=score_set.urn,
        name=score_set.title,
        description=score_set.short_description,
        license=score_set_license_to_mappable_concept(score_set),
        reportedIn=score_set_as_iri(score_set),
        releaseDate=score_set.published_date if score_set.published_date else None,
    )
