import logging
from typing import Optional

from ga4gh.va_spec.acmg_2015 import VariantPathogenicityEvidenceLine
from ga4gh.va_spec.base.core import Method
from ga4gh.va_spec.base.core import iriReference as IRI

from mavedb.lib.types.annotation import PublicationIdentifierAssociations
from mavedb.models.publication_identifier import PublicationIdentifier

logger = logging.getLogger(__name__)


def publication_as_iri(publication: PublicationIdentifier) -> Optional[IRI]:
    """
    Create an IRI as described in <https://datatracker.ietf.org/doc/html/rfc3986#section-4.1> for the provided publication. Within
    the context of VA-Spec, these can be used interchangeably with an equivalent method object for brevity.
    """
    return IRI(publication.url) if publication.url else None


def publication_identifier_to_method(publication: PublicationIdentifier, subtype: Optional[str] = None) -> Method:
    """
    Generate a [VA Method](https://va-ga4gh.readthedocs.io/en/latest/core-information-model/entities/information-entities/method.html#method)
    object based on the provided publication identifier.
    """
    reporter = publication_as_iri(publication)

    return Method(
        name=subtype,
        reportedIn=reporter if reporter else None,
    )


def publication_identifiers_to_method(publications: PublicationIdentifierAssociations) -> Optional[Method]:
    """
    Generate a [VA Method](https://va-ga4gh.readthedocs.io/en/latest/core-information-model/entities/information-entities/method.html#method)
    object based on the list of provided publication identifier associations.

    NOTE: The purpose of this function is to use the primary publication identifier of a provided
    association list as the basis of the method object. We infer that the primary publication within a list of
    associations for a MaveDB dataset describes the experimental protocol used to generate this data set.

    NOTE: This implementation assumes a single primary publication identifier is associated with an object.
    In future versions of this software this may be an unsafe assumption.
    """
    primary_publication_identifier = next(
        (publication.publication for publication in publications if publication.primary), None
    )

    return (
        publication_identifier_to_method(primary_publication_identifier, subtype="Experimental protocol")
        if primary_publication_identifier
        else None
    )


def mavedb_api_releases_as_iri() -> IRI:
    """
    Create an IRI as described in <https://datatracker.ietf.org/doc/html/rfc3986#section-4.1> for releases of the MaveDB software distribution. Within
    the context of VA-Spec, this IRI can be used interchangeably with an equivalent method object for brevity.
    """
    return IRI("https://github.com/VariantEffect/mavedb-api/releases")


def mavedb_api_as_method() -> Method:
    """
    Generate a [VA Method](https://va-ga4gh.readthedocs.io/en/latest/core-information-model/entities/information-entities/method.html#method)
    object for the MaveDB API software distribution.
    """
    return Method(name="Software version", reportedIn=mavedb_api_releases_as_iri())


def mavedb_vrs_releases_as_iri() -> IRI:
    """
    Create an IRI as described in <https://datatracker.ietf.org/doc/html/rfc3986#section-4.1> for releases of the MaveDB VRS mapping software distribution. Within
    the context of VA-Spec, this IRI can be used interchangeably with an equivalent method object for brevity.
    """
    return IRI("https://github.com/VariantEffect/dcd_mapping2/releases")


def mavedb_vrs_as_method() -> Method:
    """
    Generate a [VA Method](https://va-ga4gh.readthedocs.io/en/latest/core-information-model/entities/information-entities/method.html#method)
    object for the MaveDB VRS mapping software distribution.
    """
    return Method(name="Software version", reportedIn=mavedb_vrs_releases_as_iri())


def excalibr_calibrations_as_iri() -> IRI:
    """
    Create an IRI as described in <https://datatracker.ietf.org/doc/html/rfc3986#section-4.1> for the software used to generate pillar project calibrations. Within
    the context of VA-Spec, this IRI can be used interchangeably with an equivalent method object for brevity.
    """
    return IRI("https://github.com/Dzeiberg/mave_calibration")


def excalibr_calibration_method(method: Optional[VariantPathogenicityEvidenceLine.Criterion]) -> Method:
    """
    Generate a [VA Method](https://va-ga4gh.readthedocs.io/en/latest/core-information-model/entities/information-entities/method.html#method)
    object for the pillar project calibration software distribution.
    """
    return Method(
        name="Software version",
        reportedIn=excalibr_calibrations_as_iri(),
        methodType=method.value if method else None,
    )


def variant_interpretation_functional_guideline_as_iri() -> IRI:
    """
    Create an IRI as described in <https://datatracker.ietf.org/doc/html/rfc3986#section-4.1> for functional variant interpretation guidelines. Within
    the context of VA-Spec, this IRI can be used interchangeably with an equivalent method object for brevity.
    """
    # TODO#418
    return IRI("https://pubmed.ncbi.nlm.nih.gov/29785012/")


def variant_interpretation_functional_guideline_method() -> Method:
    """
    Generate a [VA Method](https://va-ga4gh.readthedocs.io/en/latest/core-information-model/entities/information-entities/method.html#method)
    object for the functional variant interpretation guideline.
    """
    # TODO#418
    return Method(
        name="Variant interpretation guideline",
        reportedIn=variant_interpretation_functional_guideline_as_iri(),
    )
