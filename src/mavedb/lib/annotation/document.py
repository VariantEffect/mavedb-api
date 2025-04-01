import logging
import urllib.parse

from ga4gh.va_spec.base.core import Document, iriReference as IRI, MappableConcept

from mavedb.models.experiment import Experiment
from mavedb.models.score_set import ScoreSet
from mavedb.models.variant import Variant

logger = logging.getLogger(__name__)

# TODO: move centrally
BASE_URL = "https://mavedb.org"


def experiment_as_iri(experiment: Experiment) -> IRI:
    """
    Create an IRI as described in <https://datatracker.ietf.org/doc/html/rfc3986#section-4.1> for the provided MaveDB experiment. Within
    the context of VA-Spec, these can be used interchangeably with an equivalent document object for brevity.
    """
    return IRI(f"{BASE_URL}/experiments/{experiment.urn}")


def experiment_to_document(experiment: Experiment) -> Document:
    """
    Create a [VA Document](https://va-ga4gh.readthedocs.io/en/latest/core-information-model/entities/information-entities/document.html#document)
    object from the provided MaveDB experiment.
    """
    return Document(
        id=experiment.urn,
        title=experiment.title,
        description=experiment.short_description,
        subtype=MappableConcept(
            conceptType="MaveDB Resource",
            name="MaveDB Experiment",
        ),
        urls=[experiment_as_iri(experiment).root],
    )


def score_set_as_iri(score_set: ScoreSet) -> IRI:
    """
    Create an IRI as described in <https://datatracker.ietf.org/doc/html/rfc3986#section-4.1> for the provided MaveDB score set. Within
    the context of VA-Spec, these can be used interchangeably with an equivalent document object for brevity.
    """
    return IRI(f"{BASE_URL}/score-sets/{score_set.urn}")


def score_set_to_document(score_set: ScoreSet) -> Document:
    """
    Create a [VA Document](https://va-ga4gh.readthedocs.io/en/latest/core-information-model/entities/information-entities/document.html#document)
    object from the provided MaveDB score set.
    """
    return Document(
        id=score_set.urn,
        title=score_set.title,
        description=score_set.short_description,
        subtype=MappableConcept(
            conceptType="MaveDB Resource",
            name="MaveDB Score Set",
        ),
        urls=[score_set_as_iri(score_set).root],
    )


def variant_as_iri(variant: Variant) -> IRI:
    """
    Create an IRI as described in <https://datatracker.ietf.org/doc/html/rfc3986#section-4.1> for the provided MaveDB variant. Within
    the context of VA-Spec, these can be used interchangeably with an equivalent document object for brevity.
    """
    # TODO: We should decide if this should instead link to the variant measurement page.
    # TODO(#372): nullable URN.
    return IRI(f"https://mavedb.org/score-sets/{variant.score_set.urn}?variant={urllib.parse.quote_plus(variant.urn)}")  # type: ignore


def variant_to_document(variant: Variant) -> Document:
    """
    Create a [VA Document](https://va-ga4gh.readthedocs.io/en/latest/core-information-model/entities/information-entities/document.html#document)
    object from the provided MaveDB variant.
    """
    return Document(
        id=variant.urn,
        subtype=MappableConcept(
            conceptType="MaveDB Resource",
            name="MaveDB Variant",
        ),
        urls=[variant_as_iri(variant).root],
    )
